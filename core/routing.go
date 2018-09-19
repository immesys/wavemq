package core

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/gogo/protobuf/proto"
	"github.com/immesys/wave/wve"
	pb "github.com/immesys/wavemq/mqpb"
	"golang.org/x/crypto/sha3"
	"google.golang.org/grpc"
)

type ID string
type subscription struct {
	subid   ID
	q       *Queue
	uri     string
	created time.Time
}
type subTreeNode struct {
	mu sync.Mutex
	//A subTreeNodeValue
	v atomic.Value
}
type subTreeNodeValue struct {
	children map[string]*subTreeNode
	subz     map[ID]*subscription
}

//There are two types of queues:
//Upstream queues and normal queues
//An upstream queue has the condition that you only deliver local messages
//to it. The queue is drained by the DR
//A normal queue is drained by a local connection or by a peer subscription
//and is published into by either local connections or peer downlink

type Terminus struct {
	//The subscription tree
	stree *subTreeNode

	//Other modules of the system
	qm *QManager
	am *AuthModule

	//map a subscription ID onto the snode that contains it
	rstree_lock sync.RWMutex
	rstree      map[ID]*subTreeNode

	//The configuration for the terminus
	cfg *RoutingConfig

	//The database used for storing persisted messages
	db *badger.DB

	//The supported namespaces
	namespaces map[string]*DesignatedRouter

	downlinkConns  map[string]*PeerConnection
	downlinkConnMu sync.Mutex

	//Our router id. TODO: should persist across resets
	ourNodeId string

	//The namespaces that we will be designated router for
	drnamespaces map[string]bool

	uplinkConns  map[string]*PeerConnection
	uplinkConnMu sync.RWMutex
}

type PeerConnection struct {
	Conn   *grpc.ClientConn
	Ctx    context.Context
	Cancel context.CancelFunc
}

type DesignatedRouter struct {
	Namespace string
	//	EntityHash     string
	//	EntityLocation string
	Address string
}
type RoutingConfig struct {
	//Our entity
	RouterEntityFile string
	//Where persisted messages get stored
	PersistDataStore string
	//Designated Routers
	Router []DesignatedRouter
	//Namespaces we are a designated router for
	DesignatedNamespaceFiles []string
}

type QueryElement struct {
	Error wve.WVE
	Msg   *pb.Message
}

func NewTerminus(qm *QManager, am *AuthModule, cfg *RoutingConfig) (*Terminus, error) {

	//First validate the config
	err := am.SetRouterEntityFile(cfg.RouterEntityFile)
	if err != nil {
		return nil, err
	}
	weRoute := make(map[string]bool)
	for _, dn := range cfg.DesignatedNamespaceFiles {
		ns, err := am.AddDesignatedRoutingNamespace(dn)
		if err != nil {
			return nil, err
		}
		weRoute[ns] = true
	}
	rv := &Terminus{
		stree:         newSnode(),
		rstree:        make(map[ID]*subTreeNode),
		qm:            qm,
		am:            am,
		cfg:           cfg,
		downlinkConns: make(map[string]*PeerConnection),
		uplinkConns:   make(map[string]*PeerConnection),
		drnamespaces:  weRoute,
	}

	rv.namespaces = make(map[string]*DesignatedRouter)
	for _, r := range cfg.Router {
		dr := r
		rv.namespaces[r.Namespace] = &dr
	}

	//Have we already created the queue
	foundRouters := make(map[string]ID)
	//Restore all the subscriptions present from prior queues
	for _, id := range qm.AllQueueIDs() {
		q, err := qm.GetQ(id)
		if err != nil {
			panic(err)
		}
		sub := &subscription{
			subid:   id,
			q:       q,
			created: time.Now(),
		}
		if q.GetIsPeerUpstream() {
			_, ok := rv.namespaces[q.GetRecipientID()]
			if !ok {
				//This is a router that was removed
				q.Destroy()
				continue
			}
			foundRouters[q.GetRecipientID()] = id
			sub.uri = q.GetRecipientID() + "/*"
		} else {
			subreq := q.GetSubRequest()
			uri := base64.URLEncoding.EncodeToString(subreq.Tbs.Namespace) + "/" + subreq.Tbs.Uri
			sub.uri = uri
		}
		rv.addSub(sub.uri, sub)
	}

	//Create queues for new router connections
	for ns, _ := range rv.namespaces {
		_, ok := foundRouters[ns]
		if ok {
			continue
		}
		fmt.Printf("Creating new peer routing queue for %s\n", ns)
		id := ID("upstream-" + ns)
		q, err := qm.GetQ(id)
		if err != nil {
			panic(err)
		}
		uri := ns + "/*"
		q.SetIsPeerUpstream(true)
		q.SetTrunking(true)
		q.SetRecipientID(ns)
		q.Flush()
		sub := &subscription{
			subid:   id,
			q:       q,
			uri:     uri,
			created: time.Now(),
		}
		rv.addSub(uri, sub)
	}

	//Create upstream daemons for every router
	for _, dr := range rv.namespaces {
		q, err := qm.GetQ(ID("upstream-" + dr.Namespace))
		if err != nil {
			panic(err)
		}
		go rv.beginUpstreamPeering(q, dr)
	}

	//ID
	// "" -> local
	// "something"

	//Open the database
	opts := badger.DefaultOptions
	opts.Dir = cfg.PersistDataStore
	opts.ValueDir = cfg.PersistDataStore
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	rv.db = db

	rv.ourNodeId = rv.LoadID()

	//Run the BG tasks
	go rv.bgTasks()
	return rv, nil
}

func (t *Terminus) RouterID() string {
	return t.ourNodeId
}

func (t *Terminus) Publish(m *pb.Message) {
	var clientlist []*subscription
	ns := base64.URLEncoding.EncodeToString(m.Tbs.Namespace)
	fullUri := ns + "/" + m.Tbs.Uri
	t.rMatchSubs(fullUri, func(s *subscription) {
		if s.q.GetIsPeerUpstream() {
			//We only deliver local messages
			if m.Tbs.OriginRouter != t.ourNodeId {
				//This came from upstream, don't send it back on upstream
				return
			}
		} else {
			if s.q.GetRecipientID() == m.Tbs.OriginRouter {
				if s.q.GetRecipientID() == "" {
					panic(string(s.q.ID()))
				}
				//This queue goes to the same node that issued the message
				//don't loop
				return
			}
		}

		clientlist = append(clientlist, s)
	})

	//Enqueue the message in all live subscriptions
	//Manually call unsubscribe on all queues that have expired
	//An expiry here is rare, there has to have been no dequeues for like
	//a week
	for _, sub := range clientlist {
		if sub.q.Ctx.Err() != nil {
			t.unsubscribeInternalID(sub.subid)
		} else {
			sub.q.Enqueue(m)
			//fmt.Printf("post enq length=%d (%p)\n", sub.q.length+sub.q.uncommittedLength, sub.q)
		}
	}

	//If we are the DR for this and it is a persist message, also persist it
	if t.drnamespaces[ns] && m.Persist {
		serial, err := proto.Marshal(m)
		if err != nil {
			panic(err)
		}
		t.putMessage(fullUri, serial)
	}
}

func (t *Terminus) Unsubscribe(entity []byte, subid string) wve.WVE {
	realid := toSubID(entity, subid)
	return t.unsubscribeInternalID(realid)
}

func (t *Terminus) unsubscribeInternalID(subid ID) wve.WVE {
	t.rstree_lock.Lock()
	node, ok := t.rstree[subid]
	if !ok {
		t.rstree_lock.Unlock()
		return wve.Err(NoSuchSubscription, "no such subscription exists")
	}
	toTerm := []*subscription{}

	next := node.copy()
	//Erase the subscription itself
	delete(next.subz, subid)
	node.v.Store(*next)
	//Remove it from the reverse lookup
	delete(t.rstree, subid)

	t.rstree_lock.Unlock()

	for _, tt := range toTerm {
		//This will cancel the queue context, which should cause consumers
		//to stop consuming
		tt.q.Destroy()
	}
	return nil
}

func toSubID(entityHash []byte, subid string) ID {
	h := sha3.New256()
	h.Write(entityHash)
	h.Write([]byte(subid))
	return ID(base64.URLEncoding.EncodeToString(h.Sum(nil)))
}

//This is the real function to call for creating a subscription or resuming an existing one
//the URI must already have the namespace in front
func (t *Terminus) CreateSubscription(ps *pb.PeerSubscribeParams) (*Queue, wve.WVE) {
	ns := base64.URLEncoding.EncodeToString(ps.Tbs.Namespace)
	_, ok := t.namespaces[ns]
	if !ok {
		//We are not bound to a DR for this namespace. Are we the
		//DR ourselves?
		if !t.drnamespaces[ns] {
			return nil, wve.Err(wve.InvalidParameter, "no configured DR for this namespace")
		}
	}

	fulluri := ns + "/" + ps.Tbs.Uri
	subid := toSubID(ps.Tbs.SourceEntity, ps.Tbs.Id)

	//First see if this already exists
	t.rstree_lock.Lock()
	node, ok := t.rstree[subid]
	t.rstree_lock.Unlock()

	//Resuming a subscription
	if ok {
		nv := node.v.Load().(subTreeNodeValue)
		sub := nv.subz[subid]
		//In case the proof has changed
		sub.q.SetSubRequest(ps)
		sub.q.Flush()
		return sub.q, nil
	}

	//Need to create a new subscription
	q, err := t.qm.GetQ(subid)
	if err != nil {
		return nil, wve.ErrW(QueueError, "could not obtain queue", err)
	}
	q.SetSubRequest(ps)
	//If this is not a local delivery queue, set the recipient ID to enable
	//filtering later
	if ps.Tbs.RouterID != t.ourNodeId {
		q.SetRecipientID(ps.Tbs.RouterID)
	}
	q.Flush()
	sub := &subscription{
		subid:   subid,
		q:       q,
		uri:     fulluri,
		created: time.Now(),
	}
	t.addSub(fulluri, sub)
	return q, nil
}

//addSub adds a subscription to terminus.
func (tm *Terminus) addSub(topic string, s *subscription) {
	//TODO this function also needs to create the subscription in the DR
	//it has what it needs in s.q.GetSubRequest

	if !s.q.GetIsPeerUpstream() {
		//This could be local delivery or it could be a peer downstream
		if s.q.GetRecipientID() == "" {
			//This is a local delivery queue: we need to start an upstream
			//peering connection
			go tm.beginDownstreamPeering(s.q)
		} else {
			//We are a DR and this is a downlink queue to a lower tier router
			//We don't have to do anything because the lower router is going to
			//pull from us.
		}
	}
	parts := strings.Split(topic, "/")
	fmt.Println("Add subscription: ", parts)
	node := tm.stree.addSub(parts, s)
	tm.rstree_lock.Lock()
	tm.rstree[s.subid] = node
	tm.rstree_lock.Unlock()
}

func (tm *Terminus) rMatchSubs(topic string, visitor func(s *subscription)) {
	parts := strings.Split(topic, "/")
	tm.stree.rmatchSubs(parts, visitor)
}

func (t *Terminus) bgTasks() {
	for {
		time.Sleep(5 * time.Second)
		t.rstree_lock.RLock()
		toremove := []ID{}
		for subid, stn := range t.rstree {
			s := stn.v.Load().(subTreeNodeValue).subz[subid]
			if s.q.Ctx.Err() != nil {
				toremove = append(toremove, subid)
			}
		}
		if len(t.rstree) > 0 {
			fmt.Printf("Active subscriptions:\n")
			fmt.Printf("  AGE   URI\n")
			for subid, stn := range t.rstree {
				n := stn.v.Load().(subTreeNodeValue)
				sub := n.subz[subid]
				age := time.Now().Sub(sub.created)
				fmt.Printf("  %-5s %s\n", rounddur(age, time.Second), sub.uri)
			}
		} else {
			fmt.Printf("No active subscriptions\n")
		}
		t.rstree_lock.RUnlock()
		for _, r := range toremove {
			t.unsubscribeInternalID(r)
		}
	}
}

func (t *Terminus) downstreamClient(namespace string) (*PeerConnection, error) {
	rtr := t.namespaces[namespace]
	t.downlinkConnMu.Lock()
	conn, ok := t.downlinkConns[namespace]
	t.downlinkConnMu.Unlock()
	if !ok || conn.Ctx.Err() != nil {
		ctx, cancel := context.WithCancel(context.Background())
		if rtr == nil {
			panic(rtr)
		}
		nc, err := t.dialPeer(rtr.Address, namespace)
		//
		// nc, err := grpc.Dial(rtr.Address, grpc.WithInsecure(), grpc.FailOnNonTempDialError(true), grpc.WithBlock(), grpc.WithTimeout(30*time.Second))
		if err != nil {
			return nil, err
		}
		//The above might have taken some time, double check nobody else opened a
		//connection in the meantime
		t.downlinkConnMu.Lock()
		exconn, ok := t.downlinkConns[namespace]
		if !ok || exconn.Ctx.Err() != nil {
			//Set our one
			conn = &PeerConnection{
				Ctx:    ctx,
				Cancel: cancel,
				Conn:   nc,
			}
			t.downlinkConns[namespace] = conn
			t.downlinkConnMu.Unlock()
		} else {
			//Someone raced us to making a connection
			nc.Close()
			cancel()
			t.downlinkConnMu.Unlock()
			return exconn, nil
		}
	}
	return conn, nil
}

func (t *Terminus) beginDownstreamPeering(q *Queue) {
	//Double check this is a registered peer namespace. If we are the DR
	//or the namespace is unconfigured, then we just do local delivery
	ns := base64.URLEncoding.EncodeToString(q.GetSubRequest().Tbs.Namespace)
	_, ok := t.namespaces[ns]
	if !ok {
		fmt.Printf("not peering for namespace %q\n", ns)
		return
	}

	//We need to know which address to dial.
	for {
		//This way when we unsubscribe this downstream peering will die too
		ctx, cancel := context.WithCancel(q.Ctx)
		err := t.downstreamPeer(ctx, q)
		fmt.Printf("downstream peering error %v\n", err)
		cancel()
		if q.Ctx.Err() != nil {
			//This queue is no more
			return
		}
		time.Sleep(30 * time.Second)
	}
}
func (t *Terminus) downstreamPeer(ctx context.Context, q *Queue) (err error) {
	subreq := q.GetSubRequest()
	ns := base64.URLEncoding.EncodeToString(subreq.Tbs.Namespace)
	conn, err := t.downstreamClient(ns)
	if err != nil {
		return err
	}
	defer func() {
		e := recover()
		if e != nil {
			err = e.(error)
			conn.Conn.Close()
			conn.Cancel()
		}
	}()
	peer := pb.NewWAVEMQPeeringClient(conn.Conn)
	sub, err := peer.PeerSubscribe(ctx, subreq)
	if err != nil {
		panic(err)
	}
	for {
		if ctx.Err() != nil {
			//The queue context is over. This is not a transient network error but rather
			//the result of an unsubscribe or age-out. We need to inform our peer
			peer.PeerUnsubscribe(conn.Ctx, &pb.PeerUnsubscribeParams{
				SourceEntity: subreq.Tbs.SourceEntity,
				Id:           subreq.Tbs.Id,
			})
			//If the above call fails, we rely on age-out upstream to clear the queue
			return nil
		}
		msg, err := sub.Recv()
		if err != nil {
			panic(err)
		}
		q.Enqueue(msg.Message)
	}
}
func (t *Terminus) beginUpstreamPeering(q *Queue, dr *DesignatedRouter) {
	//TODO add transport credentials using auth manager
	//Do TLS handshake with signature on the self-signed cert, ala BW2
	for {
		ctx, cancel := context.WithCancel(context.Background())
		err := t.upstreamPeer(ctx, q, dr)
		cancel()
		fmt.Printf("error peering with %s (%s): %v\n", dr.Namespace, dr.Address, err)
		time.Sleep(30 * time.Second)
	}
}

func (t *Terminus) GetDesignatedRouterConnection(namespace string) *PeerConnection {
	t.uplinkConnMu.RLock()
	conn, ok := t.uplinkConns[namespace]
	t.uplinkConnMu.RUnlock()
	if !ok {
		return nil
	}
	if conn.Ctx.Err() != nil {
		return nil
	}
	return conn
}

func (t *Terminus) upstreamPeer(ctx context.Context, q *Queue, dr *DesignatedRouter) (err error) {
	t.uplinkConnMu.Lock()
	delete(t.uplinkConns, dr.Namespace)
	t.uplinkConnMu.Unlock()

	conn, err := t.dialPeer(dr.Address, dr.Namespace)
	fmt.Printf("DIAL PEER COMPLETED: %v\n", err)
	if err != nil {
		return err
	}
	//
	// conn, err := grpc.Dial(dr.Address, grpc.WithInsecure(), grpc.FailOnNonTempDialError(true), grpc.WithBlock(), grpc.WithTimeout(30*time.Second))
	// if err != nil {
	// 	return err
	// }
	//Ensure that we always close the connection upon some kind of failure
	defer func() {
		e := recover()
		if e != nil {
			err = e.(error)
		}
		conn.Close()
	}()
	peerConn := &PeerConnection{
		Conn: conn,
		Ctx:  ctx,
	}
	t.uplinkConnMu.Lock()
	t.uplinkConns[dr.Namespace] = peerConn
	t.uplinkConnMu.Unlock()

	notify := make(chan struct{}, 5)
	q.SubscribeNotifications(&NotificationSubscriber{
		Ctx:    ctx,
		Notify: notify,
	})
	peer := pb.NewWAVEMQPeeringClient(conn)
	//TODO rather have a pool of workers that send frames to the peer, using
	//the returned pool size as an indication of how much can be sent
	// iterate -> workers x[ send across -> dequeue on complete ]
	const numWorkers = 12
	errch := make(chan error, numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func() {
			for {
				for {
					if ctx.Err() != nil {
						return
					}
					m := q.Dequeue()
					if m == nil {
						break
					}
					subctx, cancel := context.WithTimeout(ctx, 30*time.Second)
					resp, err := peer.PeerPublish(subctx, &pb.PeerPublishParams{
						Msg: m,
					})
					cancel()
					if err != nil {
						//Abort this connection and reconnect
						errch <- err
						return
					}
					if resp.Error != nil {
						fmt.Printf("WARNING: PEER PUBLISH MESSAGE ERROR: %s\n", resp.Error.Message)
					}
					//TODO use resp.sizes as mentioned above
				}
				//Wait until the queue is non empty
				<-notify
			}
		}()
	}
	return <-errch
}

func (t *Terminus) Query(namespace []byte, uri string) chan QueryElement {
	ns := base64.URLEncoding.EncodeToString(namespace)
	ruri := ns + "/" + uri
	smch := make(chan SM, 10)
	go t.GetMatchingMessage(ruri, smch)
	rv := make(chan QueryElement, 10)
	go func() {
		for e := range smch {
			m := pb.Message{}
			err := proto.Unmarshal(e.Body, &m)
			if err != nil {
				fmt.Printf("failed to unmarshal proto message from persist: %v\n", err)
				continue
			}
			rv <- QueryElement{Msg: &m}
		}
		close(rv)
	}()
	return rv
}

// func (cl *Client) Persist(m *Message) {
// 	store.PutMessage(m.Topic, m.Encoded)
// 	cl.Publish(m)
// }
//
// func (cl *Client) Query(m *Message, cb func(m *Message)) {
// 	rc := make(chan store.SM, 3)
// 	go store.GetMatchingMessage(m.Topic, rc)
// 	for sm := range rc {
// 		//We could check validity of the message, but whoever
// 		//we send this to will do that. We just check expiry because
// 		//it is cheap
// 		m, err := LoadMessage(sm.Body)
// 		if err != nil {
// 			panic("Not expecting error from unpersist: " + err.Error())
// 		}
// 		if !m.ExpireTime.Before(time.Now()) {
// 			cb(m)
// 		}
// 	}
// 	cb(nil)
// }
//
// func (cl *Client) List(m *Message, cb func(s string, ok bool)) {
// 	rc := make(chan string, 3)
// 	go store.ListChildren(m.Topic, rc)
// 	for {
// 		select {
// 		case uri, ok := <-rc:
// 			if ok {
// 				cb(uri, true)
// 			} else {
// 				cb("", false)
// 				return
// 			}
// 		}
// 	}
// }
//

func newSnode() *subTreeNode {
	n := subTreeNodeValue{
		children: make(map[string]*subTreeNode),
	}
	rv := &subTreeNode{}
	rv.v.Store(n)
	return rv
}

//For a node in the tree, match the given subscription string and call visitor
//for every subscription found
func (s *subTreeNode) rmatchSubs(parts []string, visitor func(s *subscription)) {
	n := s.v.Load().(subTreeNodeValue)
	if len(parts) == 0 {
		for _, sub := range n.subz {
			visitor(sub)
		}
		return
	}
	v1, ok1 := n.children[parts[0]]
	v2, ok2 := n.children["+"]
	v3, ok3 := n.children["*"]
	if ok1 {
		v1.rmatchSubs(parts[1:], visitor)
	}
	if ok2 {
		v2.rmatchSubs(parts[1:], visitor)
	}
	if ok3 {
		for i := 0; i <= len(parts); i++ {
			v3.rmatchSubs(parts[i:], visitor)
		}
	}
}

//Make a deep copy of the node value
func (s *subTreeNode) copy() *subTreeNodeValue {
	rv := &subTreeNodeValue{
		subz:     make(map[ID]*subscription),
		children: make(map[string]*subTreeNode),
	}
	n := s.v.Load().(subTreeNodeValue)
	for id, sub := range n.subz {
		rv.subz[id] = sub
	}
	for key, child := range n.children {
		rv.children[key] = child
	}
	return rv
}

//Add the given subscription parts starting from the given snode
//returns the node added to the tree
func (s *subTreeNode) addSub(parts []string, sub *subscription) *subTreeNode {
	if len(parts) == 0 {
		s.mu.Lock()
		n := s.copy()
		n.subz[sub.subid] = sub
		s.v.Store(*n)
		s.mu.Unlock()
		return s
	}
	n := s.v.Load().(subTreeNodeValue)
	child, ok := n.children[parts[0]]
	if !ok {
		nc := newSnode()
		node := nc.addSub(parts[1:], sub)
		s.mu.Lock()
		n := s.copy()
		n.children[parts[0]] = nc
		s.v.Store(*n)
		s.mu.Unlock()
		return node
	} else {
		return child.addSub(parts[1:], sub)
	}
}

func rounddur(d, r time.Duration) time.Duration {
	if r <= 0 {
		return d
	}
	neg := d < 0
	if neg {
		d = -d
	}
	if m := d % r; m+m < r {
		d = d - m
	} else {
		d = d + r - m
	}
	if neg {
		return -d
	}
	return d
}
