package core

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/immesys/wavemq/pb"
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

type Terminus struct {
	//The subscription tree
	stree *subTreeNode
	//The queue manager
	qm *QManager

	//map a subscription ID onto the snode that contains it
	rstree_lock sync.RWMutex
	rstree      map[ID]*subTreeNode

	//The configuration for the terminus
	cfg *RoutingConfig

	//The database used for storing persisted messages
	db *badger.DB
}

type RoutingConfig struct {
	//Where persisted messages get stored
	PersistDataStore string
}

func NewTerminus(qm *QManager, cfg *RoutingConfig) (*Terminus, error) {
	rv := &Terminus{
		stree:  newSnode(),
		rstree: make(map[ID]*subTreeNode),
		qm:     qm,
		cfg:    cfg,
	}
	//Restore all the subscriptions present from prior queues
	for _, id := range qm.AllQueueIDs() {
		q, err := qm.GetQ(id)
		if err != nil {
			panic(err)
		}
		uri := q.GetSubscription()
		sub := &subscription{
			subid:   id,
			q:       q,
			uri:     uri,
			created: time.Now(),
		}
		rv.addSub(uri, sub)
	}

	//Open the database
	opts := badger.DefaultOptions
	opts.Dir = cfg.PersistDataStore
	opts.ValueDir = cfg.PersistDataStore
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	rv.db = db

	//Run the BG tasks
	go rv.bgTasks()
	return rv, nil
}

func (t *Terminus) Publish(m *pb.Message) {
	var clientlist []*subscription
	fullUri := base64.URLEncoding.EncodeToString(m.Namespace) + "/" + m.Uri
	t.rMatchSubs(fullUri, func(s *subscription) {
		clientlist = append(clientlist, s)
	})

	//Enqueue the message in all live subscriptions
	//Manually call unsubscribe on all queues that have expired
	//An expiry here is rare, there has to have been no dequeues for like
	//a week
	for _, sub := range clientlist {
		if sub.q.Ctx.Err() != nil {
			t.Unsubscribe(sub.subid)
		} else {
			sub.q.Enqueue(m)
		}
	}
}

func (t *Terminus) Unsubscribe(subid ID) error {
	t.rstree_lock.Lock()
	node, ok := t.rstree[subid]
	if !ok {
		t.rstree_lock.Unlock()
		return fmt.Errorf("no such subscription exists")
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

//This is the real function to call for creating a subscription or resuming an existing one
//the URI must already have the namespace in front
func (t *Terminus) CreateSubscription(entityVK []byte, subid ID, uri string) (*Queue, error) {
	//First see if this already exists
	t.rstree_lock.Lock()
	node, ok := t.rstree[subid]
	t.rstree_lock.Unlock()

	//Resuming a subscription
	if ok {
		nv := node.v.Load().(subTreeNodeValue)
		sub := nv.subz[subid]
		if !bytes.Equal(sub.q.EntityVK(), entityVK) {
			return nil, fmt.Errorf("attempt to resume a subscription created by a different entity")
		}
		return sub.q, nil
	}

	//Need to create a new subscription
	q, err := t.qm.GetQ(subid)
	if err != nil {
		return nil, err
	}
	q.SetEntityVK(entityVK)
	q.SetSubscription(uri)
	sub := &subscription{
		subid:   subid,
		q:       q,
		uri:     uri,
		created: time.Now(),
	}
	t.addSub(uri, sub)
	return q, nil
}

//addSub adds a subscription to terminus.
func (tm *Terminus) addSub(topic string, s *subscription) {
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
	}
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
