package core

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/golang/protobuf/proto"
	pb "github.com/immesys/wavemq/mqpb"
)

//If a short queue is dequeued less than this amount of time ago
//it is not eligible to be flushed to disk. This prevents flushing of
//queues that are actively being processed
const IdleFlushTime = 10 * time.Second

//If a queue exceeds this size, it will be flushed to disk irrespective
//of the idle flush time. This allows queues attached to consumers that
//are active but lagging to be flushed when they get too deep.
const IdleFlushSize = 1000

//A Queue Manager keeps track of all open queues and is responsible for
//the database backing the queues
type QManager struct {
	cfg QManagerConfig
	db  *badger.DB

	qz map[ID]*Queue
	//The mutex must be held whenever qz is modified
	qzmu sync.Mutex

	ctx       context.Context
	ctxcancel context.CancelFunc
}

//A queue is used to back a subscription, or a trunking (peering) link
type Queue struct {
	//The mutex must be explictly locked before calling any private function
	//on the queue object
	mu sync.Mutex
	//Used to prevent two flushes from happening concurrently
	flushmu sync.Mutex

	mgr *QManager
	//The header contains the index (a type of sequence number) as well as
	//the expiry information
	hdr *QueueHeader
	//When the index is changed, the header is asynchronously flushed
	//to the database. This flush happens if this flag is true
	hdrChanged bool

	//The length/size of the committed portion of the queue
	length int64
	size   int64

	//How many records were dropped due to the queue being full
	drops int64

	//The length/size of the uncommitted portion of the queue
	uncommittedLength int64
	uncommittedSize   int64

	//A list of keys that should be deleted from the db. This is added to
	//whenever a commited entry is dequeued.
	togc []string

	//The head and tail of the committed (in database) queue
	head *Item
	tail *Item

	//The head and tail of the uncommitted queue
	uncommitedHead *Item
	uncommitedTail *Item

	//Subscribers interested in when this queue transitions from
	//empty to non-empty
	notifications []*NotificationSubscriber

	//Used by flushing bgtask to determine if this needs to go to db
	lastDequeue time.Time

	//This will be cancelled when the queue is destroyed by an explicit
	//unsubscribe, or by timeout
	Ctx       context.Context
	ctxcancel context.CancelFunc
}

//TODO change qmanager config to use seconds and KB instead of nano and bytes

//The configuration governing queues
type QManagerConfig struct {
	QueueDataStore string
	//Seconds
	QueueExpiry int64

	//MB
	SubscriptionQueueMaxLength int64
	SubscriptionQueueMaxSize   int64
	TrunkingQueueMaxLength     int64
	TrunkingQueueMaxSize       int64

	//Seconds between to-disk flushes
	FlushInterval int64
}

//Details about a queue that are persisted to disk
type QueueHeader struct {
	Expires int64
	ID      ID
	Index   int64
	//The maximum a queue can go to
	MaxLength int64
	MaxSize   int64

	//When the queue was created
	Created time.Time

	//Used to ensure resubscriptions are done by the same entity
	SubRequest *pb.PeerSubscribeParams

	//Used for rebuilding the subscription mux from queues
	//Subscription string

	//Used for ensuring we don't form routing loops
	RecipientID string

	//Is this queue used for uploading local publish to a peer
	PeerUpstream bool
}

type Iterator struct {
	Current *Item
}

type NotificationSubscriber struct {
	Notify chan struct{}
	Ctx    context.Context
}

//An item in a queue
type Item struct {
	Next    *Item
	Index   int64
	Content *pb.Message
}

//Serialize a queue header
func (qh *QueueHeader) Serialize() []byte {
	buf := bytes.Buffer{}
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(qh)
	if err != nil {
		panic(err)
	}
	return buf.Bytes()
}

//Deserialize a queue header
func LoadQueueHeader(ser []byte) (*QueueHeader, error) {
	buf := bytes.NewBuffer(ser)
	dec := gob.NewDecoder(buf)
	hdr := QueueHeader{}
	err := dec.Decode(&hdr)
	if err != nil {
		return nil, fmt.Errorf("corrupt database")
	}
	return &hdr, nil
}

//Create a new queue manager with the given configuration
func NewQManager(cfg *QManagerConfig) (*QManager, error) {
	//Open database
	opts := badger.DefaultOptions
	opts.Dir = cfg.QueueDataStore
	opts.ValueDir = cfg.QueueDataStore
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	rv := &QManager{
		db:        db,
		qz:        make(map[ID]*Queue),
		ctx:       ctx,
		ctxcancel: cancel,
		cfg:       *cfg,
	}
	err = rv.recover()
	if err != nil {
		return nil, err
	}
	go rv.bgTasks()

	return rv, nil
}

func (qm *QManager) Shutdown() {
	//TODO flush all queues
	qm.ctxcancel()
	err := qm.db.Close()
	if err != nil {
		panic(err)
	}
}

func (qm *QManager) AllQueueIDs() []ID {
	qm.qzmu.Lock()
	defer qm.qzmu.Unlock()
	rv := make([]ID, 0, len(qm.qz))
	for id, _ := range qm.qz {
		rv = append(rv, id)
	}
	return rv
}

//Create a new queue, truncating it if it already existed
func (qm *QManager) NewQ(id ID) (*Queue, error) {
	qm.qzmu.Lock()
	q, ok := qm.qz[id]
	defer qm.qzmu.Unlock()
	if ok {
		q.mu.Lock()
		err := q.reset()
		q.mu.Unlock()
		if err != nil {
			return nil, err
		}
		return q, nil
	}
	q = &Queue{
		hdr: &QueueHeader{
			ID:        id,
			MaxLength: qm.cfg.SubscriptionQueueMaxLength,
			MaxSize:   qm.cfg.SubscriptionQueueMaxSize * 1024 * 1024,
		},
		mgr: qm,
	}
	q.reset()
	qm.qz[id] = q
	return q, nil
}

//Get an existing queue or create a new one
func (qm *QManager) GetQ(id ID) (*Queue, error) {
	//Return an existing queue
	qm.qzmu.Lock()
	q, ok := qm.qz[id]
	qm.qzmu.Unlock()
	if ok {
		return q, nil
	}
	return qm.NewQ(id)
}

//Restores the queue in-memory states from disk
func (qm *QManager) recover() error {
	return qm.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		hdrprefix := []byte("h/")
		for it.Seek(hdrprefix); it.ValidForPrefix(hdrprefix); it.Next() {
			v, err := it.Item().Value()
			if err != nil {
				return err
			}
			hdr, err := LoadQueueHeader(v)
			if err != nil {
				return err
			}
			ctx, cancel := context.WithCancel(context.Background())
			q := &Queue{
				hdr:       hdr,
				mgr:       qm,
				Ctx:       ctx,
				ctxcancel: cancel,
			}
			if q.expired() {
				q.remove()
			} else {
				qm.qz[hdr.ID] = q
			}
		}

		for _, q := range qm.qz {
			var largest int64
			qprefix := []byte(keyQueuePrefix(q.hdr.ID))
			for it.Seek(qprefix); it.ValidForPrefix(qprefix); it.Next() {
				k := it.Item().Key()
				indexString := k[len(qprefix):]
				index, err := strconv.ParseInt(string(indexString), 10, 64)
				if err != nil {
					return err
				}
				if index > largest {
					largest = index
				}
				v, err := it.Item().Value()
				if err != nil {
					return err
				}
				m := &pb.Message{}
				err = proto.Unmarshal(v, m)
				if err != nil {
					return err
				}
				q.enqueueCommitted(index, m)
			}
			q.hdr.Index = largest + 1
			q.WriteHeader()
		}

		for _, q := range qm.qz {
			fmt.Printf("recovered queue %s (length=%d)\n", q.ID(), q.length)
		}

		return nil
	})
}

//Runs the periodic background tasks for all queues
func (qm *QManager) bgTasks() {
	last := time.Now()
	for {
		last = last.Add(time.Duration(qm.cfg.FlushInterval * 1e9))
		toSleep := last.Sub(time.Now())
		if toSleep > 0 {
			time.Sleep(toSleep)
		} else {
			last = time.Now()
		}
		if qm.ctx.Err() != nil {
			return
		}
		//Collate a list of queues to process
		qm.qzmu.Lock()
		qz := make([]*Queue, 0, len(qm.qz))
		for _, q := range qm.qz {
			qz = append(qz, q)
		}
		qm.qzmu.Unlock()

		toremove := []ID{}
		nw := time.Now()
		for _, q := range qz {
			if qm.ctx.Err() != nil {
				return
			}
			if q.expired() {
				toremove = append(toremove, q.hdr.ID)
				q.remove()
				continue
			}
			//Write out a new header
			if q.hdrChanged {
				err := q.WriteHeader()
				if err != nil {
					panic(err)
				}
			}

			if nw.Sub(q.lastDequeue) > IdleFlushTime ||
				q.uncommittedSize > IdleFlushSize {
				//Move uncommited entries to committed
				err := q.Flush()
				if err != nil {
					panic(err)
				}
			}
		}
	}
}

//Subscribe for notifications when the queue transitions from empty to
//nonempty
func (q *Queue) SubscribeNotifications(n *NotificationSubscriber) {
	q.mu.Lock()
	q.notifications = append(q.notifications, n)
	q.mu.Unlock()
}

func (q *Queue) PeekIterator() *Iterator {
	q.mu.Lock()
	it := &Iterator{
		Current: q.head,
	}
	q.mu.Unlock()
	return it
}

func (q *Queue) Header() *QueueHeader {
	return q.hdr
}

func (q *Queue) SetSubRequest(r *pb.PeerSubscribeParams) {
	q.mu.Lock()
	q.hdr.SubRequest = r
	q.hdrChanged = true
	q.mu.Unlock()
}

func (q *Queue) GetSubRequest() *pb.PeerSubscribeParams {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.hdr.SubRequest
}

func (q *Queue) SetRecipientID(s string) {
	q.mu.Lock()
	q.hdr.RecipientID = s
	q.hdrChanged = true
	q.mu.Unlock()
}

func (q *Queue) GetRecipientID() string {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.hdr.RecipientID
}

func (q *Queue) SetIsPeerUpstream(b bool) {
	q.mu.Lock()
	q.hdr.PeerUpstream = b
	q.hdrChanged = true
	q.mu.Unlock()
}

func (q *Queue) GetIsPeerUpstream() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.hdr.PeerUpstream
}

// func (q *Queue) SetSubscription(s string) {
// 	q.mu.Lock()
// 	q.hdr.Subscription = s
// 	q.hdrChanged = true
// 	q.mu.Unlock()
// }
// func (q *Queue) GetSubscription() string {
// 	q.mu.Lock()
// 	defer q.mu.Unlock()
// 	return q.hdr.Subscription
// }
func (q *Queue) notifyAndDropLock() {
	cpidx := 0
	//Process the notifications but also compact them, removing
	//ones where the context is expired
	for _, e := range q.notifications {
		if e.Ctx.Err() != nil {
			continue
		}
		//If the given queue is full, it's ok to drop a notification
		select {
		case e.Notify <- struct{}{}:
		default:
		}
		q.notifications[cpidx] = e
		cpidx++
	}
	q.notifications = q.notifications[:cpidx]
	q.mu.Unlock()
}

//Set the queue maximums to the configured defaults for
//trunking queues (true) or subscription queues (false)
func (q *Queue) SetTrunking(v bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if v {
		q.hdr.MaxSize = q.mgr.cfg.TrunkingQueueMaxSize * 1024 * 1024
		q.hdr.MaxLength = q.mgr.cfg.TrunkingQueueMaxLength
	} else {
		q.hdr.MaxSize = q.mgr.cfg.SubscriptionQueueMaxSize * 1024 * 1024
		q.hdr.MaxLength = q.mgr.cfg.SubscriptionQueueMaxLength
	}
	q.hdrChanged = true
	q.writeHeader()
}

//Add an element to the queue, dropping old records as required
func (q *Queue) Enqueue(m *pb.Message) error {
	q.ck()
	defer q.ck()
	fmt.Printf("start of enq %p %p\n", q.uncommitedHead, q.uncommitedTail)
	defer func() { fmt.Printf("end of enq %p %p\n", q.uncommitedHead, q.uncommitedTail) }()
	sz := proto.Size(m)
	q.mu.Lock()

	//Drop elements to make space for the new one
	for {
		if ((q.uncommittedSize + q.size) > 0) && ((q.uncommittedSize+q.size+int64(sz) > q.hdr.MaxSize) ||
			(q.uncommittedLength+q.length+1 > q.hdr.MaxLength)) {
			fmt.Printf("dropping message: %d\n", sz)
			fmt.Printf("sizes: %d %d %d %d\n", q.uncommittedSize, q.size+q.uncommittedLength+q.length)
			fmt.Printf("maxes: %d %d\n", q.hdr.MaxSize, q.hdr.MaxLength)
			q.dequeue()
			q.drops++
			continue
		}
		break
	}

	it := &Item{
		Content: m,
	}
	it.Index = q.hdr.Index
	q.hdr.Index++
	q.hdrChanged = true
	mustnotify := false
	if q.uncommitedHead == nil {
		q.uncommitedHead = it
		mustnotify = q.head == nil
		//If this is the head of the uncommited queue, link it to the
		//committed queue
		if q.tail != nil {
			q.tail.Next = it
		}
	} else {
		q.uncommitedTail.Next = it
	}
	q.uncommitedTail = it
	q.uncommittedSize += int64(sz)
	q.uncommittedLength++
	if mustnotify {
		q.notifyAndDropLock()
	} else {
		q.mu.Unlock()
	}
	return nil
}

//Return the remaining space in the queue
func (q *Queue) Remaining() (size int64, length int64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	rsize := q.hdr.MaxSize - q.size - q.uncommittedSize
	rlen := q.hdr.MaxLength - q.length - q.uncommittedLength
	return rsize, rlen
}

//Like Dequeue but the element is not removed from the queue
func (q *Queue) Peek() *pb.Message {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.head != nil {
		return q.head.Content
	}
	if q.uncommitedHead != nil {
		return q.uncommitedHead.Content
	}
	return nil
}

//Remove an element from the queue
func (q *Queue) Dequeue() *pb.Message {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.dequeue()
}

//Get the ID of the queue
func (q *Queue) ID() ID {
	return q.hdr.ID
}

//Write the header out to the database
func (q *Queue) WriteHeader() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.writeHeader()
}

func (q *Queue) ck() {
	if q.uncommitedHead == nil && q.uncommitedTail != nil {
		panic("nil ucHead with !nil uctail")
	}
	if q.uncommitedHead != nil && q.uncommitedTail == nil {
		panic("nil ucTail with !nil ucHead")
	}
	if q.head == nil && q.tail != nil {
		panic("nil head with !nil tail")
	}
	if q.head != nil && q.tail == nil {
		panic("nil Tail with !nil Head")
	}
}

//Move items from uncommitted to committed, writing them to the database
func (q *Queue) Flush() error {
	fmt.Printf("flush called\n")
	q.ck()
	defer q.ck()
	q.WriteHeader()
	//We release the q mu quite early, so we hold the flushmu to prevent
	//accidentally flushing concurrently
	q.flushmu.Lock()
	defer q.flushmu.Unlock()

	q.mu.Lock()
	if q.uncommitedHead == nil {
		q.mu.Unlock()
		return nil
	}
	q.ck()
	ucHead := q.uncommitedHead
	ucTail := q.uncommitedTail
	if q.head == nil {
		fmt.Printf("assigning q.head/tail to %p %p\n", ucHead, ucTail)
		q.head = ucHead
		q.tail = ucTail
	} else {
		fmt.Printf("q.head is %p q.tail is %p\n", q.head, q.tail)
		q.tail.Next = ucHead
		q.tail = ucTail
	}
	q.size += q.uncommittedSize
	q.length += q.uncommittedLength
	q.uncommittedSize = 0
	q.uncommittedLength = 0
	q.uncommitedHead = nil
	q.uncommitedTail = nil
	q.ck()
	togc := q.togc
	q.togc = []string{}
	q.mu.Unlock()

	//While dequeue might modify q.head it won't modify the pointers within
	//the elements, so there is no danger of the list being corrupted while
	//we walk it here. The only danger is that another flush modifies
	//the Next pointer of the tail, so we hold flushmu to prevent that
	txn := q.mgr.db.NewTransaction(true)
	//Be prepared to break the transaction into smaller ones
bulkflush:
	for {
		it := ucHead
		for it != nil {
			nextit := it.Next
			bin, err := proto.Marshal(it.Content)
			if err != nil {
				panic(err)
			}
			err = txn.Set([]byte(keyQueueItem(q.hdr.ID, it.Index)), bin)
			if err == badger.ErrTxnTooBig {
				err := txn.Commit(nil)
				if err != nil {
					return err
				}
				txn = q.mgr.db.NewTransaction(true)
				continue bulkflush
			} else if err != nil {
				return err
			}
			it = nextit
		}
		break
	}
	for _, e := range togc {
		fmt.Printf("GC %q\n", e)
		err := txn.Delete([]byte(e))
		if err == badger.ErrTxnTooBig {
			err := txn.Commit(nil)
			if err != nil {
				return err
			}
			txn = q.mgr.db.NewTransaction(true)
			err = txn.Delete([]byte(e))
			if err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
	}
	txn.Commit(nil)

	return nil
}

func (q *Queue) Destroy() {
	q.mu.Lock()
	q.ctxcancel()
	q.remove()
	q.mu.Unlock()
}

//Reset deletes all state for a queue, essentially deleting it and
//recreating it
func (q *Queue) Reset() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.reset()
}

//Removes all the data in the database pertaining to this queue. Does
//not clear the datastructure for reuse (see reset for that)
func (q *Queue) remove() error {
	//Remove the header
	hdrprefix := []byte(keyHeader(q.hdr.ID))
	q.mgr.db.Update(func(txn *badger.Txn) error {
		fmt.Printf("deleting key %q\n", hdrprefix)
		txn.Delete(hdrprefix)
		return nil
	})

	//Transactions are limited in size. Be prepared to break up into lots
	//of smaller transactions if there are a lot of records to delete
bulkerase:
	for {
		txn := q.mgr.db.NewTransaction(true)
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		prefix := []byte(keyQueuePrefix(q.hdr.ID))
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			fmt.Printf("doing delete: %q\n", it.Item().Key())
			err := txn.Delete(it.Item().Key())
			if err == badger.ErrTxnTooBig {
				it.Close()
				err := txn.Commit(nil)
				if err != nil {
					return err
				}
				continue bulkerase
			} else if err != nil {
				it.Close()
				return err
			}
		}
		it.Close()
		break
	}

	return nil
}

//Returns true if the queue should be removed
func (q *Queue) expired() bool {
	return time.Now().UnixNano() > q.hdr.Expires
}

//Drain the queue and reset its header. The lock must be held
func (q *Queue) reset() error {
	//Delete all old data
	q.remove()
	//Reset the data structure
	qh := &QueueHeader{
		Expires:   time.Now().Add(time.Duration(q.mgr.cfg.QueueExpiry * 1e9)).UnixNano(),
		Index:     0,
		ID:        q.hdr.ID,
		Created:   time.Now(),
		MaxLength: q.hdr.MaxLength,
		MaxSize:   q.hdr.MaxSize,
	}
	ctx, cancel := context.WithCancel(context.Background())
	*q = Queue{
		hdr:       qh,
		mgr:       q.mgr,
		Ctx:       ctx,
		ctxcancel: cancel,
	}
	q.writeHeader()
	return nil
}

//Enqueue directly into the committed queue, only used for on-startup recovery
func (q *Queue) enqueueCommitted(index int64, m *pb.Message) error {
	q.ck()
	defer q.ck()
	it := &Item{
		Content: m,
	}
	it.Index = index
	if q.head == nil {
		q.head = it
	}
	if q.tail != nil {
		q.tail.Next = it
	}
	q.tail = it
	q.size += int64(proto.Size(m))
	q.length++
	return nil
}

//Internal dequeue, mutex must be held
func (q *Queue) dequeue() *pb.Message {
	q.ck()
	defer q.ck()
	nw := time.Now()
	q.hdr.Expires = nw.Add(time.Duration(q.mgr.cfg.QueueExpiry * 1e9)).UnixNano()
	q.hdrChanged = true
	q.lastDequeue = nw
	if q.head != nil {
		it := q.head
		//Special case, if this was the end of the committed queue, make sure we
		//nil out the pointers
		if q.head == q.tail {
			q.head = nil
			q.tail = nil
		} else {
			q.head = q.head.Next
		}
		fmt.Printf("appending to gc\n")
		q.togc = append(q.togc, keyQueueItem(q.hdr.ID, it.Index))
		q.size -= int64(proto.Size(it.Content))
		q.length--
		return it.Content
	}

	it := q.uncommitedHead
	if it == nil {
		return nil
	}
	fmt.Printf("dequeue uc\n")
	q.uncommitedHead = q.uncommitedHead.Next
	if q.uncommitedHead == nil {
		q.uncommitedTail = nil
	}
	q.uncommittedSize -= int64(proto.Size(it.Content))
	q.uncommittedLength--
	return it.Content
}

//Write the header out to the database
func (q *Queue) writeHeader() error {
	return q.mgr.db.Update(func(txn *badger.Txn) error {
		txn.Set([]byte(keyHeader(q.hdr.ID)), q.hdr.Serialize())
		return nil
	})
}

func (i *Iterator) Next() {
	i.Current = i.Current.Next
}
func (i *Iterator) Value() *pb.Message {
	if i.Current == nil {
		return nil
	}
	return i.Current.Content
}

//The DB key prefix for a queue
func keyQueuePrefix(id ID) string {
	return "q/" + string(id) + "/"
}

//The DB key for a header
func keyHeader(id ID) string {
	return "h/" + string(id)
}

//The DB key for a specific item in a queue
func keyQueueItem(id ID, index int64) string {
	return fmt.Sprintf("q/%s/%08d", id, index)
}
