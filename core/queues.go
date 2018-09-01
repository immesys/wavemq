package core

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"
	"time"

	"github.com/dgraph-io/badger"
)

//A queue is made up of entries written to a database that can be read out in order

type QManager struct {
	cfg QManagerConfig
	db  *badger.DB

	qz   map[string]*Queue
	qzmu sync.Mutex
}

type Queue struct {
	mu         sync.Mutex
	mgr        *QManager
	hdr        *QueueHeader
	hdrChanged bool

	length           int64
	uncommitedLength int64

	//A list of keys that should be deleted from the db
	togc []string

	head           *Item
	tail           *Item
	uncommitedHead *Item
	uncommitedTail *Item
}

type QManagerConfig struct {
	QueueDataStore string
	//Nanoseconds
	QueueExpiry int64
}

type QueueHeader struct {
	Expires int64
	ID      string
	Index   int64
}

type Item struct {
	Next    *Item
	Index   int64
	Content []byte
}

func (qh *QueueHeader) Serialize() []byte {
	buf := bytes.Buffer{}
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(qh)
	if err != nil {
		panic(err)
	}
	return buf.Bytes()
}
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

func NewQManager(cfg *QManagerConfig) (*QManager, error) {
	//Open database
	opts := badger.DefaultOptions
	opts.Dir = cfg.QueueDataStore
	opts.ValueDir = cfg.QueueDataStore
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return &QManager{
		db: db,
		qz: make(map[string]*Queue),
	}, nil
}
func (qm *QManager) NewQ(id string) (*Queue, error) {
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
			ID: id,
		},
		mgr: qm,
	}
	q.reset()
	qm.qz[id] = q
	return q, nil
}

//Drain the queue and reset its header. The lock must be held
func (q *Queue) reset() error {
	//Truncate the queue
	qh := &QueueHeader{
		Expires: time.Now().Add(time.Duration(q.mgr.cfg.QueueExpiry)).UnixNano(),
		Index:   0,
		ID:      q.hdr.ID,
	}
	q.hdr = qh
	bin := qh.Serialize()
	prefix := []byte(keyQueuePrefix(q.hdr.ID))
	q.mgr.db.Update(func(txn *badger.Txn) error {
		txn.Set(prefix, bin)
		return nil
	})

bulkerase:
	for {
		txn := q.mgr.db.NewTransaction(true)
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			err := txn.Delete(it.Item().Key())
			if err == badger.ErrTxnTooBig {
				err := txn.Commit(nil)
				if err != nil {
					return err
				}
				it.Close()
				continue bulkerase
			} else if err != nil {
				return err
			}
		}
		break
	}

	return nil
}

func (qm *QManager) GetQ(id string) (*Queue, error) {
	//Return an existing queue
	qm.qzmu.Lock()
	q, ok := qm.qz[id]
	qm.qzmu.Unlock()
	if ok {
		return q, nil
	}
	return qm.NewQ(id)
}

func (q *Queue) Enqueue(v []byte) error {
	//TODO: If the queue is too long we need to dequeue an item
	it := &Item{
		Content: v,
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	it.Index = q.hdr.Index
	q.hdr.Index++
	if q.uncommitedHead == nil {
		q.uncommitedHead = it
	}
	if q.uncommitedTail != nil {
		q.uncommitedTail.Next = it
	}
	q.uncommitedTail = it
	return nil
}
func (q *Queue) Dequeue() ([]byte, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	//TODO header must be written out every now and then
	q.hdr.Expires = time.Now().Add(time.Duration(q.mgr.cfg.QueueExpiry)).UnixNano()
	q.hdrChanged = true
	if q.head != nil {
		it := q.head
		q.head = q.head.Next
		if q.head == nil {
			q.tail = nil
		}
		q.togc = append(q.togc, keyQueueItem(q.hdr.ID, it.Index))
		return it.Content, nil
	}
	it := q.uncommitedHead
	if it == nil {
		return nil, nil
	}
	q.uncommitedHead = q.uncommitedHead.Next
	if q.uncommitedHead == nil {
		q.uncommitedTail = nil
	}
	return it.Content, nil

}
func (q *Queue) ID() string {
	return q.hdr.ID
}
func (q *Queue) WriteHeader() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.mgr.db.Update(func(txn *badger.Txn) error {
		txn.Set([]byte(keyQueuePrefix(q.hdr.ID)), q.hdr.Serialize())
		return nil
	})
}

func (q *Queue) Flush() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	txn := q.mgr.db.NewTransaction(true)
bulkflush:
	for {
		it := q.uncommitedHead
		for it != nil {
			nextit := it.Next
			it.Next = nil
			if q.head == nil {
				q.head = it
				q.tail = it
			} else {
				q.tail.Next = it
				q.tail = it
			}
			err := txn.Set([]byte(keyQueueItem(q.hdr.ID, it.Index)), it.Content)
			if err == badger.ErrTxnTooBig {
				err := txn.Commit(nil)
				if err != nil {
					return err
				}
				continue bulkflush
			} else if err != nil {
				return err
			}
			it = nextit
		}
		break
	}
	q.uncommitedHead = nil
	q.uncommitedTail = nil
	return nil
}

//Drain deletes all persistent state for a queue
func (q *Queue) Drain() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.reset()
}

func keyQueuePrefix(id string) string {
	return "q/" + id
}
func keyQueueItem(id string, index int64) string {
	return fmt.Sprintf("q/%s/%08d", id, index)
}
