package core

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func getqm(t testing.TB) *QManager {
	td, err := ioutil.TempDir("/tmp", "mq")
	require.NoError(t, err)
	cfg := &QManagerConfig{
		QueueDataStore: td,
		QueueExpiry:    int64(24 * time.Hour),
	}
	rv, err := NewQManager(cfg)
	require.NoError(t, err)
	return rv
}

func TestQueueInsert(t *testing.T) {
	qm := getqm(t)
	q, err := qm.NewQ("helloworld")
	require.NoError(t, err)
	err = q.Enqueue([]byte("hello"))
	require.NoError(t, err)
	it, err := q.Dequeue()
	require.NoError(t, err)
	require.EqualValues(t, it, "hello")
}

func BenchmarkHello(b *testing.B) {
	qm := getqm(b)
	content := make([]byte, 2048)
	rand.Read(content)
	q, err := qm.GetQ("aq")
	require.NoError(b, err)
	b.ResetTimer()
	fmt.Printf("N is %d\n", b.N)
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(content, uint64(i))
		err := q.Enqueue(content)
		q.Flush()
		require.NoError(b, err)
	}
	for i := 0; i < b.N; i++ {
		it, err := q.Dequeue()
		require.NoError(b, err)
		require.NotNil(b, it)
	}
}
