package core

import (
	"crypto/rand"
	"io/ioutil"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	pb "github.com/immesys/wavemq/mqpb"
	"github.com/stretchr/testify/require"
)

func mkmsg() *pb.Message {
	m := &pb.Message{}
	m.Tbs = &pb.MessageTBS{
		Uri:       "a/typical/uri/sort/of/length",
		Namespace: make([]byte, 32),
	}
	rand.Read(m.Tbs.Namespace)
	m.ProofDER = make([]byte, 300000)
	rand.Read(m.ProofDER)
	return m
}
func getqm(t testing.TB) *QManager {
	td, err := ioutil.TempDir("/tmp", "mq")
	require.NoError(t, err)
	cfg := &QManagerConfig{
		QueueDataStore:             td,
		QueueExpiry:                int64(24 * time.Hour),
		SubscriptionQueueMaxLength: 100,
		SubscriptionQueueMaxSize:   1 * 1024 * 1024,
		TrunkingQueueMaxLength:     100,
		TrunkingQueueMaxSize:       1 * 1024 * 1024,
	}
	rv, err := NewQManager(cfg)
	require.NoError(t, err)
	return rv
}

func BenchmarkMessageSerialization(b *testing.B) {
	m := mkmsg()
	for i := 0; i < b.N; i++ {
		_, _ = proto.Marshal(m)
	}
}

func BenchmarkMessageSize(b *testing.B) {
	m := mkmsg()
	for i := 0; i < b.N; i++ {
		proto.Size(m)
	}
}

func BenchmarkMessageSize2(b *testing.B) {
	b.Skip()
	//It's about 110 ns, fast.
	msgs := make([]*pb.Message, b.N)
	for i := 0; i < b.N; i++ {
		msgs[i] = mkmsg()
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		proto.Size(msgs[i])
	}
}

func BenchmarkMessageDeSerialization(b *testing.B) {
	m := mkmsg()
	bin, _ := proto.Marshal(m)
	for i := 0; i < b.N; i++ {
		m2 := &pb.Message{}
		proto.Unmarshal(bin, m2)
	}
}
