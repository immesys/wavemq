package server

import (
	"context"
	"crypto/rand"
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/immesys/wave/waved"
	"github.com/immesys/wavemq/core"
	pb "github.com/immesys/wavemq/mqpb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
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
func getqm(t testing.TB) *core.QManager {
	td, err := ioutil.TempDir("/tmp", "mq_q")
	require.NoError(t, err)
	cfg := &core.QManagerConfig{
		QueueDataStore:             td,
		QueueExpiry:                int64(24 * time.Hour),
		SubscriptionQueueMaxLength: 100,
		SubscriptionQueueMaxSize:   1 * 1024 * 1024,
		TrunkingQueueMaxLength:     100,
		TrunkingQueueMaxSize:       1 * 1024 * 1024,
	}
	rv, err := core.NewQManager(cfg)
	require.NoError(t, err)
	return rv
}
func gettm(t testing.TB, qm *core.QManager) *core.Terminus {
	td, err := ioutil.TempDir("/tmp", "mq_tm")
	require.NoError(t, err)
	cfg := &core.RoutingConfig{
		PersistDataStore: td,
	}
	rv, err := core.NewTerminus(qm, cfg)
	require.NoError(t, err)
	return rv
}

func getam(t testing.TB) *core.AuthModule {
	td, err := ioutil.TempDir("/tmp", "mq_am")
	require.NoError(t, err)
	storage := make(map[string]map[string]string)
	storage["default"] = make(map[string]string)
	storage["default"]["provider"] = "http_v1"
	storage["default"]["url"] = "https://standalone.storage.bwave.io/v1"
	storage["default"]["version"] = "1"
	am, err := core.NewAuthModule(&core.AuthConfig{
		WaveConfig: &waved.Configuration{
			Database: td,
			Storage:  storage,
		},
	})
	require.NoError(t, err)
	return am
}

func TestServer(t *testing.T) {
	qm := getqm(t)
	tm := gettm(t, qm)
	am := getam(t)

	server := NewLocalServer(tm, am, &LocalServerConfig{
		ListenAddr: "127.0.0.1:54000",
	})
	_ = server
	conn, err := grpc.Dial("127.0.0.1:54000", grpc.WithInsecure(), grpc.FailOnNonTempDialError(true), grpc.WithBlock())
	require.NoError(t, err)
	client := pb.NewWAVEMQClient(conn)
	ns := make([]byte, 32)
	rand.Read(ns)
	uri := "a/common/uri"
	identifier := "myid"
	sc, err := client.Subscribe(context.Background(), &pb.SubscribeParams{
		Namespace:  ns,
		Uri:        uri,
		Identifier: identifier,
	})
	require.NoError(t, err)
	go func() {
		for {
			msg, err := sc.Recv()
			if err != nil {
				fmt.Printf("got error: %v\n", err)
				return
			}
			fmt.Printf("got message! %v\n", msg)
		}
	}()
	time.Sleep(1 * time.Second)

	client.Publish(context.Background(), &pb.PublishParams{
		Namespace: ns,
		Uri:       uri,
		Content: []*pb.PayloadObject{
			{Schema: "hello", Content: []byte("world")},
		},
	})
	time.Sleep(5 * time.Second)
}
