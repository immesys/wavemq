package core

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
)

func gettm(t testing.TB, qm *QManager) *Terminus {
	td, err := ioutil.TempDir("/tmp", "mq_tm")
	require.NoError(t, err)
	cfg := &RoutingConfig{
		PersistDataStore: td,
	}
	rv, err := NewTerminus(qm, cfg)
	require.NoError(t, err)
	return rv
}

func TestRequire(t *testing.T) {
	a := 3
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer cancel()
		b := 0
		fmt.Printf("pre\n")
		require.Equal(t, b, a)
		fmt.Printf("post\n")
	}()
	<-ctx.Done()
}

func TestRequire2(t *testing.T) {
	a := 3
	b := 0
	ctx, cancel := context.WithCancel(context.Background())
	func() {
		fmt.Printf("pre\n")
		require.Equal(t, b, a)
		fmt.Printf("post\n")
		cancel()
	}()
	<-ctx.Done()
}

func mkConsumer(t testing.TB, ctx context.Context, q *Queue, expected int) chan bool {
	rv := make(chan bool)
	go func() {
		got := 0
		n := func() {
			for {
				m := q.Dequeue()
				if m == nil {
					return
				}
				got++
			}
		}

		q.SubscribeNotifications(&NotificationSubscriber{
			Notify: n,
			//This is a context that means stop notifying. We never want that
			Ctx: context.Background(),
		})
		<-ctx.Done()
		if got != expected {
			fmt.Printf("expected to consume %d records, got %d\n", expected, got)
			rv <- false
		} else {
			rv <- true
		}
		close(rv)
	}()
	return rv
}

func TestBasicSubscribe(t *testing.T) {
	qm := getqm(t)
	tm := gettm(t, qm)

	vk := make([]byte, 32)
	ns := make([]byte, 32)
	rand.Read(ns)
	nss := base64.URLEncoding.EncodeToString(ns)
	id := ID(uuid.NewRandom().String())
	id2 := ID(uuid.NewRandom().String())
	q, err := tm.CreateSubscription(vk, id, nss+"/foo/+")
	require.NoError(t, err)
	q2, err := tm.CreateSubscription(vk, id2, nss+"/*")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	c1 := mkConsumer(t, ctx, q, 2)
	c2 := mkConsumer(t, ctx, q2, 3)

	m1 := mkmsg()
	m1.Namespace = ns
	m1.Uri = "foo/bar"
	tm.Publish(m1)

	m2 := mkmsg()
	m2.Namespace = ns
	m2.Uri = "foo/baz"
	tm.Publish(m2)

	m3 := mkmsg()
	m3.Namespace = ns
	m3.Uri = "bar/buz"
	tm.Publish(m3)

	time.Sleep(100 * time.Millisecond)
	cancel()
	require.True(t, <-c1)
	require.True(t, <-c2)
}
