package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"time"

	"github.com/davecgh/go-spew/spew"
	pb "github.com/immesys/wavemq/mqpb"
	"google.golang.org/grpc"
)

const namespace = "GyBfHOKpk7MJJZJDatu1EBQH5wbldv1zkWVBhgsGFEniSQ=="

//todo mkdir -p for databases
//change publish namespace to string
func main() {

	perspcontents, err := ioutil.ReadFile("clientperspective.ent")
	if err != nil {
		panic(err)
	}
	persp := &pb.Perspective{
		EntitySecret: &pb.EntitySecret{
			DER: perspcontents,
		},
	}

	go clienta(persp)
	go clientb(persp)
	time.Sleep(1 * time.Second)
	conn, err := grpc.Dial("127.0.0.1:7012", grpc.WithInsecure(), grpc.FailOnNonTempDialError(true), grpc.WithBlock())
	if err != nil {
		panic(err)
	}

	client := pb.NewWAVEMQClient(conn)
	ctx := context.Background()
	ns, _ := base64.URLEncoding.DecodeString(namespace)
	_, err = client.Publish(ctx, &pb.PublishParams{
		Perspective: persp,
		Namespace:   ns,
		Uri:         "foo/bar2",
		Content:     []*pb.PayloadObject{{Schema: "hello", Content: []byte("world")}},
		Persist:     true,
	})
	if err != nil {
		panic(err)
	}

	srv, err := client.Query(ctx, &pb.QueryParams{
		Perspective: persp,
		Namespace:   ns,
		Uri:         "foo/*",
	})
	if err != nil {
		panic(err)
	}
	for {
		m, err := srv.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			panic(err)
		}
		if m.Error != nil {
			spew.Dump(m.Error)
			panic("err")
		}
		fmt.Printf("got qm: %s\n", m.Message.Tbs.Uri)
	}
}

func clienta(persp *pb.Perspective) {
	//This is the client
	conn, err := grpc.Dial("127.0.0.1:7012", grpc.WithInsecure(), grpc.FailOnNonTempDialError(true), grpc.WithBlock())
	if err != nil {
		panic(err)
	}
	client := pb.NewWAVEMQClient(conn)
	ns, _ := base64.URLEncoding.DecodeString(namespace)
	cl, err := client.Subscribe(context.Background(), &pb.SubscribeParams{
		Perspective: persp,
		Namespace:   ns,
		Uri:         "foo/*",
		Identifier:  "hai",
	})
	if err != nil {
		panic(err)
	}
	for {
		sm, err := cl.Recv()
		if err != nil {
			panic(err)
		}
		if sm.Error != nil {
			panic(sm.Error.Message)
		}
		fmt.Printf("CLIENT A: %v\n", sm.Message.Tbs.Uri)
	}
}
func clientb(persp *pb.Perspective) {
	//This is the DR
	conn, err := grpc.Dial("127.0.0.1:7002", grpc.WithInsecure(), grpc.FailOnNonTempDialError(true), grpc.WithBlock())
	if err != nil {
		panic(err)
	}
	client := pb.NewWAVEMQClient(conn)
	ns, _ := base64.URLEncoding.DecodeString(namespace)
	cl, err := client.Subscribe(context.Background(), &pb.SubscribeParams{
		Perspective: persp,
		Namespace:   ns,
		Uri:         "foo/*",
		Identifier:  "hai2",
		Expiry:      67,
	})
	if err != nil {
		panic(err)
	}
	for {
		sm, err := cl.Recv()
		if err != nil {
			panic(err)
		}
		fmt.Printf("CLIENT B: %v\n", sm.Message.Tbs.Uri)
	}
}
