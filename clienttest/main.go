package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/davecgh/go-spew/spew"
	pb "github.com/immesys/wavemq/mqpb"
	"google.golang.org/grpc"
)

//todo mkdir -p for databases
//change publish namespace to string
func main() {
	go clienta()
	//go clientb()
	time.Sleep(1 * time.Second)
	conn, err := grpc.Dial("127.0.0.1:7012", grpc.WithInsecure(), grpc.FailOnNonTempDialError(true), grpc.WithBlock())
	if err != nil {
		panic(err)
	}
	client := pb.NewWAVEMQClient(conn)
	ctx := context.Background()
	ns, _ := base64.URLEncoding.DecodeString("GyBzLKTkBE4a7tPqGjHMQ_VDgqSQRSVafAyUYcURg5scAg==")
	resp, err := client.Publish(ctx, &pb.PublishParams{
		Namespace: ns,
		Uri:       "foo/bar2",
		Content:   []*pb.PayloadObject{{Schema: "hello", Content: []byte("world")}},
	})
	if err != nil {
		panic(err)
	}
	spew.Dump(resp)
	time.Sleep(5 * time.Second)
}

func clienta() {
	conn, err := grpc.Dial("127.0.0.1:7012", grpc.WithInsecure(), grpc.FailOnNonTempDialError(true), grpc.WithBlock())
	if err != nil {
		panic(err)
	}
	client := pb.NewWAVEMQClient(conn)
	ns, _ := base64.URLEncoding.DecodeString("GyBzLKTkBE4a7tPqGjHMQ_VDgqSQRSVafAyUYcURg5scAg==")
	cl, err := client.Subscribe(context.Background(), &pb.SubscribeParams{
		Namespace:  ns,
		Uri:        "foo/*",
		Identifier: "hai",
	})
	if err != nil {
		panic(err)
	}
	for {
		sm, err := cl.Recv()
		if err != nil {
			panic(err)
		}
		fmt.Printf("CLIENT A: %v\n", sm.Message.Tbs.Uri)
	}
}
func clientb() {
	conn, err := grpc.Dial("127.0.0.1:7002", grpc.WithInsecure(), grpc.FailOnNonTempDialError(true), grpc.WithBlock())
	if err != nil {
		panic(err)
	}
	client := pb.NewWAVEMQClient(conn)
	ns, _ := base64.URLEncoding.DecodeString("GyBzLKTkBE4a7tPqGjHMQ_VDgqSQRSVafAyUYcURg5scAg==")
	cl, err := client.Subscribe(context.Background(), &pb.SubscribeParams{
		Namespace:  ns,
		Uri:        "foo/*",
		Identifier: "hai2",
	})
	if err != nil {
		panic(err)
	}
	for {
		sm, err := cl.Recv()
		if err != nil {
			panic(err)
		}
		fmt.Printf("CLIENT A: %v\n", sm.Message.Tbs.Uri)
	}
}
