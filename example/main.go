package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/immesys/wavemq/mqpb"
	"github.com/pborman/uuid"
	"google.golang.org/grpc"
)

// This is an example that shows how to publish and subscribe to a WAVEMQ site router
// Fill these fields in:
const EntityFile = "entity.ent"
const Namespace = "GyAlyQyfJuai4MCyg6Rx9KkxnZZXWyDaIo0EXGY9-WEq6w=="
const SiteRouter = "127.0.0.1:4516"

var namespaceBytes []byte

func main() {
	var err error
	namespaceBytes, err = base64.URLEncoding.DecodeString(Namespace)
	if err != nil {
		fmt.Printf("failed to decode namespace: %v\n", err)
		os.Exit(1)
	}

	// Load the WAVE3 entity that will be used
	perspective, err := ioutil.ReadFile(EntityFile)
	if err != nil {
		fmt.Printf("could not load entity %q, you might need to create one and grant it permissions\n", EntityFile)
		os.Exit(1)
	}

	// Establish a GRPC connection to the site router.
	conn, err := grpc.Dial(SiteRouter, grpc.WithInsecure(), grpc.FailOnNonTempDialError(true), grpc.WithBlock())
	if err != nil {
		fmt.Printf("could not connect to the site router: %v\n", err)
		os.Exit(1)
	}

	// Create the WAVEMQ client
	client := mqpb.NewWAVEMQClient(conn)

	go subscribe(client, perspective)

	//Publish five messages, then exit
	for i := 0; i < 5; i++ {
		time.Sleep(1 * time.Second)
		presp, err := client.Publish(context.Background(), &mqpb.PublishParams{
			Perspective: &mqpb.Perspective{
				EntitySecret: &mqpb.EntitySecret{
					DER: perspective,
				},
			},
			Namespace:           namespaceBytes,
			Uri:                 "example/topic",
			Content:             []*mqpb.PayloadObject{{Schema: "text", Content: []byte("world")}},
			EncryptionPartition: [][]byte{[]byte("example"), []byte("topic")},
		})
		if err != nil {
			fmt.Printf("publish error: %v\n", err)
			os.Exit(1)
		}
		if presp.Error != nil {
			fmt.Printf("publish error: %v\n", presp.Error.Message)
			os.Exit(1)
		}
	}
	time.Sleep(2 * time.Second)
}

func subscribe(client mqpb.WAVEMQClient, perspective []byte) {

	sub, err := client.Subscribe(context.Background(), &mqpb.SubscribeParams{
		Perspective: &mqpb.Perspective{
			EntitySecret: &mqpb.EntitySecret{
				DER: perspective,
			},
		},
		Namespace: namespaceBytes,
		Uri:       "example/*",
		//If you want a persistent subscription between different runs of this program,
		//specify this to be something constant (but unique)
		Identifier: uuid.NewRandom().String(),
		//This subscription will automatically unsubscribe one minute after this
		//program ends
		Expiry: 60,
	})
	if err != nil {
		fmt.Printf("subscribe error: %v\n", err)
		os.Exit(1)
	}
	for {
		m, err := sub.Recv()
		if err != nil {
			fmt.Printf("subscribe error: %v\n", err)
			os.Exit(1)
		}
		if m.Error != nil {
			fmt.Printf("subscribe error: %v\n", m.Error.Message)
			os.Exit(1)
		}
		fmt.Printf("received message on URI: %s\n", m.Message.Tbs.Uri)
		fmt.Printf("  contents:\n")
		for _, po := range m.Message.Tbs.Payload {
			fmt.Printf("    schema=%q content=%q\n", po.Schema, po.Content)
		}
	}
}
