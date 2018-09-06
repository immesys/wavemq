package server

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/immesys/wave/wve"
	"github.com/immesys/wavemq/core"
	pb "github.com/immesys/wavemq/mqpb"
	"google.golang.org/grpc"
)

// Implement AuthModule methods
// Local pub/sub should work
// Implement peer subscription in terminus
// Implement * to DR in terminus
// agent -> dr -> agent should work

type peerServer struct {
	tm         *core.Terminus
	am         *core.AuthModule
	grpcServer *grpc.Server
}

type PeerServerConfig struct {
	ListenAddr string
}

func NewPeerServer(tm *core.Terminus, am *core.AuthModule, cfg *PeerServerConfig) ShutdownAble {
	fmt.Printf("Listening on %s\n", cfg.ListenAddr)
	l, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		panic(err)
	}
	grpcServer := grpc.NewServer()
	s := &peerServer{
		tm:         tm,
		am:         am,
		grpcServer: grpcServer,
	}
	pb.RegisterWAVEMQPeeringServer(grpcServer, s)
	go grpcServer.Serve(l)
	return s
}

func (s *peerServer) PeerPublish(ctx context.Context, p *pb.PeerPublishParams) (*pb.PeerPublishResponse, error) {
	err := s.am.CheckMessage(p.Msg)
	if err != nil {
		return &pb.PeerPublishResponse{
			Error: ToError(err),
		}, nil
	}
	s.tm.Publish(p.Msg)
	return &pb.PeerPublishResponse{}, nil
}
func (s *peerServer) PeerSubscribe(p *pb.PeerSubscribeParams, r pb.WAVEMQPeering_PeerSubscribeServer) error {
	err := s.am.CheckSubscription(p)
	if err != nil {
		senderr := r.Send(&pb.SubscriptionMessage{
			Error: ToError(err),
		})
		if senderr != nil {
			return senderr
		}
		return nil
	}

	q, err := s.tm.CreateSubscription(p)
	if err != nil {
		senderr := r.Send(&pb.SubscriptionMessage{
			Error: ToError(err),
		})
		if senderr != nil {
			return senderr
		}
		return nil
	}
	notify := make(chan struct{}, 5)
	q.SubscribeNotifications(&core.NotificationSubscriber{
		Ctx:    r.Context(),
		Notify: notify,
	})
	notify <- struct{}{} //Run through once
	//Dequeueing resets the un-drained queue timer. We need to call dequeue
	//every now and then even if there is no data
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-notify:
		case <-r.Context().Done():
		case <-q.Ctx.Done():
		case <-ticker.C:
		}
		if q.Ctx.Err() != nil {
			r.Send(&pb.SubscriptionMessage{
				Error: ToError(wve.Err(core.Unsubscribed, "subscription has ended")),
			})
			return nil
		}
		if r.Context().Err() != nil {
			return nil
		}
		for {
			it := q.Dequeue()
			if it == nil {
				break
			}
			err := s.am.CheckMessage(it)
			if err != nil {
				fmt.Printf("dropping message due to invalid proof\n")
			} else {
				r.Send(&pb.SubscriptionMessage{
					Message: it,
				})
			}
		}
	}
}

func (s *peerServer) PeerUnsubscribe(ctx context.Context, p *pb.PeerUnsubscribeParams) (*pb.PeerUnsubscribeResponse, error) {
	err := s.tm.Unsubscribe(p.SourceEntity, p.Id)
	if err != nil {
		return &pb.PeerUnsubscribeResponse{
			Error: ToError(err),
		}, nil
	}
	return &pb.PeerUnsubscribeResponse{}, nil
}

func (s *peerServer) Shutdown() {

}
