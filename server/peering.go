package server

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/immesys/wave/wve"
	"github.com/immesys/wavemq/core"
	pb "github.com/immesys/wavemq/mqpb"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
	_ "google.golang.org/grpc/encoding/gzip"
)

//Some instrumentation
var pmFailedQuery = prometheus.NewCounter(prometheus.CounterOpts{
	Subsystem: "peerserver",
	Name:      "failed_query",
	Help:      "Number of query requests that failed proof",
})
var pmFailedSubscribe = prometheus.NewCounter(prometheus.CounterOpts{
	Subsystem: "peerserver",
	Name:      "failed_subscribe",
	Help:      "Number of subscribe requests that failed proof",
})
var pmFailedPublish = prometheus.NewCounter(prometheus.CounterOpts{
	Subsystem: "peerserver",
	Name:      "failed_publish",
	Help:      "Number of publish requests that failed proof",
})

func init() {
	prometheus.MustRegister(pmFailedQuery)
	prometheus.MustRegister(pmFailedSubscribe)
	prometheus.MustRegister(pmFailedPublish)

	encoding.RegisterCompressor(encoding.GetCompressor("gzip"))
}

type peerServer struct {
	tm         *core.Terminus
	am         *core.AuthModule
	grpcServer *grpc.Server
}

type PeerServerConfig struct {
	ListenAddr string
}

func NewPeerServer(tm *core.Terminus, am *core.AuthModule, cfg *PeerServerConfig) ShutdownAble {
	//TODO add the code for verifying key exchange
	fmt.Printf("Listening for peering connections on %s\n", cfg.ListenAddr)
	l, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		panic(err)
	}
	grpcServer := grpc.NewServer(grpc.Creds(tm.ServerTransportCredentials()))
	s := &peerServer{
		tm:         tm,
		am:         am,
		grpcServer: grpcServer,
	}
	pb.RegisterWAVEMQPeeringServer(grpcServer, s)
	go grpcServer.Serve(l)
	return s
}

func (s *peerServer) PeerQueryRequest(p *pb.PeerQueryParams, r pb.WAVEMQPeering_PeerQueryRequestServer) error {
	err := s.am.CheckQuery(p)
	if err != nil {
		pmFailedQuery.Add(1)
		r.Send(&pb.QueryMessage{
			Error: ToError(err),
		})
		return nil
	}

	//Lets execute the request
	rchan := s.tm.Query(p.Namespace, p.Uri)
	//Rchan must be completely consumed, lets ensure that happens
	defer func() {
		for _ = range rchan {
		}
	}()
	for e := range rchan {
		if e.Error != nil {
			return r.Send(&pb.QueryMessage{
				Error: ToError(e.Error),
			})
		}
		uerr := r.Send(&pb.QueryMessage{
			Message: e.Msg,
		})
		if uerr != nil {
			return uerr
		}
	}
	return nil
}

func (s *peerServer) PeerPublish(ctx context.Context, p *pb.PeerPublishParams) (*pb.PeerPublishResponse, error) {
	err := s.am.CheckMessage(p.Msg)
	if err != nil {
		pmFailedPublish.Add(1)
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
		pmFailedSubscribe.Add(1)
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
			it = pb.ShallowCloneMessageForDrops(it)
			it.Drops = append(it.Drops, q.Drops())
			// err := s.am.CheckMessage(it)
			// if err != nil {
			// 	fmt.Printf("dropping message due to invalid proof\n")
			// 	continue
			// }
			//We don't prepare messages sent to peers
			// m, err := s.am.PrepareMessage(p, m)
			// if err != nil {
			// 	fmt.Printf("dropping message, could not prepare: %v\n", err)
			// 	continue
			// }
			r.Send(&pb.SubscriptionMessage{
				Message: it,
			})
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
