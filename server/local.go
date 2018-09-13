package server

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/immesys/wave/wve"
	"github.com/immesys/wavemq/core"
	pb "github.com/immesys/wavemq/mqpb"
	logging "github.com/op/go-logging"
	"google.golang.org/grpc"
)

var lg = logging.MustGetLogger("main")

// On the roles of this daemon.
// A running wavemq daemon can be in a bunch of different roles:
// - A designated router: there is only one per namespace and it should
//   be running somewhere with good internet connectivity. This is the
//   canonical location of persisted messages and all published messages
//   will be delivered here
// - A local router. This is part of the namespace and must be authorized
//   by the namespace entity. Subscription and publish requests will be
//   validated here, enabling local autonomous function if connectivity
//   to the DR is down
// Future work:
// - An agent. This will generate proofs for pub/sub and forward these
//   to either an LR or a DR. This does not perform local routing but
//   allows for clients to not have to send their private keys to the
//   LR or DR.

//OR

//- Designated router, all traffic goes here, same as before. The DR
//  will re-validate the proofs of all published messages and requires
//  proofs for every subscription.
//- Local Router / Agent combo: you give it your entity but it is
//  not a trusted part of the namespace. To "bridge" with the DR
//  it carefully uses subscription proofs furnished by client entities
//  that are subscribing (but still only uses one if there is a duplicate
//  subscription). Having access to the private keys of the entities
//  doing the subscription, it can also generate a signature of some
//  ephemeral session token with the bridged router to ensure no
//  replay of the subscription message.

//Checklist:

// Implement AuthModule methods
// Local pub/sub should work
// Implement peer subscription in terminus
// Implement * to DR in terminus
// agent -> dr -> agent should work
type srv struct {
	tm         *core.Terminus
	am         *core.AuthModule
	grpcServer *grpc.Server
}

func ToError(e wve.WVE) *pb.Error {
	if e == nil {
		return nil
	}
	return &pb.Error{
		Code:    int32(e.Code()),
		Message: e.Error(),
	}
}

type LocalServerConfig struct {
	ListenAddr string
}

type ShutdownAble interface {
	Shutdown()
}

func NewLocalServer(tm *core.Terminus, am *core.AuthModule, cfg *LocalServerConfig) ShutdownAble {
	fmt.Printf("Listening on %s\n", cfg.ListenAddr)
	l, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		panic(err)
	}
	grpcServer := grpc.NewServer()
	s := &srv{
		tm:         tm,
		am:         am,
		grpcServer: grpcServer,
	}
	pb.RegisterWAVEMQServer(grpcServer, s)
	go grpcServer.Serve(l)
	return s
}

func (s *srv) Publish(ctx context.Context, p *pb.PublishParams) (*pb.PublishResponse, error) {
	m, err := s.am.FormMessage(p, s.tm.RouterID())
	if err != nil {
		return &pb.PublishResponse{
			Error: ToError(err),
		}, nil
	}
	s.tm.Publish(m)
	return &pb.PublishResponse{}, nil
}

func (s *srv) Subscribe(p *pb.SubscribeParams, r pb.WAVEMQ_SubscribeServer) error {
	sub, err := s.am.FormSubRequest(p, s.tm.RouterID())
	if err != nil {
		lg.Infof("failed to subscribe to %q: %s", p.Uri, err)
		r.Send(&pb.SubscriptionMessage{
			Error: ToError(err),
		})
		return nil
	}
	q, uerr := s.tm.CreateSubscription(sub)
	if uerr != nil {
		lg.Infof("failed to subscribe to %q: %s", p.Uri, uerr)
		r.Send(&pb.SubscriptionMessage{
			Error: ToError(wve.ErrW(core.SubscriptionFailed, "could not subscribe", uerr)),
		})
		return nil
	}
	notify := make(chan struct{}, 5)
	q.SubscribeNotifications(&core.NotificationSubscriber{
		Notify: notify,
		//When this client disconnects, stop receiving these notifications
		Ctx: r.Context(),
	})
	notify <- struct{}{} //Run through once
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
				lg.Infof("dropping message on %q due to invalid proof", m.Tbs.Uri)
			} else {
				fmt.Printf("sending\n")
				err := r.Send(&pb.SubscriptionMessage{
					Message: it,
				})
				if err != nil {
					return err
				}
			}
		}
	}
}

func (s *srv) Shutdown() {

}
