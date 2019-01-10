package server

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/immesys/wave/wve"
	"github.com/immesys/wavemq/core"
	pb "github.com/immesys/wavemq/mqpb"
	logging "github.com/op/go-logging"
	"github.com/pborman/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip"
)

var lg = logging.MustGetLogger("main")

//Some instrumentation
var pmFailedFormMessage = prometheus.NewCounter(prometheus.CounterOpts{
	Subsystem: "localserver",
	Name:      "failed_form_message",
	Help:      "Number of messages that could not build a proof",
})
var pmFailedFormQuery = prometheus.NewCounter(prometheus.CounterOpts{
	Subsystem: "localserver",
	Name:      "failed_form_query",
	Help:      "Number of query ops that could not build a proof",
})
var pmFailedFormSubscribe = prometheus.NewCounter(prometheus.CounterOpts{
	Subsystem: "localserver",
	Name:      "failed_form_subscribe",
	Help:      "Number of subscribe ops that could not build a proof",
})
var pmLinkDownQuery = prometheus.NewCounter(prometheus.CounterOpts{
	Subsystem: "localserver",
	Name:      "failed_linkdown_query",
	Help:      "Number of query ops that failed to to uplink down",
})
var pmFailedProofs = prometheus.NewCounter(prometheus.CounterOpts{
	Subsystem: "localserver",
	Name:      "failed_incoming_proof",
	Help:      "Number of subscribe/query messages dropped due to bad proofs",
})
var pmFailedDecryption = prometheus.NewCounter(prometheus.CounterOpts{
	Subsystem: "localserver",
	Name:      "failed_incoming_decryption",
	Help:      "Number of subscribe/query messages dropped due to failed decryption",
})

func init() {
	prometheus.MustRegister(pmFailedFormMessage)
	prometheus.MustRegister(pmFailedFormQuery)
	prometheus.MustRegister(pmFailedFormSubscribe)
	prometheus.MustRegister(pmLinkDownQuery)
	prometheus.MustRegister(pmFailedProofs)
	prometheus.MustRegister(pmFailedDecryption)
}

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
	fmt.Printf("Listening for local connections on %s\n", cfg.ListenAddr)
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

func (s *srv) ConnectionStatus(ctx context.Context, p *pb.ConnectionStatusParams) (*pb.ConnectionStatusResponse, error) {
	a, b := s.tm.ConnectionStatus()
	return &pb.ConnectionStatusResponse{
		TotalPeers:     int32(b),
		ConnectedPeers: int32(a),
	}, nil
}

func (s *srv) Publish(ctx context.Context, p *pb.PublishParams) (*pb.PublishResponse, error) {
	m, err := s.am.FormMessage(p, s.tm.RouterID())
	if err != nil {
		pmFailedFormMessage.Add(1)
		return &pb.PublishResponse{
			Error: ToError(err),
		}, nil
	}
	s.tm.Publish(m)
	return &pb.PublishResponse{}, nil
}

func (s *srv) Query(p *pb.QueryParams, r pb.WAVEMQ_QueryServer) error {
	qm, err := s.am.FormQueryRequest(p, s.tm.RouterID())
	if err != nil {
		pmFailedFormQuery.Add(1)
		r.Send(&pb.QueryMessage{
			Error: ToError(err),
		})
		return nil
	}

	nsString := base64.URLEncoding.EncodeToString(p.Namespace)
	drconn := s.tm.GetDesignatedRouterConnection(nsString)
	if drconn == nil {
		pmLinkDownQuery.Add(1)
		r.Send(&pb.QueryMessage{
			Error: ToError(wve.Err(core.DesignatedRouterLinkDown, "could not query: designated router link down")),
		})
		return nil
	}

	peer := pb.NewWAVEMQPeeringClient(drconn.Conn)
	client, uerr := peer.PeerQueryRequest(r.Context(), qm)
	if uerr != nil {
		r.Send(&pb.QueryMessage{
			Error: ToError(wve.ErrW(core.DesignatedRouterLinkDown, "could not query", uerr)),
		})
		return nil
	}
	for {
		msg, err := client.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if msg.Error != nil {
			r.Send(&pb.QueryMessage{
				Error: ToError(wve.Err(core.UpstreamError, "could not query: "+msg.Error.Message)),
			})
			return nil
		}

		if msg.Message == nil {
			panic("no message but no error?")
		}

		//Validate the message
		err = s.am.CheckMessage(msg.Message)
		if err != nil {
			pmFailedProofs.Add(1)
			lg.Info("dropping query message: %v", err)
			continue
		}

		//prepare message
		pmsg, err := s.am.PrepareMessage(p.Perspective, msg.Message)
		if err != nil {
			pmFailedDecryption.Add(1)
			lg.Info("dropping query message: %v", err)
			continue
		}

		uerr := r.Send(&pb.QueryMessage{Message: pmsg})
		if uerr != nil {
			return uerr
		}
	}
}
func (s *srv) Subscribe(p *pb.SubscribeParams, r pb.WAVEMQ_SubscribeServer) error {
	if p.Expiry < 60 {
		p.Expiry = 60
	}
	if p.Identifier == "" {
		p.Identifier = uuid.NewRandom().String()
	}
	sub, err := s.am.FormSubRequest(p, s.tm.RouterID())
	if err != nil {
		pmFailedFormSubscribe.Add(1)
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
			it = pb.ShallowCloneMessageForDrops(it)
			it.Drops = append(it.Drops, q.Drops())
			err := s.am.CheckMessage(it)
			if err != nil {
				pmFailedProofs.Add(1)
				lg.Infof("dropping message in subscribe %q due to invalid proof", it.Tbs.Uri)
				continue
			}

			it, err = s.am.PrepareMessage(p.Perspective, it)
			if err != nil {
				pmFailedDecryption.Add(1)
				lg.Info("dropping message in subscribe %q: could not prepare: %v", it.Tbs.Uri, err.Reason())
				continue
			}
			uerr := r.Send(&pb.SubscriptionMessage{
				Message: it,
			})
			if uerr != nil {
				return uerr
			}
		}
	}
}

func (s *srv) Shutdown() {

}
