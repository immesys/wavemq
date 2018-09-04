package core

import (
	"sync"
	"time"

	"github.com/immesys/wave/eapi"
	"github.com/immesys/wave/localdb/lls"
	"github.com/immesys/wave/localdb/poc"
	"github.com/immesys/wave/waved"
	"github.com/immesys/wave/wve"
	"github.com/immesys/wavemq/pb"
)

type AuthModule struct {
	cfg     *AuthConfig
	wave    *eapi.EAPI
	cachemu sync.Mutex
	cache   map[cacheKey]time.Time
}

type AuthConfig struct {
	WaveConfig *waved.Configuration
}

type cacheKey struct {
	Namespace  [32]byte
	URI        string
	Permission string
	ProofHash  [32]byte
}

func NewAuthModule(cfg *AuthConfig) (*AuthModule, error) {
	llsdb, err := lls.NewLowLevelStorage(cfg.WaveConfig.Database)
	if err != nil {
		return nil, err
	}
	ws := poc.NewPOC(llsdb)
	eapi := eapi.NewEAPI(ws)
	return &AuthModule{
		cfg:   cfg,
		wave:  eapi,
		cache: make(map[cacheKey]time.Time),
	}, nil
}

//This checks that a publish message is authorized for the given URI
func (am *AuthModule) CheckMessage(m *pb.Message) wve.WVE {
	//TODO
	return nil
}

//Check that the given proof is valid for subscription on the given URI
func (am *AuthModule) CheckSubscription(s *pb.PeerSubscription) wve.WVE {
	//TODO
	return nil
}

func (am *AuthModule) FormMessage(p *pb.PublishParams) (*pb.Message, wve.WVE) {
	//TODO
	return &pb.Message{
		Tbs: &pb.MessageTBS{
			Namespace: p.Namespace,
			Uri:       p.Uri,
			Payload:   p.Content,
		},
		Persist: p.Persist,
	}, nil
}

func (am *AuthModule) FormSubRequest(p *pb.SubscribeParams) (*pb.PeerSubscription, wve.WVE) {
	//TODO
	return &pb.PeerSubscription{
		Tbs: &pb.PeerSubscriptionTBS{
			Namespace: p.Namespace,
			Uri:       p.Uri,
			Id:        p.Identifier,
		},
	}, nil
}
