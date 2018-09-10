package core

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/immesys/wave/eapi"
	eapipb "github.com/immesys/wave/eapi/pb"
	"github.com/immesys/wave/iapi"
	"github.com/immesys/wave/localdb/lls"
	"github.com/immesys/wave/localdb/poc"
	"github.com/immesys/wave/storage/overlay"
	"github.com/immesys/wave/waved"
	"github.com/immesys/wave/wve"
	pb "github.com/immesys/wavemq/mqpb"
	"golang.org/x/crypto/sha3"
)

const WAVEMQPermissionSet = "\x4a\xd2\x3f\x5f\x6e\x73\x17\x38\x98\xef\x51\x8c\x6a\xe2\x7a\x7f\xcf\xf4\xfe\x9b\x86\xa3\xf1\xa2\x08\xc4\xde\x9e\xac\x95\x39\x6b"
const WAVEMQPublish = "publish"
const WAVEMQSubscribe = "subscribe"

type AuthModule struct {
	cfg     *waved.Configuration
	wave    *eapi.EAPI
	cachemu sync.Mutex
	cache   map[cacheKey]time.Time
}

type cacheKey struct {
	Namespace  [32]byte
	URI        string
	Permission string
	ProofHash  [32]byte
}

func NewAuthModule(cfg *waved.Configuration) (*AuthModule, error) {
	llsdb, err := lls.NewLowLevelStorage(cfg.Database)
	if err != nil {
		return nil, err
	}
	si, err := overlay.NewOverlay(cfg.Storage)
	if err != nil {
		fmt.Printf("storage overlay error: %v\n", err)
		os.Exit(1)
	}
	iapi.InjectStorageInterface(si)
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	//TODO cache
	resp, err := am.wave.VerifyProof(ctx, &eapipb.VerifyProofParams{
		ProofDER: m.ProofDER,
		RequiredRTreePolicy: &eapipb.RTreePolicy{
			Namespace: m.Tbs.Namespace,
			Statements: []*eapipb.RTreePolicyStatement{
				{
					PermissionSet: []byte(WAVEMQPermissionSet),
					Permissions:   []string{WAVEMQPublish},
					Resource:      m.Tbs.Uri,
				},
			},
		},
	})
	cancel()
	if err != nil {
		return wve.ErrW(wve.InternalError, "could not validate proof", err)
	}
	if resp.Error != nil {
		return wve.Err(wve.ProofInvalid, resp.Error.Message)
	}
	return nil
}

//Check that the given proof is valid for subscription on the given URI
func (am *AuthModule) CheckSubscription(s *pb.PeerSubscribeParams) wve.WVE {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	resp, err := am.wave.VerifyProof(ctx, &eapipb.VerifyProofParams{
		ProofDER: s.ProofDER,
		RequiredRTreePolicy: &eapipb.RTreePolicy{
			Namespace: s.Tbs.Namespace,
			Statements: []*eapipb.RTreePolicyStatement{
				{
					PermissionSet: []byte(WAVEMQPermissionSet),
					Permissions:   []string{WAVEMQSubscribe},
					Resource:      s.Tbs.Uri,
				},
			},
		},
	})
	cancel()
	if err != nil {
		return wve.ErrW(wve.InternalError, "could not validate proof", err)
	}
	if resp.Error != nil {
		return wve.Err(wve.ProofInvalid, resp.Error.Message)
	}

	//If the user did not specify an absolute expiry, or specified one greater than
	//the proof allows, then set the field to the proof's expiry
	if s.AbsoluteExpiry == 0 || s.AbsoluteExpiry > resp.Result.Expiry {
		s.AbsoluteExpiry = resp.Result.Expiry
	}
	return nil
}

func (am *AuthModule) PrepareMessage(s *pb.SubscribeParams, m *pb.Message) (*pb.Message, wve.WVE) {
	//Decrypt the message with the perspective given in the subscribe params. Copies the message
	//and returns it.
	//TODO decryption
	return m, nil
}

func (am *AuthModule) FormMessage(p *pb.PublishParams, routerID string) (*pb.Message, wve.WVE) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	//TODO cache

	perspective := &eapipb.Perspective{
		EntitySecret: &eapipb.EntitySecret{
			DER:        p.Perspective.EntitySecret.DER,
			Passphrase: p.Perspective.EntitySecret.Passphrase,
		},
	}
	proofresp, err := am.wave.BuildRTreeProof(ctx, &eapipb.BuildRTreeProofParams{
		Perspective: perspective,
		Namespace:   p.Namespace,
		Statements: []*eapipb.RTreePolicyStatement{
			{
				PermissionSet: []byte(WAVEMQPermissionSet),
				Permissions:   []string{WAVEMQPublish},
				Resource:      p.Uri,
			},
		},
		ResyncFirst: true,
	})
	cancel()
	if err != nil {
		return nil, wve.ErrW(wve.NoProofFound, "failed to build", err)
	}
	if proofresp.Error != nil {
		return nil, wve.Err(wve.NoProofFound, proofresp.Error.Message)
	}
	todo
	hash := sha3.New256()
	hash.Write(p.SourceEntity)
	hash.Write(p.namespace)
	hash.Write([]byte(m.Tbs.Uri))
	for _, po := range m.Tbs.Payload {
		hash.Write([]byte(po.Schema))
		hash.Write(po.Content)
	}
	hash.Write([]byte(m.Tbs.OriginRouter))
	digest := hash.Sum(nil)

	signresp, err := am.wave.Sign(context.Background(), &eapipb.SignParams{
		Perspective: perspective,
		Content:     digest,
	})
	if err != nil {
		return nil, wve.ErrW(wve.InvalidSignature, "failed to sign", err)
	}
	if signresp.Error != nil {
		return nil, wve.ErrW(wve.InvalidSignature, signresp.Error.Message)
	}

	return &pb.Message{
		ProofDER:  proofresp.ProofDER,
		Signature: signresp.Signature,
		Persist:   p.Persist,
		Tbs: &pb.MessageTBS{
			Namespace:    p.Namespace,
			Uri:          p.Uri,
			Payload:      p.Content,
			OriginRouter: routerID,
		},
	}, nil
}

func (am *AuthModule) FormSubRequest(p *pb.SubscribeParams, routerID string) (*pb.PeerSubscribeParams, wve.WVE) {
	//TODO
	return &pb.PeerSubscribeParams{
		Tbs: &pb.PeerSubscriptionTBS{
			Namespace: p.Namespace,
			Uri:       p.Uri,
			Id:        p.Identifier,
			RouterID:  routerID,
		},
	}, nil
}
