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

const WAVEMQPermissionSet = "\x1b\x20\x14\x33\x74\xb3\x2f\xd2\x74\x39\x54\xfe\x47\x86\xf6\xcf\x86\xd4\x03\x72\x0f\x5e\xc4\x42\x36\xb6\x58\xc2\x6a\x1e\x68\x0f\x6e\x01"
const WAVEMQPublish = "publish"
const WAVEMQSubscribe = "subscribe"

const ProofMaxCacheTime = 6 * time.Hour

type AuthModule struct {
	cfg  *waved.Configuration
	wave *eapi.EAPI

	// the Incoming cache stores the time that a given proof must be
	// revalidated
	icachemu sync.Mutex
	icache   map[icacheKey]*icacheItem

	// the Build cache stores the results of proof build operations
	bcachemu sync.Mutex
	bcache   map[bcacheKey]*bcacheItem
}

type icacheKey struct {
	Namespace  [32]byte
	Entity     [32]byte
	URI        string
	Permission string
	ProofHash  [32]byte
}
type icacheItem struct {
	expires time.Time
	valid   bool
}

type bcacheKey struct {
	Namespace  [32]byte
	Target     [32]byte
	PolicyHash [32]byte
}
type bcacheItem struct {
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
		cfg:    cfg,
		wave:   eapi,
		icache: make(map[icacheKey]*icacheItem),
		bcache: make(map[bcacheKey]*bcacheItem),
	}, nil
}

//This checks that a publish message is authorized for the given URI
func (am *AuthModule) CheckMessage(m *pb.Message) wve.WVE {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	//Check the signature
	hash := sha3.New256()
	hash.Write(m.Tbs.Namespace)
	hash.Write([]byte(m.Tbs.Uri))
	for _, po := range m.Tbs.Payload {
		hash.Write([]byte(po.Schema))
		hash.Write(po.Content)
	}
	hash.Write([]byte(m.Tbs.OriginRouter))
	digest := hash.Sum(nil)
	resp, err := am.wave.VerifySignature(ctx, &eapipb.VerifySignatureParams{
		Signer: m.Tbs.SourceEntity,
		//Todo signer location
		Signature: m.Signature,
		Content:   digest,
	})
	if err != nil {
		return wve.ErrW(wve.InvalidSignature, "could not validate signature", err)
	}
	if resp.Error != nil {
		return wve.Err(wve.InvalidSignature, "failed to validate message signature: "+resp.Error.Message)
	}

	//Now check the proof
	ick := icacheKey{}
	copy(ick.Namespace[:], m.Tbs.Namespace)
	copy(ick.Entity[:], m.Tbs.SourceEntity)
	ick.URI = m.Tbs.Uri
	ick.Permission = WAVEMQPublish
	h := sha3.NewShake256()
	h.Write(m.ProofDER)
	h.Read(ick.ProofHash[:])

	am.icachemu.Lock()
	entry, ok := am.icache[ick]
	am.icachemu.Unlock()
	if ok && entry.expires.After(time.Now()) {
		if entry.valid {
			return nil
		}
		return wve.Err(wve.ProofInvalid, "this proof has been cached as invalid\n")
	}

	presp, err := am.wave.VerifyProof(ctx, &eapipb.VerifyProofParams{
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
	if presp.Error != nil {
		am.icachemu.Lock()
		am.icache[ick] = &icacheItem{
			expires: time.Now().Add(ProofMaxCacheTime),
			valid:   false,
		}
		am.icachemu.Unlock()
		return wve.Err(wve.ProofInvalid, presp.Error.Message)
	}
	fmt.Printf("yy G\n")

	expiry := time.Unix(0, presp.Result.Expiry*1e6)
	fmt.Printf("proof expires: %s\n", expiry)
	if expiry.After(time.Now().Add(ProofMaxCacheTime)) {
		expiry = time.Now().Add(ProofMaxCacheTime)
	}
	fmt.Printf("xx B\n")
	am.icachemu.Lock()
	am.icache[ick] = &icacheItem{
		expires: expiry,
		valid:   true,
	}
	am.icachemu.Unlock()
	return nil
}

//Check that the given proof is valid for subscription on the given URI
func (am *AuthModule) CheckSubscription(s *pb.PeerSubscribeParams) wve.WVE {

	//Check the signature
	hash := sha3.New256()
	hash.Write(s.Tbs.Namespace)
	hash.Write([]byte(s.Tbs.Uri))
	hash.Write([]byte(s.Tbs.Id))
	digest := hash.Sum(nil)

	resp, err := am.wave.VerifySignature(context.Background(), &eapipb.VerifySignatureParams{
		Signer: s.Tbs.SourceEntity,
		//Todo signer location
		Signature: s.Signature,
		Content:   digest,
	})
	if err != nil {
		return wve.ErrW(wve.InvalidSignature, "could not validate signature", err)
	}
	if resp.Error != nil {
		return wve.Err(wve.InvalidSignature, "failed to validate subscription signature: "+resp.Error.Message)
	}

	ick := icacheKey{}
	copy(ick.Namespace[:], s.Tbs.Namespace)
	ick.URI = s.Tbs.Uri
	ick.Permission = WAVEMQSubscribe
	h := sha3.NewShake256()
	h.Write(s.ProofDER)
	h.Read(ick.ProofHash[:])

	am.icachemu.Lock()
	entry, ok := am.icache[ick]
	am.icachemu.Unlock()
	if ok && entry.expires.After(time.Now()) {
		if entry.valid {
			return nil
		}
		return wve.Err(wve.ProofInvalid, "this proof has been cached as invalid\n")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	presp, err := am.wave.VerifyProof(ctx, &eapipb.VerifyProofParams{
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
	if presp.Error != nil {
		return wve.Err(wve.ProofInvalid, presp.Error.Message)
	}

	//If the user did not specify an absolute expiry, or specified one greater than
	//the proof allows, then set the field to the proof's expiry
	if s.AbsoluteExpiry == 0 || s.AbsoluteExpiry > presp.Result.Expiry {
		s.AbsoluteExpiry = presp.Result.Expiry
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
	//We need our entity hash
	iresp, err := am.wave.Inspect(ctx, &eapipb.InspectParams{
		Content: p.Perspective.EntitySecret.DER,
	})
	if err != nil {
		return nil, wve.ErrW(wve.NoProofFound, "failed validate perspective", err)
	}
	hash := sha3.New256()
	hash.Write(p.Namespace)
	hash.Write([]byte(p.Uri))
	for _, po := range p.Content {
		hash.Write([]byte(po.Schema))
		hash.Write(po.Content)
	}
	hash.Write([]byte(routerID))
	digest := hash.Sum(nil)

	signresp, err := am.wave.Sign(context.Background(), &eapipb.SignParams{
		Perspective: perspective,
		Content:     digest,
	})
	if err != nil {
		return nil, wve.ErrW(wve.InvalidSignature, "failed to sign", err)
	}
	if signresp.Error != nil {
		return nil, wve.Err(wve.InvalidSignature, signresp.Error.Message)
	}

	return &pb.Message{
		ProofDER:  proofresp.ProofDER,
		Signature: signresp.Signature,
		Persist:   p.Persist,
		Tbs: &pb.MessageTBS{
			SourceEntity: iresp.Entity.Hash,
			//TODO source location
			Namespace:    p.Namespace,
			Uri:          p.Uri,
			Payload:      p.Content,
			OriginRouter: routerID,
		},
	}, nil
}

func (am *AuthModule) FormSubRequest(p *pb.SubscribeParams, routerID string) (*pb.PeerSubscribeParams, wve.WVE) {
	//TODO

	//Check the signature
	hash := sha3.New256()
	hash.Write(p.Namespace)
	hash.Write([]byte(p.Uri))
	hash.Write([]byte(p.Identifier))
	hash.Write([]byte(routerID))
	digest := hash.Sum(nil)

	perspective := &eapipb.Perspective{
		EntitySecret: &eapipb.EntitySecret{
			DER:        p.Perspective.EntitySecret.DER,
			Passphrase: p.Perspective.EntitySecret.Passphrase,
		},
	}

	signresp, err := am.wave.Sign(context.Background(), &eapipb.SignParams{
		Perspective: perspective,
		Content:     digest,
	})
	if err != nil {
		return nil, wve.ErrW(wve.InvalidSignature, "failed to sign", err)
	}
	if signresp.Error != nil {
		return nil, wve.Err(wve.InvalidSignature, signresp.Error.Message)
	}

	if p.CustomProofDER != nil {
		//Build a proof
		proofresp, err := am.wave.BuildRTreeProof(context.Background(), &eapipb.BuildRTreeProofParams{
			Perspective: perspective,
			Namespace:   p.Namespace,
			Statements: []*eapipb.RTreePolicyStatement{
				{
					PermissionSet: []byte(WAVEMQPermissionSet),
					Permissions:   []string{WAVEMQSubscribe},
					Resource:      p.Uri,
				},
			},
			ResyncFirst: true,
		})
		if err != nil {
			return nil, wve.ErrW(wve.NoProofFound, "failed to build", err)
		}
		if proofresp.Error != nil {
			return nil, wve.Err(wve.NoProofFound, proofresp.Error.Message)
		}

		expiry := time.Unix(0, proofresp.Result.Expiry*1e6)
		if p.AbsoluteExpiry != 0 && expiry.After(time.Unix(0, p.AbsoluteExpiry)) {
			expiry = time.Unix(0, p.AbsoluteExpiry)
		}
		return &pb.PeerSubscribeParams{
			Tbs: &pb.PeerSubscriptionTBS{
				Namespace: p.Namespace,
				Uri:       p.Uri,
				Id:        p.Identifier,
				RouterID:  routerID,
			},
			Signature:      signresp.Signature,
			ProofDER:       proofresp.ProofDER,
			AbsoluteExpiry: expiry.UnixNano(),
		}, nil
	} else {
		return &pb.PeerSubscribeParams{
			Tbs: &pb.PeerSubscriptionTBS{
				Namespace: p.Namespace,
				Uri:       p.Uri,
				Id:        p.Identifier,
				RouterID:  routerID,
			},
			Signature:      signresp.Signature,
			ProofDER:       p.CustomProofDER,
			AbsoluteExpiry: p.AbsoluteExpiry,
		}, nil
	}

}
