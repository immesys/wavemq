package core

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"io/ioutil"
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
const WAVEMQRoute = "route"

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

	ourPerspective  *eapipb.Perspective
	perspectiveHash []byte

	routingProofs map[string][]byte
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
		cfg:           cfg,
		wave:          eapi,
		icache:        make(map[icacheKey]*icacheItem),
		bcache:        make(map[bcacheKey]*bcacheItem),
		routingProofs: make(map[string][]byte),
	}, nil
}

func (am *AuthModule) AddDesignatedRoutingNamespace(filename string) (ns string, err error) {
	contents, err := ioutil.ReadFile(filename)
	if err != nil {
		return "", fmt.Errorf("could not read designated routing file: %v", err)
	}

	der := contents
	pblock, _ := pem.Decode(contents)
	if pblock != nil {
		der = pblock.Bytes
	}

	resp, err := am.wave.VerifyProof(context.Background(), &eapipb.VerifyProofParams{
		ProofDER: der,
	})
	if err != nil {
		return "", fmt.Errorf("could not verify dr file: %v", err)
	}
	if resp.Error != nil {
		return "", fmt.Errorf("could not verify dr file: %v", resp.Error.Message)
	}

	ns = base64.URLEncoding.EncodeToString(resp.Result.Policy.RTreePolicy.Namespace)
	//Check proof actually grants the right permissions:
	found := false
outer:
	for _, s := range resp.Result.Policy.RTreePolicy.Statements {
		if bytes.Equal(s.GetPermissionSet(), []byte(WAVEMQPermissionSet)) {
			for _, perm := range s.Permissions {
				if perm == WAVEMQRoute {
					found = true
					break outer
				}
			}
		}
	}

	if !found {
		return "", fmt.Errorf("designated routing proof does not actually prove wavemq:route on any namespace")
	}

	am.routingProofs[ns] = der
	return ns, nil
}

func (am *AuthModule) SetRouterEntityFile(filename string) error {
	contents, err := ioutil.ReadFile(filename)
	if err != nil {
		if os.IsNotExist(err) {
			//Generate a new entity
			resp, err := am.wave.CreateEntity(context.Background(), &eapipb.CreateEntityParams{
				ValidUntil: time.Now().Add(30*365*24*time.Hour).UnixNano() / 1e6,
			})
			if err != nil {
				return err
			}
			if resp.Error != nil {
				return errors.New(resp.Error.Message)
			}

			presp, err := am.wave.PublishEntity(context.Background(), &eapipb.PublishEntityParams{
				DER: resp.PublicDER,
			})
			if err != nil {
				return err
			}
			if presp.Error != nil {
				return errors.New(presp.Error.Message)
			}

			bl := pem.Block{
				Type:  eapi.PEM_ENTITY_SECRET,
				Bytes: resp.SecretDER,
			}
			contents = pem.EncodeToMemory(&bl)
			err = ioutil.WriteFile(filename, contents, 0600)
			if err != nil {
				return fmt.Errorf("could not write entity file: %v\n", err)
			}
		} else {
			return fmt.Errorf("could not open router entity file: %v\n", err)
		}
	}

	am.ourPerspective = &eapipb.Perspective{
		EntitySecret: &eapipb.EntitySecret{
			DER: contents,
		},
	}

	//Check perspective is okay by doing a resync
	resp, err := am.wave.ResyncPerspectiveGraph(context.Background(), &eapipb.ResyncPerspectiveGraphParams{
		Perspective: am.ourPerspective,
	})
	if err != nil {
		return fmt.Errorf("could not sync router entity file: %v", err)
	}
	if resp.Error != nil {
		return fmt.Errorf("could not sync router entity file: %v", resp.Error.Message)
	}

	//Wait for sync, for the fun of it
	err = am.wave.WaitForSyncCompleteHack(&eapipb.SyncParams{
		Perspective: am.ourPerspective,
	})
	if err != nil {
		return fmt.Errorf("could not sync router entity file: %v", err)
	}

	//also inspect so we can learn our hash
	iresp, err := am.wave.Inspect(context.Background(), &eapipb.InspectParams{
		Content: contents,
	})
	if err != nil {
		return fmt.Errorf("could not inspect router entity file: %v", err)
	}
	if resp.Error != nil {
		return fmt.Errorf("could not inspect router entity file: %v", resp.Error.Message)
	}
	am.perspectiveHash = iresp.Entity.Hash

	return nil
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

	expiry := time.Unix(0, presp.Result.Expiry*1e6)
	if expiry.After(time.Now().Add(ProofMaxCacheTime)) {
		expiry = time.Now().Add(ProofMaxCacheTime)
	}
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
	hash.Write([]byte(s.Tbs.RouterID))
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

	if p.Perspective == nil || p.Perspective.EntitySecret == nil {
		return nil, wve.Err(wve.InvalidParameter, "missing perspective")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	//TODO cache

	bk := bcacheKey{}
	copy(bk.Namespace[:], p.Namespace)

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

	if p.Perspective == nil || p.Perspective.EntitySecret == nil {
		return nil, wve.Err(wve.InvalidParameter, "missing perspective")
	}

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

	iresp, err := am.wave.Inspect(context.Background(), &eapipb.InspectParams{
		Content: p.Perspective.EntitySecret.DER,
	})
	if err != nil {
		return nil, wve.ErrW(wve.NoProofFound, "failed validate perspective", err)
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

	if p.CustomProofDER == nil {
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
				Expiry:       p.Expiry,
				SourceEntity: iresp.Entity.Hash,
				Namespace:    p.Namespace,
				Uri:          p.Uri,
				Id:           p.Identifier,
				RouterID:     routerID,
			},
			Signature:      signresp.Signature,
			ProofDER:       proofresp.ProofDER,
			AbsoluteExpiry: expiry.UnixNano(),
		}, nil
	} else {
		return &pb.PeerSubscribeParams{
			Tbs: &pb.PeerSubscriptionTBS{
				Expiry:       p.Expiry,
				SourceEntity: iresp.Entity.Hash,
				Namespace:    p.Namespace,
				Uri:          p.Uri,
				Id:           p.Identifier,
				RouterID:     routerID,
			},
			Signature:      signresp.Signature,
			ProofDER:       p.CustomProofDER,
			AbsoluteExpiry: p.AbsoluteExpiry,
		}, nil
	}

}
