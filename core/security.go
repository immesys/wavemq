package core

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/creachadair/cityhash"
	"github.com/gogo/protobuf/proto"
	"github.com/huichen/murmur"
	"github.com/immesys/wave/eapi"
	eapipb "github.com/immesys/wave/eapi/pb"
	"github.com/immesys/wave/iapi"
	"github.com/immesys/wave/jedistore"
	"github.com/immesys/wave/jediutils"
	"github.com/immesys/wave/localdb/lls"
	"github.com/immesys/wave/localdb/poc"
	"github.com/immesys/wave/storage/overlay"
	"github.com/immesys/wave/waved"
	"github.com/immesys/wave/wve"
	pb "github.com/immesys/wavemq/mqpb"
	"github.com/ucbrise/jedi-protocol-go"
	"golang.org/x/crypto/sha3"
)

const WAVEMQPermissionSet = "\x1b\x20\x14\x33\x74\xb3\x2f\xd2\x74\x39\x54\xfe\x47\x86\xf6\xcf\x86\xd4\x03\x72\x0f\x5e\xc4\x42\x36\xb6\x58\xc2\x6a\x1e\x68\x0f\x6e\x01"
const WAVEMQPublish = "publish"
const WAVEMQSubscribe = "subscribe"
const WAVEMQQuery = "query"
const WAVEMQRoute = "route"

const ValidatedProofMaxCacheTime = 6 * time.Hour
const SuccessfulProofCacheTime = 6 * time.Hour
const FailedProofCacheTime = 5 * time.Minute

const docaching = true

type AuthModule struct {
	cfg  *waved.Configuration
	wave *eapi.EAPI

	// the Incoming cache stores the time that a given proof must be
	// revalidated
	icachemu sync.RWMutex
	icache   map[icacheKey]*icacheItem

	// the Build cache stores the results of proof build operations
	bcachemu sync.RWMutex
	bcache   map[bcacheKey]*bcacheItem

	ourPerspective  *eapipb.Perspective
	perspectiveHash []byte

	routingProofs map[string][]byte

	//Hash of perspective DER -> public entity hash
	phashcachemu sync.RWMutex
	phashcache   map[uint32][]byte

	// State for JEDI
	jediClientsLock sync.RWMutex
	jediClients     map[string]*jedi.ClientState
	jediPublicInfo  *jedistore.WAVEPublicInfo
}

type icacheKey struct {
	Namespace  [32]byte
	Entity     [32]byte
	URI        string
	Permission string
	ProofLow   uint64
	ProofHigh  uint64
	//ProofHash  [32]byte
}
type icacheItem struct {
	CacheExpiry time.Time
	ProofExpiry time.Time
	Valid       bool
	DER         []byte
}

type bcacheKey struct {
	Namespace  [32]byte
	Target     [32]byte
	PolicyHash [32]byte
}
type bcacheItem struct {
	CacheExpiry time.Time
	Valid       bool
	DER         []byte
	ProofExpiry time.Time
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
		cfg:            cfg,
		wave:           eapi,
		icache:         make(map[icacheKey]*icacheItem),
		bcache:         make(map[bcacheKey]*bcacheItem),
		routingProofs:  make(map[string][]byte),
		phashcache:     make(map[uint32][]byte),
		jediClients:    make(map[string]*jedi.ClientState),
		jediPublicInfo: jedistore.NewWAVEPublicInfo(eapi.GetEngineNoPerspective()),
	}, nil
}

func (am *AuthModule) GetJEDIClient(ctx context.Context, perspective *eapipb.Perspective) (*jedi.ClientState, error) {
	var err error

	var serialized []byte
	if serialized, err = proto.Marshal(perspective); err != nil {
		return nil, err
	}

	mapkey := string(serialized)

	am.jediClientsLock.RLock()
	client, ok := am.jediClients[mapkey]
	am.jediClientsLock.RUnlock()

	if !ok {
		am.jediClientsLock.Lock()
		defer am.jediClientsLock.Unlock()

		client, ok = am.jediClients[mapkey]
		if !ok {
			eng, werr := am.wave.GetEngine(ctx, perspective)
			if werr != nil {
				return nil, werr
			}
			store := jedistore.NewWAVEKeyStore(eng)
			client = jedi.NewClientState(am.jediPublicInfo, store, jediutils.WAVEPatternEncoderSingleton, 32<<20)
			am.jediClients[mapkey] = client
		}
	}

	return client, nil
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
	defer cancel()
	if m.Tbs == nil {
		return wve.Err(wve.InvalidParameter, "message missing TBS")
	}
	//Check the signature
	hash := sha3.New256()
	hash.Write(m.Tbs.SourceEntity)
	hash.Write(m.Tbs.Namespace)
	hash.Write([]byte(m.Tbs.Uri))
	for _, po := range m.Tbs.Payload {
		hash.Write([]byte(po.Schema))
		hash.Write(po.Content)
	}
	hash.Write([]byte(m.Tbs.OriginRouter))
	if m.Tbs.JediData != nil {
		buffer := make([]byte, 8)
		binary.LittleEndian.PutUint64(buffer, uint64(m.Tbs.JediData.Timestamp))
		hash.Write(buffer)
	}
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

	elidedProof := len(m.ProofDER) == 0 && len(m.ProofHash) == 16

	if elidedProof {
		ick.ProofHigh = binary.BigEndian.Uint64(m.ProofHash[0:8])
		ick.ProofLow = binary.BigEndian.Uint64(m.ProofHash[8:16])
	} else {
		ick.ProofLow, ick.ProofHigh = cityhash.Hash128(m.ProofDER)
	}

	am.icachemu.Lock()
	entry, ok := am.icache[ick]
	am.icachemu.Unlock()
	if ok && entry.CacheExpiry.After(time.Now()) {
		if entry.Valid {
			//fmt.Printf("returning message valid from cache\n")
			if elidedProof {
				m.ProofDER = entry.DER
			}
			return nil
		}
		//fmt.Printf("returning message invalid from cache\n")
		return wve.Err(wve.ProofInvalid, "this proof has been cached as invalid")
	} else {
		if elidedProof {
			return wve.Err(wve.ProofNotCached, "send the full proof please")
		}
	}

	presp, err := am.wave.VerifyProof(ctx, &eapipb.VerifyProofParams{
		ProofDER: m.ProofDER,
		Subject:  m.Tbs.SourceEntity,
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
	if err != nil {
		return wve.ErrW(wve.InternalError, "could not validate proof", err)
	}
	if presp.Error != nil {
		if docaching {
			am.icachemu.Lock()
			am.icache[ick] = &icacheItem{
				CacheExpiry: time.Now().Add(ValidatedProofMaxCacheTime),
				Valid:       false,
				DER:         m.ProofDER,
			}
			am.icachemu.Unlock()
		}
		return wve.Err(wve.ProofInvalid, presp.Error.Message)
	}

	expiry := time.Unix(0, presp.Result.Expiry*1e6)
	if expiry.After(time.Now().Add(ValidatedProofMaxCacheTime)) {
		expiry = time.Now().Add(ValidatedProofMaxCacheTime)
	}
	am.icachemu.Lock()
	if docaching {
		am.icache[ick] = &icacheItem{
			CacheExpiry: expiry,
			Valid:       true,
			DER:         m.ProofDER,
		}
	}
	am.icachemu.Unlock()
	return nil
}

//Check that the given proof is valid for subscription on the given URI
func (am *AuthModule) CheckSubscription(s *pb.PeerSubscribeParams) wve.WVE {

	//Check the signature
	hash := sha3.New256()
	hash.Write(s.Tbs.SourceEntity)
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
	ick.ProofLow, ick.ProofHigh = cityhash.Hash128(s.ProofDER)
	//
	// h := sha3.NewShake256()
	// h.Write(s.ProofDER)
	// h.Read(ick.ProofHash[:])

	am.icachemu.Lock()
	entry, ok := am.icache[ick]
	am.icachemu.Unlock()
	if ok && entry.CacheExpiry.After(time.Now()) {
		if entry.Valid {
			if time.Unix(0, s.AbsoluteExpiry).After(entry.ProofExpiry) {
				s.AbsoluteExpiry = entry.ProofExpiry.UnixNano()
			}
			return nil
		}
		return wve.Err(wve.ProofInvalid, "this proof has been cached as invalid\n")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	presp, err := am.wave.VerifyProof(ctx, &eapipb.VerifyProofParams{
		ProofDER: s.ProofDER,
		Subject:  s.Tbs.SourceEntity,
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
		if docaching {
			entry := &icacheItem{
				CacheExpiry: time.Now().Add(ValidatedProofMaxCacheTime),
				Valid:       false,
			}
			am.icachemu.Lock()
			am.icache[ick] = entry
			am.icachemu.Unlock()
		}
		return wve.Err(wve.ProofInvalid, presp.Error.Message)
	}

	fmt.Printf("proof expiry is %s\n", time.Unix(0, presp.Result.Expiry*1e6))

	entry = &icacheItem{
		CacheExpiry: time.Now().Add(ValidatedProofMaxCacheTime),
		Valid:       true,
		ProofExpiry: time.Unix(0, presp.Result.Expiry*1e6),
		DER:         s.ProofDER,
	}
	if docaching {
		am.icachemu.Lock()
		am.icache[ick] = entry
		am.icachemu.Unlock()
	}
	//If the user did not specify an absolute expiry, or specified one greater than
	//the proof allows, then set the field to the proof's expiry
	if s.AbsoluteExpiry == 0 || s.AbsoluteExpiry > presp.Result.Expiry {
		s.AbsoluteExpiry = entry.ProofExpiry.UnixNano()
	}
	return nil
}

//Check that the given proof is valid for query on the given URI
func (am *AuthModule) CheckQuery(s *pb.PeerQueryParams) wve.WVE {

	//Check the signature
	hash := sha3.New256()
	hash.Write(s.Namespace)
	hash.Write([]byte(s.Uri))
	digest := hash.Sum(nil)

	resp, err := am.wave.VerifySignature(context.Background(), &eapipb.VerifySignatureParams{
		Signer: s.SourceEntity,
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
	copy(ick.Namespace[:], s.Namespace)
	ick.URI = s.Uri
	ick.Permission = WAVEMQQuery
	ick.ProofLow, ick.ProofHigh = cityhash.Hash128(s.ProofDER)
	// h := sha3.NewShake256()
	// h.Write(s.ProofDER)
	// h.Read(ick.ProofHash[:])

	am.icachemu.RLock()
	entry, ok := am.icache[ick]
	am.icachemu.RUnlock()
	if ok && entry.CacheExpiry.After(time.Now()) {
		if entry.Valid {
			return nil
		}
		return wve.Err(wve.ProofInvalid, "this proof has been cached as invalid\n")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	presp, err := am.wave.VerifyProof(ctx, &eapipb.VerifyProofParams{
		ProofDER: s.ProofDER,
		Subject:  s.SourceEntity,
		RequiredRTreePolicy: &eapipb.RTreePolicy{
			Namespace: s.Namespace,
			Statements: []*eapipb.RTreePolicyStatement{
				{
					PermissionSet: []byte(WAVEMQPermissionSet),
					Permissions:   []string{WAVEMQQuery},
					Resource:      s.Uri,
				},
			},
		},
	})
	cancel()
	if err != nil {
		return wve.ErrW(wve.InternalError, "could not validate proof", err)
	}
	if presp.Error != nil {
		entry := &icacheItem{
			CacheExpiry: time.Now().Add(ValidatedProofMaxCacheTime),
			Valid:       false,
		}
		if docaching {
			am.icachemu.Lock()
			am.icache[ick] = entry
			am.icachemu.Unlock()
		}
		return wve.Err(wve.ProofInvalid, presp.Error.Message)
	}

	entry = &icacheItem{
		CacheExpiry: time.Now().Add(ValidatedProofMaxCacheTime),
		Valid:       true,
		ProofExpiry: time.Unix(0, presp.Result.Expiry*1e6),
		DER:         s.ProofDER,
	}
	if docaching {
		am.icachemu.Lock()
		am.icache[ick] = entry
		am.icachemu.Unlock()
	}
	return nil
}

//TODO check all params as well formed

func (am *AuthModule) PrepareMessage(persp *pb.Perspective, m *pb.Message) (*pb.Message, wve.WVE) {
	perspective := &eapipb.Perspective{
		EntitySecret: &eapipb.EntitySecret{
			DER:        persp.EntitySecret.DER,
			Passphrase: persp.EntitySecret.Passphrase,
		},
	}
	decryptedPayload := m.Tbs.Payload
	if m.EncryptionPartition != nil {
		decryptedPayload = []*pb.PayloadObject{}
		for _, po := range m.Tbs.Payload {
			decresp, err := am.wave.DecryptMessage(context.Background(), &eapipb.DecryptMessageParams{
				Perspective: perspective,
				Ciphertext:  po.Content,
			})
			if err != nil {
				return nil, wve.ErrW(wve.MessageDecryptionError, "failed to decrypt", err)
			}
			if decresp.Error != nil && decresp.Error.Code == 913 {
				//This could be because we did not resync, try again with resync
				decresp, err = am.wave.DecryptMessage(context.Background(), &eapipb.DecryptMessageParams{
					Perspective: perspective,
					Ciphertext:  po.Content,
					ResyncFirst: true,
				})
				if err != nil {
					return nil, wve.ErrW(wve.MessageDecryptionError, "failed to decrypt", err)
				}
				if decresp.Error != nil {
					return nil, wve.Err(wve.MessageDecryptionError, decresp.Error.Message)
				}
			} else if decresp.Error != nil {
				return nil, wve.Err(wve.MessageDecryptionError, decresp.Error.Message)
			}
			decryptedPayload = append(decryptedPayload, &pb.PayloadObject{
				Schema:  po.Schema,
				Content: decresp.Content,
			})
		}
	}
	if m.Tbs.JediData != nil {
		if len(m.Tbs.Payload) != 2 || m.Tbs.Payload[0].Schema != "jedi:encryptedKey" || m.Tbs.Payload[1].Schema != "jedi:encryptedMessage" {
			return nil, wve.Err(wve.MessageDecryptionError, "malformed message for JEDI decryption")
		}
		client, err := am.GetJEDIClient(context.TODO(), perspective)
		if err != nil {
			return nil, wve.ErrW(wve.MessageDecryptionError, "could not obtain JEDI state", err)
		}
		timestamp := time.Unix(m.Tbs.JediData.Timestamp, 0)
		encryptedKey := m.Tbs.Payload[0].Content
		encryptedMessage := m.Tbs.Payload[1].Content
		decrypted, err := client.DecryptSeparated(context.TODO(), m.Tbs.Namespace, m.Tbs.Uri, timestamp, encryptedKey, encryptedMessage)
		if err != nil {
			return nil, wve.ErrW(wve.MessageDecryptionError, "could not decrypt JEDI message", err)
		}
		payload := new(pb.Payload)
		if err = proto.Unmarshal(decrypted, payload); err != nil {
			return nil, wve.ErrW(wve.MessageDecryptionError, "could not unmarshal payload objects after JEDI decryption", err)
		}
		decryptedPayload = payload.Objects
	}
	return &pb.Message{
		Signature:           m.Signature,
		Persist:             m.Persist,
		EncryptionPartition: m.EncryptionPartition,
		ProofDER:            m.ProofDER,
		Tbs: &pb.MessageTBS{
			SourceEntity: m.Tbs.SourceEntity,
			//TODO source location
			Namespace:    m.Tbs.Namespace,
			Uri:          m.Tbs.Uri,
			Payload:      decryptedPayload,
			OriginRouter: m.Tbs.OriginRouter,
			JediData:     m.Tbs.JediData,
		},
	}, nil
}

func (am *AuthModule) FormMessage(p *pb.PublishParams, routerID string) (*pb.Message, wve.WVE) {

	if p.Perspective == nil || p.Perspective.EntitySecret == nil {
		return nil, wve.Err(wve.InvalidParameter, "missing perspective")
	}

	perspectiveHash := murmur.Murmur3(p.Perspective.EntitySecret.DER)
	am.phashcachemu.RLock()
	realhash, ok := am.phashcache[perspectiveHash]
	am.phashcachemu.RUnlock()
	if !ok {
		//We need our entity hash
		iresp, err := am.wave.Inspect(context.Background(), &eapipb.InspectParams{
			Content: p.Perspective.EntitySecret.DER,
		})
		if err != nil {
			return nil, wve.ErrW(wve.NoProofFound, "failed validate perspective", err)
		}
		if iresp.Error != nil {
			return nil, wve.Err(wve.NoProofFound, "failed validate perspective: "+iresp.Error.Message)
		}
		am.phashcachemu.Lock()
		am.phashcache[perspectiveHash] = iresp.Entity.Hash
		am.phashcachemu.Unlock()
		realhash = iresp.Entity.Hash
	}

	perspective := &eapipb.Perspective{
		EntitySecret: &eapipb.EntitySecret{
			DER:        p.Perspective.EntitySecret.DER,
			Passphrase: p.Perspective.EntitySecret.Passphrase,
		},
	}

	bk := bcacheKey{}
	copy(bk.Namespace[:], p.Namespace)
	copy(bk.Target[:], realhash)

	policyhash := sha3.New256()
	policyhash.Write([]byte(WAVEMQPublish))
	policyhash.Write([]byte("onuri="))
	policyhash.Write([]byte(p.Uri))
	poldigest := policyhash.Sum(nil)
	copy(bk.PolicyHash[:], poldigest)

	am.bcachemu.RLock()
	cachedproof, ok := am.bcache[bk]
	am.bcachemu.RUnlock()

	var proofder []byte

	if p.CustomProofDER != nil {
		proofder = p.CustomProofDER
	} else {
		rebuildproof := true
		if ok {
			if cachedproof.CacheExpiry.After(time.Now()) {
				rebuildproof = false
			}
		}

		// if rebuildproof {
		// 	fmt.Printf("[PC] form message proof cache MISS: %v\n", p.Uri)
		// } else {
		// 	fmt.Printf("[PC] form message proof cache HIT\n")
		// }

		if rebuildproof {
			proofresp, err := am.wave.BuildRTreeProof(context.Background(), &eapipb.BuildRTreeProofParams{
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
			if err != nil {
				return nil, wve.ErrW(wve.NoProofFound, "failed to build", err)
			}
			if proofresp.Error != nil {
				ci := &bcacheItem{
					CacheExpiry: time.Now().Add(FailedProofCacheTime),
					Valid:       false,
				}
				if docaching {
					am.bcachemu.Lock()
					am.bcache[bk] = ci
					am.bcachemu.Unlock()
				}
				return nil, wve.Err(wve.NoProofFound, proofresp.Error.Message)
			}

			proofder = proofresp.ProofDER
			ci := &bcacheItem{
				CacheExpiry: time.Now().Add(SuccessfulProofCacheTime),
				Valid:       true,
				DER:         proofresp.ProofDER,
				ProofExpiry: time.Unix(0, proofresp.Result.Expiry*1e6),
			}
			if ci.ProofExpiry.Before(ci.CacheExpiry) {
				ci.CacheExpiry = ci.ProofExpiry
			}
			if docaching {
				am.bcachemu.Lock()
				am.bcache[bk] = ci
				am.bcachemu.Unlock()
			}
		} else {
			proofder = cachedproof.DER
		}
	}

	encryptedPayload := p.Content
	if p.EncryptionPartition != nil {
		chunks := []string{}
		for _, chunk := range p.EncryptionPartition {
			chunks = append(chunks, string(chunk))
		}
		partition := strings.Join(chunks[:], "/")
		encryptedPayload = []*pb.PayloadObject{}
		for _, po := range p.Content {
			encresp, err := am.wave.EncryptMessage(context.Background(), &eapipb.EncryptMessageParams{
				Namespace: p.Namespace,
				Resource:  partition,
				Content:   po.Content,
			})
			if err != nil {
				return nil, wve.ErrW(wve.MessageEncryptionError, "failed to encrypt", err)
			}
			if encresp.Error != nil {
				return nil, wve.Err(wve.MessageEncryptionError, encresp.Error.Message)
			}
			encryptedPayload = append(encryptedPayload, &pb.PayloadObject{
				Schema:  po.Schema,
				Content: encresp.Ciphertext,
			})
		}
	}
	var jediData *pb.JEDIData
	if p.EncryptWithJEDI {
		jediClient, err := am.GetJEDIClient(context.TODO(), perspective)
		if err != nil {
			return nil, wve.ErrW(wve.MessageEncryptionError, "could not obtain JEDI state", err)
		}
		payload := &pb.Payload{Objects: p.Content}
		message, err := proto.Marshal(payload)
		if err != nil {
			return nil, wve.ErrW(wve.MessageEncryptionError, "could not marshal payload objects for JEDI encryption", err)
		}
		timestamp := time.Now()
		jediData = &pb.JEDIData{Timestamp: timestamp.Unix()}
		encrypted, err := jediClient.Encrypt(context.TODO(), p.Namespace, p.Uri, timestamp, message)
		if err != nil {
			return nil, wve.ErrW(wve.MessageEncryptionError, "could not encrypt message with JEDI", err)
		}
		keyPO := &pb.PayloadObject{
			Schema:  "jedi:encryptedKey",
			Content: encrypted[:jedi.EncryptedKeySize],
		}
		messagePO := &pb.PayloadObject{
			Schema:  "jedi:encryptedMessage",
			Content: encrypted[jedi.EncryptedKeySize:],
		}
		encryptedPayload = []*pb.PayloadObject{keyPO, messagePO}
	}

	hash := sha3.New256()
	hash.Write(realhash)
	hash.Write(p.Namespace)
	hash.Write([]byte(p.Uri))
	for _, po := range encryptedPayload {
		hash.Write([]byte(po.Schema))
		hash.Write(po.Content)
	}
	hash.Write([]byte(routerID))
	if jediData != nil {
		buffer := make([]byte, 8)
		binary.LittleEndian.PutUint64(buffer, uint64(jediData.Timestamp))
		hash.Write(buffer)
	}
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
		ProofDER:            proofder,
		Signature:           signresp.Signature,
		Persist:             p.Persist,
		EncryptionPartition: p.EncryptionPartition,
		Tbs: &pb.MessageTBS{
			SourceEntity: realhash,
			//TODO source location
			Namespace:    p.Namespace,
			Uri:          p.Uri,
			Payload:      encryptedPayload,
			OriginRouter: routerID,
			JediData:     jediData,
		},
	}, nil
}

func (am *AuthModule) FormSubRequest(p *pb.SubscribeParams, routerID string) (*pb.PeerSubscribeParams, wve.WVE) {

	if p.Perspective == nil || p.Perspective.EntitySecret == nil {
		return nil, wve.Err(wve.InvalidParameter, "missing perspective")
	}

	perspectiveHash := murmur.Murmur3(p.Perspective.EntitySecret.DER)
	am.phashcachemu.RLock()
	realhash, ok := am.phashcache[perspectiveHash]
	am.phashcachemu.RUnlock()
	if !ok {
		//We need our entity hash
		iresp, err := am.wave.Inspect(context.Background(), &eapipb.InspectParams{
			Content: p.Perspective.EntitySecret.DER,
		})
		if err != nil {
			return nil, wve.ErrW(wve.NoProofFound, "failed validate perspective", err)
		}
		if iresp.Error != nil {
			return nil, wve.Err(wve.NoProofFound, "failed validate perspective: "+iresp.Error.Message)
		}
		am.phashcachemu.Lock()
		am.phashcache[perspectiveHash] = iresp.Entity.Hash
		am.phashcachemu.Unlock()
		realhash = iresp.Entity.Hash
	}

	hash := sha3.New256()
	hash.Write(realhash)
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

	bk := bcacheKey{}
	copy(bk.Namespace[:], p.Namespace)
	copy(bk.Target[:], realhash)

	policyhash := sha3.New256()
	policyhash.Write([]byte(WAVEMQSubscribe))
	policyhash.Write([]byte("onuri="))
	policyhash.Write([]byte(p.Uri))
	poldigest := policyhash.Sum(nil)
	copy(bk.PolicyHash[:], poldigest)

	am.bcachemu.RLock()
	cachedproof, ok := am.bcache[bk]
	am.bcachemu.RUnlock()

	var proofder []byte
	var expiry time.Time

	if p.CustomProofDER != nil {
		proofder = p.CustomProofDER
	} else {
		rebuildproof := true
		if ok {
			if cachedproof.CacheExpiry.After(time.Now()) {
				rebuildproof = false
			}
		}

		if rebuildproof {
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
				ci := &bcacheItem{
					CacheExpiry: time.Now().Add(FailedProofCacheTime),
					Valid:       false,
				}
				if docaching {
					am.bcachemu.Lock()
					am.bcache[bk] = ci
					am.bcachemu.Unlock()
				}
				return nil, wve.Err(wve.NoProofFound, proofresp.Error.Message)
			}

			proofder = proofresp.ProofDER
			ci := &bcacheItem{
				CacheExpiry: time.Now().Add(SuccessfulProofCacheTime),
				Valid:       true,
				DER:         proofresp.ProofDER,
				ProofExpiry: time.Unix(0, proofresp.Result.Expiry*1e6),
			}
			if ci.ProofExpiry.Before(ci.CacheExpiry) {
				ci.CacheExpiry = ci.ProofExpiry
			}
			if docaching {
				am.bcachemu.Lock()
				am.bcache[bk] = ci
				am.bcachemu.Unlock()
			}

			expiry = time.Unix(0, proofresp.Result.Expiry*1e6)
			if p.AbsoluteExpiry != 0 && expiry.After(time.Unix(0, p.AbsoluteExpiry)) {
				expiry = time.Unix(0, p.AbsoluteExpiry)
			}
		} else {
			if !cachedproof.Valid {
				return nil, wve.Err(wve.NoProofFound, "we've cached that there is no proof for this")
			}
			proofder = cachedproof.DER
			expiry = cachedproof.ProofExpiry
			if p.AbsoluteExpiry != 0 && expiry.After(time.Unix(0, p.AbsoluteExpiry)) {
				expiry = time.Unix(0, p.AbsoluteExpiry)
			}
		}
	}

	return &pb.PeerSubscribeParams{
		Tbs: &pb.PeerSubscriptionTBS{
			Expiry:       p.Expiry,
			SourceEntity: realhash,
			Namespace:    p.Namespace,
			Uri:          p.Uri,
			Id:           p.Identifier,
			RouterID:     routerID,
		},
		Signature:      signresp.Signature,
		ProofDER:       proofder,
		AbsoluteExpiry: expiry.UnixNano(),
	}, nil

}

func (am *AuthModule) FormQueryRequest(p *pb.QueryParams, routerID string) (*pb.PeerQueryParams, wve.WVE) {

	if p.Perspective == nil || p.Perspective.EntitySecret == nil {
		return nil, wve.Err(wve.InvalidParameter, "missing perspective")
	}

	perspectiveHash := murmur.Murmur3(p.Perspective.EntitySecret.DER)
	am.phashcachemu.RLock()
	realhash, ok := am.phashcache[perspectiveHash]
	am.phashcachemu.RUnlock()
	if !ok {
		//We need our entity hash
		iresp, err := am.wave.Inspect(context.Background(), &eapipb.InspectParams{
			Content: p.Perspective.EntitySecret.DER,
		})
		if err != nil {
			return nil, wve.ErrW(wve.NoProofFound, "failed validate perspective", err)
		}
		if iresp.Error != nil {
			return nil, wve.Err(wve.NoProofFound, "failed validate perspective: "+iresp.Error.Message)
		}
		am.phashcachemu.Lock()
		am.phashcache[perspectiveHash] = iresp.Entity.Hash
		am.phashcachemu.Unlock()
		realhash = iresp.Entity.Hash
	}

	perspective := &eapipb.Perspective{
		EntitySecret: &eapipb.EntitySecret{
			DER:        p.Perspective.EntitySecret.DER,
			Passphrase: p.Perspective.EntitySecret.Passphrase,
		},
	}

	var proofder []byte
	if p.CustomProofDER == nil {
		bk := bcacheKey{}
		copy(bk.Namespace[:], p.Namespace)
		copy(bk.Target[:], realhash)

		policyhash := sha3.New256()
		policyhash.Write([]byte(WAVEMQQuery))
		policyhash.Write([]byte("onuri="))
		policyhash.Write([]byte(p.Uri))
		poldigest := policyhash.Sum(nil)
		copy(bk.PolicyHash[:], poldigest)

		am.bcachemu.RLock()
		cachedproof, ok := am.bcache[bk]
		am.bcachemu.RUnlock()

		rebuildproof := true
		if ok {
			if cachedproof.CacheExpiry.After(time.Now()) {
				rebuildproof = false
			}
		}

		// if rebuildproof {
		// 	fmt.Printf("[PC] query proof cache MISS\n")
		// } else {
		// 	fmt.Printf("[PC] query proof cache HIT\n")
		// }

		if rebuildproof {

			//Build a proof
			proofresp, err := am.wave.BuildRTreeProof(context.Background(), &eapipb.BuildRTreeProofParams{
				Perspective: perspective,
				Namespace:   p.Namespace,
				Statements: []*eapipb.RTreePolicyStatement{
					{
						PermissionSet: []byte(WAVEMQPermissionSet),
						Permissions:   []string{WAVEMQQuery},
						Resource:      p.Uri,
					},
				},
				ResyncFirst: true,
			})
			if err != nil {
				return nil, wve.ErrW(wve.NoProofFound, "failed to build", err)
			}
			if proofresp.Error != nil {
				ci := &bcacheItem{
					CacheExpiry: time.Now().Add(FailedProofCacheTime),
					Valid:       false,
				}
				if docaching {
					am.bcachemu.Lock()
					am.bcache[bk] = ci
					am.bcachemu.Unlock()
				}
				return nil, wve.Err(wve.NoProofFound, proofresp.Error.Message)
			}

			ci := &bcacheItem{
				CacheExpiry: time.Now().Add(SuccessfulProofCacheTime),
				Valid:       true,
				DER:         proofresp.ProofDER,
				ProofExpiry: time.Unix(0, proofresp.Result.Expiry*1e6),
			}
			if ci.ProofExpiry.Before(ci.CacheExpiry) {
				ci.CacheExpiry = ci.ProofExpiry
			}
			if docaching {
				am.bcachemu.Lock()
				am.bcache[bk] = ci
				am.bcachemu.Unlock()
			}
			proofder = proofresp.ProofDER

		} else {
			if cachedproof.Valid {
				proofder = cachedproof.DER
			} else {
				return nil, wve.Err(wve.NoProofFound, "we've cached that there is no proof for this")
			}
		}

	} else {
		proofder = p.CustomProofDER
	}

	hash := sha3.New256()
	hash.Write(p.Namespace)
	hash.Write([]byte(p.Uri))
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

	return &pb.PeerQueryParams{
		SourceEntity: realhash,
		Namespace:    p.Namespace,
		Uri:          p.Uri,
		Signature:    signresp.Signature,
		ProofDER:     proofder,
	}, nil

}

func (am *AuthModule) VerifyServerHandshake(nsString string, entityHash []byte, signature []byte, proof []byte, cert []byte) error {
	//First verify the signature
	resp, err := am.wave.VerifySignature(context.Background(), &eapipb.VerifySignatureParams{
		Signer:    entityHash,
		Signature: signature,
		Content:   cert,
	})
	if err != nil {
		return err
	}
	if resp.Error != nil {
		return errors.New(resp.Error.Message)
	}

	ns, err := base64.URLEncoding.DecodeString(nsString)
	if err != nil {
		return err
	}

	//Signature ok, verify proof
	presp, err := am.wave.VerifyProof(context.Background(), &eapipb.VerifyProofParams{
		ProofDER: proof,
		Subject:  entityHash,
		RequiredRTreePolicy: &eapipb.RTreePolicy{
			Namespace: ns,
			Statements: []*eapipb.RTreePolicyStatement{
				{
					PermissionSet: []byte(WAVEMQPermissionSet),
					Permissions:   []string{WAVEMQRoute},
					Resource:      "*",
				},
			},
		},
	})

	if err != nil {
		return err
	}
	if presp.Error != nil {
		return errors.New(presp.Error.Message)
	}
	if !bytes.Equal(presp.Result.Subject, entityHash) {
		return errors.New("proof valid but for a different entity")
	}
	return nil
}

//A 34 byte multihash
func (am *AuthModule) GeneratePeerHeader(ns []byte, cert []byte) ([]byte, error) {
	hdr := bytes.Buffer{}
	if len(am.perspectiveHash) != 34 {
		panic(am.perspectiveHash)
	}
	//First: 34 byte entity hash
	hdr.Write(am.perspectiveHash)
	//Second: signature of cert
	sigresp, err := am.wave.Sign(context.Background(), &eapipb.SignParams{
		Perspective: am.ourPerspective,
		Content:     cert,
	})
	if err != nil {
		return nil, err
	}
	if sigresp.Error != nil {
		return nil, errors.New(sigresp.Error.Message)
	}
	siglen := make([]byte, 2)
	sig := sigresp.Signature
	binary.LittleEndian.PutUint16(siglen, uint16(len(sig)))
	hdr.Write(siglen)
	hdr.Write(sig)
	//Third: the namespace proof for this namespace
	proof, ok := am.routingProofs[base64.URLEncoding.EncodeToString(ns)]
	if !ok {
		return nil, fmt.Errorf("we are not a DR for this namespace\n")
	}
	prooflen := make([]byte, 4)
	binary.LittleEndian.PutUint32(prooflen, uint32(len(proof)))
	hdr.Write(prooflen)
	hdr.Write(proof)
	return hdr.Bytes(), nil
}
