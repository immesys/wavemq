package core

import (
	"context"
	"fmt"
	"testing"

	"github.com/immesys/wave/eapi/pb"
	"github.com/immesys/wave/waved"
	"github.com/immesys/wavemq/mqpb"
	"github.com/stretchr/testify/require"
)

func getam() *AuthModule {
	rv := &waved.Configuration{
		Database:     "/tmp/waved",
		ListenIP:     "127.0.0.1:4410",
		HTTPListenIP: "127.0.0.1:4411",
		Storage:      make(map[string]map[string]string),
	}
	rv.Storage["default"] = make(map[string]string)
	rv.Storage["default"]["provider"] = "http_v1"
	rv.Storage["default"]["url"] = "https://standalone.storage.bwave.io/v1"
	rv.Storage["default"]["version"] = "1"
	am, err := NewAuthModule(rv)
	if err != nil {
		panic(err)
	}
	return am
}

var am *AuthModule

func init() {
	am = getam()
}

func TestSubProof(t *testing.T) {
	ns, err := am.wave.CreateEntity(context.Background(), &pb.CreateEntityParams{})
	require.NoError(t, err)
	am.wave.PublishEntity(context.Background(), &pb.PublishEntityParams{
		DER: ns.PublicDER,
	})
	ent, err := am.wave.CreateEntity(context.Background(), &pb.CreateEntityParams{})
	require.NoError(t, err)
	am.wave.PublishEntity(context.Background(), &pb.PublishEntityParams{
		DER: ent.PublicDER,
	})
	attresp, err := am.wave.CreateAttestation(context.Background(), &pb.CreateAttestationParams{
		Perspective: &pb.Perspective{
			EntitySecret: &pb.EntitySecret{
				DER: ns.SecretDER,
			},
		},
		Publish:     true,
		SubjectHash: ent.Hash,
		Policy: &pb.Policy{
			RTreePolicy: &pb.RTreePolicy{
				Namespace: ns.Hash,
				Statements: []*pb.RTreePolicyStatement{
					{
						PermissionSet: []byte(WAVEMQPermissionSet),
						Permissions:   []string{WAVEMQSubscribe},
						Resource:      "foo/bar",
					},
				},
			},
		},
	})
	require.NoError(t, err)
	require.Nil(t, attresp.Error)
	persp := &mqpb.Perspective{
		EntitySecret: &mqpb.EntitySecret{
			DER: ent.SecretDER,
		},
	}
	subreq, err := am.FormSubRequest(&mqpb.SubscribeParams{
		Perspective: persp,
		Namespace:   ns.Hash,
		Uri:         "foo/bar",
		Identifier:  "super-unique",
		Expiry:      120,
	}, "lol")

	require.NoError(t, err)
	err = am.CheckSubscription(subreq)
	require.NoError(t, err)
}
func TestMessageNoProof(t *testing.T) {
	ns, err := am.wave.CreateEntity(context.Background(), &pb.CreateEntityParams{})
	require.NoError(t, err)
	am.wave.PublishEntity(context.Background(), &pb.PublishEntityParams{
		DER: ns.PublicDER,
	})
	ent, err := am.wave.CreateEntity(context.Background(), &pb.CreateEntityParams{})
	require.NoError(t, err)
	am.wave.PublishEntity(context.Background(), &pb.PublishEntityParams{
		DER: ent.PublicDER,
	})
	attresp, err := am.wave.CreateAttestation(context.Background(), &pb.CreateAttestationParams{
		Perspective: &pb.Perspective{
			EntitySecret: &pb.EntitySecret{
				DER: ns.SecretDER,
			},
		},
		Publish:     true,
		SubjectHash: ent.Hash,
		Policy: &pb.Policy{
			RTreePolicy: &pb.RTreePolicy{
				Namespace: ns.Hash,
				Statements: []*pb.RTreePolicyStatement{
					{
						PermissionSet: []byte(WAVEMQPermissionSet),
						Permissions:   []string{WAVEMQPublish},
						Resource:      "foo/bar",
					},
				},
			},
		},
	})
	require.NoError(t, err)
	require.Nil(t, attresp.Error)

	persp := &mqpb.Perspective{
		EntitySecret: &mqpb.EntitySecret{
			DER: ent.SecretDER,
		},
	}
	msg, err := am.FormMessage(&mqpb.PublishParams{
		Perspective: persp,
		Namespace:   ns.Hash,
		Uri:         "foo/baz",
	}, "lol")
	require.NotNil(t, err)
	_ = msg
}

func BenchmarkFormMessage(b *testing.B) {
	ns, err := am.wave.CreateEntity(context.Background(), &pb.CreateEntityParams{})
	require.NoError(b, err)
	am.wave.PublishEntity(context.Background(), &pb.PublishEntityParams{
		DER: ns.PublicDER,
	})
	ent, err := am.wave.CreateEntity(context.Background(), &pb.CreateEntityParams{})
	require.NoError(b, err)
	am.wave.PublishEntity(context.Background(), &pb.PublishEntityParams{
		DER: ent.PublicDER,
	})
	attresp, err := am.wave.CreateAttestation(context.Background(), &pb.CreateAttestationParams{
		Perspective: &pb.Perspective{
			EntitySecret: &pb.EntitySecret{
				DER: ns.SecretDER,
			},
		},
		Publish:     true,
		SubjectHash: ent.Hash,
		Policy: &pb.Policy{
			RTreePolicy: &pb.RTreePolicy{
				Namespace: ns.Hash,
				Statements: []*pb.RTreePolicyStatement{
					{
						PermissionSet: []byte(WAVEMQPermissionSet),
						Permissions:   []string{WAVEMQPublish},
						Resource:      "foo/bar",
					},
				},
			},
		},
	})
	require.NoError(b, err)
	require.Nil(b, attresp.Error)

	persp := &mqpb.Perspective{
		EntitySecret: &mqpb.EntitySecret{
			DER: ent.SecretDER,
		},
	}
	b.ResetTimer()
	fmt.Printf("===== BEGIN <<<<\n")
	for i := 0; i < b.N; i++ {
		msg, err := am.FormMessage(&mqpb.PublishParams{
			Perspective: persp,
			Namespace:   ns.Hash,
			Uri:         "foo/bar",
		}, "lol")
		require.NoError(b, err)
		_ = msg
	}
	fmt.Printf("===== END >>>>>>\n")
}
func TestMessage(t *testing.T) {
	ns, err := am.wave.CreateEntity(context.Background(), &pb.CreateEntityParams{})
	require.NoError(t, err)
	am.wave.PublishEntity(context.Background(), &pb.PublishEntityParams{
		DER: ns.PublicDER,
	})
	ent, err := am.wave.CreateEntity(context.Background(), &pb.CreateEntityParams{})
	require.NoError(t, err)
	am.wave.PublishEntity(context.Background(), &pb.PublishEntityParams{
		DER: ent.PublicDER,
	})
	attresp, err := am.wave.CreateAttestation(context.Background(), &pb.CreateAttestationParams{
		Perspective: &pb.Perspective{
			EntitySecret: &pb.EntitySecret{
				DER: ns.SecretDER,
			},
		},
		Publish:     true,
		SubjectHash: ent.Hash,
		Policy: &pb.Policy{
			RTreePolicy: &pb.RTreePolicy{
				Namespace: ns.Hash,
				Statements: []*pb.RTreePolicyStatement{
					{
						PermissionSet: []byte(WAVEMQPermissionSet),
						Permissions:   []string{WAVEMQPublish},
						Resource:      "foo/bar",
					},
				},
			},
		},
	})
	require.NoError(t, err)
	require.Nil(t, attresp.Error)

	persp := &mqpb.Perspective{
		EntitySecret: &mqpb.EntitySecret{
			DER: ent.SecretDER,
		},
	}
	msg, err := am.FormMessage(&mqpb.PublishParams{
		Perspective: persp,
		Namespace:   ns.Hash,
		Uri:         "foo/bar",
	}, "lol")
	require.NoError(t, err)

	//validate
	try1 := am.CheckMessage(msg)
	require.NoError(t, try1)
	try2 := am.CheckMessage(msg)
	require.NoError(t, try2)
}

func BenchmarkCheckMessage(t *testing.B) {
	ns, err := am.wave.CreateEntity(context.Background(), &pb.CreateEntityParams{})
	require.NoError(t, err)
	am.wave.PublishEntity(context.Background(), &pb.PublishEntityParams{
		DER: ns.PublicDER,
	})
	ent, err := am.wave.CreateEntity(context.Background(), &pb.CreateEntityParams{})
	require.NoError(t, err)
	am.wave.PublishEntity(context.Background(), &pb.PublishEntityParams{
		DER: ent.PublicDER,
	})
	attresp, err := am.wave.CreateAttestation(context.Background(), &pb.CreateAttestationParams{
		Perspective: &pb.Perspective{
			EntitySecret: &pb.EntitySecret{
				DER: ns.SecretDER,
			},
		},
		Publish:     true,
		SubjectHash: ent.Hash,
		Policy: &pb.Policy{
			RTreePolicy: &pb.RTreePolicy{
				Namespace: ns.Hash,
				Statements: []*pb.RTreePolicyStatement{
					{
						PermissionSet: []byte(WAVEMQPermissionSet),
						Permissions:   []string{WAVEMQPublish},
						Resource:      "foo/bar",
					},
				},
			},
		},
	})
	require.NoError(t, err)
	require.Nil(t, attresp.Error)

	persp := &mqpb.Perspective{
		EntitySecret: &mqpb.EntitySecret{
			DER: ent.SecretDER,
		},
	}
	msg, err := am.FormMessage(&mqpb.PublishParams{
		Perspective: persp,
		Namespace:   ns.Hash,
		Uri:         "foo/bar",
	}, "lol")
	require.NoError(t, err)

	t.ResetTimer()
	fmt.Printf("===== BEGIN <<<<\n")
	for i := 0; i < t.N; i++ {
		//validate
		try1 := am.CheckMessage(msg)
		require.NoError(t, try1)
	}
	fmt.Printf("===== END >>>>>>\n")
}
