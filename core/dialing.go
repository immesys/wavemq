package core

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"time"

	"github.com/cloudflare/cfssl/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/encoding/gzip"
)

// type TransportCredentials interface {
// 	// ClientHandshake does the authentication handshake specified by the corresponding
// 	// authentication protocol on rawConn for clients. It returns the authenticated
// 	// connection and the corresponding auth information about the connection.
// 	// Implementations must use the provided context to implement timely cancellation.
// 	// gRPC will try to reconnect if the error returned is a temporary error
// 	// (io.EOF, context.DeadlineExceeded or err.Temporary() == true).
// 	// If the returned error is a wrapper error, implementations should make sure that
// 	// the error implements Temporary() to have the correct retry behaviors.
// 	//
// 	// If the returned net.Conn is closed, it MUST close the net.Conn provided.
// 	ClientHandshake(context.Context, string, net.Conn) (net.Conn, AuthInfo, error)
// 	// ServerHandshake does the authentication handshake for servers. It returns
// 	// the authenticated connection and the corresponding auth information about
// 	// the connection.
// 	//
// 	// If the returned net.Conn is closed, it MUST close the net.Conn provided.
// 	ServerHandshake(net.Conn) (net.Conn, AuthInfo, error)
// 	// Info provides the ProtocolInfo of this TransportCredentials.
// 	Info() ProtocolInfo
// 	// Clone makes a copy of this TransportCredentials.
// 	Clone() TransportCredentials
// 	// OverrideServerName overrides the server name used to verify the hostname on the returned certificates from the server.
// 	// gRPC internals also use it to override the virtual hosting name if it is set.
// 	// It must be called before dialing. Currently, this is only used by grpclb.
// 	OverrideServerName(string) error
// }

type peerTC struct {
	t         *Terminus
	namespace string
}

func (ptc *peerTC) ClientHandshake(ctx context.Context, authority string, rawConn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	roots := x509.NewCertPool()

	conn := tls.Client(rawConn, &tls.Config{
		InsecureSkipVerify: true,
		RootCAs:            roots,
	})

	err := conn.Handshake()
	if err != nil {
		rawConn.Close()
		return nil, nil, err
	}
	cs := conn.ConnectionState()
	if len(cs.PeerCertificates) != 1 {
		fmt.Printf("peer connection weird response")
		rawConn.Close()
		return nil, nil, errors.New("Wrong certificates")
	}

	nsbin, err := base64.URLEncoding.DecodeString(ptc.namespace)
	if err != nil {
		panic(err)
	}
	_, err = conn.Write(nsbin)
	if err != nil {
		rawConn.Close()
		return nil, nil, fmt.Errorf("could not read proof: %v\n", err)
	}

	entityHashBA := make([]byte, 34)
	_, err = io.ReadFull(conn, entityHashBA)
	if err != nil {
		rawConn.Close()
		return nil, nil, fmt.Errorf("could not read proof: %v\n", err)
	}
	signatureSizeBA := make([]byte, 2)
	_, err = io.ReadFull(conn, signatureSizeBA)
	if err != nil {
		rawConn.Close()
		return nil, nil, fmt.Errorf("could not read proof: %v\n", err)
	}
	signatureSize := binary.LittleEndian.Uint16(signatureSizeBA)
	signature := make([]byte, signatureSize)
	_, err = io.ReadFull(conn, signature)
	if err != nil {
		rawConn.Close()
		return nil, nil, fmt.Errorf("could not read proof: %v\n", err)
	}
	proofSizeBA := make([]byte, 4)
	_, err = io.ReadFull(conn, proofSizeBA)
	if err != nil {
		rawConn.Close()
		return nil, nil, fmt.Errorf("could not read proof: %v\n", err)
	}
	proofSize := binary.LittleEndian.Uint32(proofSizeBA)
	if proofSize > 10*1024*1024 {
		rawConn.Close()
		return nil, nil, fmt.Errorf("bad proof")
	}
	proof := make([]byte, proofSize)
	_, err = io.ReadFull(conn, proof)
	if err != nil {
		rawConn.Close()
		return nil, nil, fmt.Errorf("could not read proof: %v\n", err)
	}
	//First verify the signature
	err = ptc.t.am.VerifyServerHandshake(ptc.namespace, entityHashBA, signature, proof, cs.PeerCertificates[0].Signature)
	if err != nil {
		rawConn.Close()
		return nil, nil, err
	}

	return conn, nil, nil
}

func (ptc *peerTC) ServerHandshake(rawConn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	//Generate TLS certificate
	cert, cert2 := genCert()
	tlsConfig := tls.Config{Certificates: []tls.Certificate{cert}}
	conn := tls.Server(rawConn, &tlsConfig)

	err := conn.Handshake()
	if err != nil {
		rawConn.Close()
		return nil, nil, err
	}
	namespace := make([]byte, 34)
	_, err = io.ReadFull(conn, namespace)
	if err != nil {
		rawConn.Close()
		return nil, nil, fmt.Errorf("could not generate header: %v", err)
	}
	header, err := ptc.t.am.GeneratePeerHeader(namespace, cert2.Signature)
	if err != nil {
		rawConn.Close()
		return nil, nil, fmt.Errorf("could not generate header: %v", err)
	}
	_, err = conn.Write(header)
	if err != nil {
		rawConn.Close()
		return nil, nil, err
	}
	return conn, nil, nil
}

func (ptc *peerTC) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{
		SecurityProtocol: "tls",
		SecurityVersion:  "1.2",
	}
}
func (ptc *peerTC) Clone() credentials.TransportCredentials {
	return &peerTC{
		t:         ptc.t,
		namespace: ptc.namespace,
	}
}
func (ptc *peerTC) OverrideServerName(name string) error {
	return nil
}

//Peer protocol is
// CL -> DR namespace
// DR -> CL  (entity public der, signed TLS cert, proof of route)
func (t *Terminus) dialPeer(address string, namespace string) (*grpc.ClientConn, error) {
	tc := &peerTC{
		t:         t,
		namespace: namespace,
	}
	return grpc.Dial(address, grpc.WithTransportCredentials(tc), grpc.FailOnNonTempDialError(true), grpc.WithBlock(), grpc.WithTimeout(30*time.Second), grpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name)))
}

func (t *Terminus) ServerTransportCredentials() credentials.TransportCredentials {
	return &peerTC{
		t: t,
	}
}

func genCert() (tls.Certificate, *x509.Certificate) {
	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		log.Criticalf("failed to generate serial number: %s", err)
		panic(err)
	}
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName: "wavemq-dr",
		},
		NotBefore: time.Now(),
		NotAfter:  time.Now().Add(365 * 24 * time.Hour),

		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	template.IsCA = true
	template.KeyUsage |= x509.KeyUsageCertSign
	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		log.Criticalf("Failed to create certificate: %s", err)
		panic(err)
	}
	x509cert, err := x509.ParseCertificate(derBytes)
	if err != nil {
		panic(err)
	}

	keybytes := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})
	certbytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	cert, err := tls.X509KeyPair(certbytes, keybytes)
	if err != nil {
		panic(err)
	}
	return cert, x509cert
}

//
// func Start(bw *BW) {
// 	//Generate TLS certificate
// 	vk := crypto.FmtKey(bw.Entity.GetVK())
// 	cert, cert2 := genCert(vk)
// 	tlsConfig := tls.Config{Certificates: []tls.Certificate{cert}}
// 	ln, err := tls.Listen("tcp", bw.Config.Native.ListenOn, &tlsConfig)
// 	log.Info("peer server listening on:", bw.Config.Native.ListenOn)
// 	if err != nil {
// 		log.Criticalf("Could not open native adapter socket: %v", err)
// 		os.Exit(1)
// 	}
// 	proof := make([]byte, 32+64)
// 	copy(proof, bw.Entity.GetVK())
// 	if err != nil {
// 		log.Criticalf("Could not parse certificate")
// 		log.Flush()
// 		os.Exit(1)
// 	}
// 	crypto.SignBlob(bw.Entity.GetSK(), bw.Entity.GetVK(), proof[32:], cert2.Signature)
// 	for {
// 		conn, err := ln.Accept()
// 		if err != nil {
// 			log.Criticalf("Socket error: %v", err)
// 		}
// 		//First thing we do is write the 96 byte proof that the self-signed cert was
// 		//generated by the person posessing the router's SK
// 		conn.Write(proof)
// 		//Create a client
// 		cl := bw.CreateClient(context.Background(), "PEER:"+conn.RemoteAddr().String())
// 		//Then handle the session
// 		go handleSession(cl, conn)
// 	}
// }
