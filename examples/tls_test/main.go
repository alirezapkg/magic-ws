package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"log"
	"math/big"
	"net"
	"net/http"
	"sync"

	magicws "github.com/alirezapkg/magic-ws"
)

func GenerateInMemoryTLSConfig() (*tls.Config, error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"MagicWS Test"},
		},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:    []string{"localhost"},
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return nil, err
	}

	cert := tls.Certificate{
		Certificate: [][]byte{derBytes},
		PrivateKey:  priv,
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
	}, nil
}

func main() {
	serverTLSConfig, err := GenerateInMemoryTLSConfig()
	if err != nil {
		log.Fatalf("Failed to generate in-memory TLS cert: %v", err)
	}

	server := magicws.NewServer()
	serverEvents := magicws.NewServerEvents(server)

	var echoEvent = magicws.NewEvent("ping_pong")

	serverEvents.On(echoEvent, func(u *magicws.User, payload []byte) {
		fmt.Printf("[Server WSS] Received over TLS: %s\n", string(payload))
		serverEvents.Emit(u, echoEvent, []byte("PONG_SECURE"))
	})

	l, err := tls.Listen("tcp", "127.0.0.1:8443", serverTLSConfig)
	if err != nil {
		log.Fatalf("Failed to listen on 8443: %v", err)
	}
	defer l.Close()

	go func() {
		srv := &http.Server{
			Handler: http.HandlerFunc(server.HandleWS),
		}
		_ = srv.Serve(l)
	}()

	fmt.Println("🔒 Secure TLS Server started on wss://127.0.0.1:8443/ws")

	client := magicws.NewClient()
	client.SetTLSConfig(&tls.Config{
		InsecureSkipVerify: true,
	})

	clientEvents := magicws.NewClientEvents(client)

	var wg sync.WaitGroup
	wg.Add(1)

	clientEvents.On(echoEvent, func(payload []byte) {
		fmt.Printf("[Client WSS] Received Response over TLS: %s\n", string(payload))
		wg.Done()
	})

	ctx := context.Background()
	err = client.Connect(ctx, "wss://127.0.0.1:8443/ws")
	if err != nil {
		log.Fatalf("Client failed to connect via WSS: %v", err)
	}
	defer client.Close()

	fmt.Println("🚀 Client connected successfully via wss:// protocol!")

	clientEvents.Emit(echoEvent, []byte("PING_SECURE"))

	wg.Wait()
	fmt.Println("✅ TLS Test completed successfully!")
}
