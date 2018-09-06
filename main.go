package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/BurntSushi/toml"
	"github.com/immesys/wave/waved"
	"github.com/immesys/wavemq/core"
	"github.com/immesys/wavemq/server"
)

//TODO upstream doesn't seem to work properly if dr starts disconnected
//TODO persist router ID
type Configuration struct {
	RoutingConfig core.RoutingConfig
	WaveConfig    waved.Configuration
	QueueConfig   core.QManagerConfig
	LocalConfig   server.LocalServerConfig
	PeerConfig    server.PeerServerConfig
}

func main() {
	if len(os.Args) != 2 {
		fmt.Printf("usage: wavemq config.toml\n")
		os.Exit(1)
	}
	file := os.Args[1]
	var conf Configuration
	if _, err := toml.DecodeFile(file, &conf); err != nil {
		fmt.Printf("failed to load configuration: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("configuration loaded\n")
	qm, err := core.NewQManager(&conf.QueueConfig)
	if err != nil {
		fmt.Printf("failed to initialize queues: %v\n", err)
		os.Exit(1)
	}
	am, err := core.NewAuthModule(&conf.WaveConfig)
	if err != nil {
		fmt.Printf("failed to initialize auth: %v\n", err)
		os.Exit(1)
	}
	tm, err := core.NewTerminus(qm, am, &conf.RoutingConfig)
	if err != nil {
		fmt.Printf("failed to initialize routing: %v\n", err)
		os.Exit(1)
	}

	server.NewLocalServer(tm, am, &conf.LocalConfig)
	server.NewPeerServer(tm, am, &conf.PeerConfig)

	sigchan := make(chan os.Signal, 30)
	signal.Notify(sigchan, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	<-sigchan
	fmt.Printf("SHUTTING DOWN\n")
	qm.Shutdown()
}
