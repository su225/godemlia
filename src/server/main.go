package main

import (
	"flag"
	"log"

	"github.com/su225/godemlia/src/server/config"
	"github.com/su225/godemlia/src/server/service"
)

func main() {
	nodeID := flag.Uint64("id", 0, "ID of the node")
	ipAddress := flag.String("ip", "127.0.0.1", "IP address of the node")
	port := flag.Uint("port", 6666, "Port on which server listens")
	restPort := flag.Uint("rest-port", 6667, "Port on which clients can contact")
	bootstrap := flag.Bool("bootstrap", false, "is this the first node?")
	joinAddress := flag.String("join", "", "Address to contact to get config")
	concurrency := flag.Uint("concurrency", 1, "Concurrency factor of the node")
	replication := flag.Uint("replication", 3, "Replication factor for the node")

	flag.Parse()

	// Current node configuration (NodeID, IP, Port)
	currentNodeInfo := &config.NodeInfo{
		NodeID:    *nodeID,
		IPAddress: *ipAddress,
		Port:      uint32(*port),
	}

	log.Printf("Config: concurrency=%d, replication=%d", *concurrency, *replication)
	suppliedConfig := &config.Configuration{
		ConcurrencyFactor: uint32(*concurrency),
		ReplicationFactor: uint32(*replication),
	}

	suppliedRESTConfig := &config.RESTServerConfiguration{
		RESTPort: uint32(*restPort),
	}

	// Create the node context with the nodeInfo and supplied config. Note that,
	// supplied config is overriden by the actual network configuration if it is
	// not in bootstrap mode.
	log.Printf("Creating node config for node [%d, %s, %d]", *nodeID, *ipAddress, *port)
	nodeContext, err := service.CreateNodeContext(suppliedConfig, currentNodeInfo, 4, suppliedRESTConfig)
	if err != nil {
		log.Printf("Could not create node context. Exiting...")
		return
	}

	// Start node context. In case it is not bootstrap mode then contact one of the
	// nodes already in the network, obtain configuration. Then start RPC server.
	log.Printf("Starting node context...bootstrap_node=%t", *bootstrap)
	if *bootstrap && len(*joinAddress) > 0 {
		log.Printf("In bootstrap mode, join address is not needed. Ignoring it...")
	}
	nodeContext.StartNodeContext(*bootstrap, []string{*joinAddress})
}
