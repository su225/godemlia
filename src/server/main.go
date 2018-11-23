package main

import (
	"flag"
	"log"
	"math/rand"
	"strconv"
	"strings"

	"github.com/su225/godemlia/src/server/config"
	"github.com/su225/godemlia/src/server/service"
)

func main() {
	nodeID := flag.String("id", "", "ID of the node")
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
		IPAddress: *ipAddress,
		Port:      uint32(*port),
	}

	// If the nodeID is not specified then generate
	// a random unsigned 64-bit number as node. Dtherwise
	// use the supplied nodeID.
	if len(*nodeID) != 0 {
		if nodeIDUint, parseErr := strconv.ParseUint(*nodeID, 10, 64); parseErr != nil {
			log.Println("[STARTUP] Failed to parse the nodeID. Generating one...")
			currentNodeInfo.NodeID = rand.Uint64()
		} else {
			currentNodeInfo.NodeID = nodeIDUint
		}
	} else {
		log.Println("[STARTUP] nodeID not specified. Generating one...")
		currentNodeInfo.NodeID = rand.Uint64()
	}

	log.Printf("[STARTUP] Current NodeID = %d", currentNodeInfo.NodeID)
	log.Printf("[STARTUP] Config: concurrency=%d, replication=%d", *concurrency, *replication)
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
	log.Printf("[STARTUP] Creating node config for node [%d, %s, %d]",
		currentNodeInfo.NodeID, currentNodeInfo.IPAddress, currentNodeInfo.Port)
	nodeContext, err := service.CreateNodeContext(suppliedConfig, currentNodeInfo, 4, suppliedRESTConfig)
	if err != nil {
		log.Printf("[STARTUP] Could not create node context. Exiting...")
		return
	}

	// Start node context. In case it is not bootstrap mode then contact one of the
	// nodes already in the network, obtain configuration. Then start RPC server.
	log.Printf("[STARTUP] Starting node context...bootstrap_node=%t", *bootstrap)
	if *bootstrap && len(*joinAddress) > 0 {
		log.Printf("[STARTUP] In bootstrap mode, join address is not needed. Ignoring it...")
	}

	// Parse the comma separated join addresses
	joinAddresses := strings.Split(*joinAddress, ",")
	nodeContext.StartNodeContext(*bootstrap, joinAddresses)
}
