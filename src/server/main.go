package main

import (
	"flag"
	"log"
	"math/rand"
	"net"
	"strconv"
	"strings"

	"github.com/su225/godemlia/src/server/config"
	"github.com/su225/godemlia/src/server/service"
)

func main() {
	nodeID := flag.String("id", "", "ID of the node")
	port := flag.Uint("port", 6666, "Port on which server listens")
	restPort := flag.Uint("rest-port", 6667, "Port on which clients can contact")
	bootstrap := flag.Bool("bootstrap", false, "is this the first node?")
	joinAddress := flag.String("join", "", "Address to contact to get config")
	concurrency := flag.Uint("concurrency", 1, "Concurrency factor of the node")
	replication := flag.Uint("replication", 3, "Replication factor for the node")

	// These flags must be present only when running inside Kubernetes cluster
	k8sFlag := flag.Bool("k8s", false, "True if the node is running in Kubernetes cluster")
	k8sPodIP := flag.String("k8s-pod-ip", "127.0.0.1", "IP of the pod in the K8S cluster")

	flag.Parse()

	// Current node configuration (NodeID, IP, Port)
	currentNodeInfo := &config.NodeInfo{Port: uint32(*port)}

	// When inside Kubernetes cluster, get Pod IP. Else IP address
	// is the address advertised by this node for other nodes in the
	// cluster. That is, the address in which this node can be contacted.
	if *k8sFlag {
		currentNodeInfo.IPAddress = *k8sPodIP
	} else {
		// Pings udp://8.8.8.8:80 and gets the local address
		// from the resulting connection.
		currentNodeInfo.IPAddress = getExternalIPAddress()
	}
	log.Printf("[STARTUP] IP address of this node = %s", currentNodeInfo.IPAddress)

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
	nodeContext, err := service.CreateNodeContext(suppliedConfig, currentNodeInfo, 4, suppliedRESTConfig, *k8sFlag)
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
	if startErr := nodeContext.StartNodeContext(*bootstrap, joinAddresses); startErr != nil {
		log.Printf("[STARTUP] Unable to start the node. Error=%s, Terminating...", startErr.Error())
	}
}

// Obtain the external IP address used by this machine
func getExternalIPAddress() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Println("[IP] Error while obtaining IP address for this node")
		return ""
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr).String()

	ipv4HostPortSplit := strings.Split(localAddr, ":")
	if len(ipv4HostPortSplit) != 2 {
		log.Printf("[IP] Error while parsing %s as IPv4 address", localAddr)
		return localAddr
	}
	return ipv4HostPortSplit[0]
}
