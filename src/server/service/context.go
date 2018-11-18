package service

import (
	"errors"
	"fmt"
	"log"
	"net"
	"time"

	pb "github.com/su225/godemlia/src/kademliapb"
	"github.com/su225/godemlia/src/server/config"
	"github.com/su225/godemlia/src/server/network"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// NodeContext provides access to various parts of
// the software stack of the node.
type NodeContext struct {
	// Config represents the network-wide configuration
	// like ReplicationFactor, ConcurrencyFactor etc.
	Config *config.Configuration

	// CurrentNodeInfo represents the contact info
	// of the current node like IP address, UDP port
	// and Node identifier (NodeID) in the network
	CurrentNodeInfo *config.NodeInfo

	// Represents the routing table used by this node
	// This contains the knowledge of the node about
	// other nodes in the network.
	ContactNodeTable network.RoutingTable

	// Server to handle incoming kademlia protocol messages
	// Now, it is tied to a particular format. In future, it
	// must be changed to make it format independent.
	MessagesHandler pb.KademliaProtocolServer

	// Communication handler to send messages to other nodes
	CommHandler *network.CommunicationHandler

	// RoutingTableRefresher refreshes the routing table by
	// querying neighboring nodes for the closest nodes to
	// this node's ID.
	Refresher     *RoutingTableRefresher
	RefreshPeriod time.Duration
}

// CreateNodeContext creates and initializes all the necessary components required
// for a Kademlia node to function.
func CreateNodeContext(netConfig *config.Configuration, nodeInfo *config.NodeInfo, refreshPeriod time.Duration) (*NodeContext, error) {
	nodeContext := &NodeContext{Config: netConfig, CurrentNodeInfo: nodeInfo}

	// Create the routing table and add current node information to it
	// This is required to mark the bucket to which the current node belongs to
	// This is helpful in splitting buckets when needed
	treeRoutingTable, err := network.CreateTreeRoutingTable(nodeInfo.NodeID, 1024, netConfig.ConcurrencyFactor)
	if err != nil {
		log.Printf("Cannot create routing table.")
		return nil, err
	}
	treeRoutingTable.AddNode(nodeInfo)

	// Initialize the protocol buffer server to handle various Kademlia protocol messages
	msgHandler := CreateKademliaMessagesHandler(nodeContext)
	nodeContext.MessagesHandler = msgHandler

	// Initialize the communication handler
	commHandler := network.CreateCommunicationHandler(treeRoutingTable, nodeInfo)
	nodeContext.CommHandler = commHandler

	// This is the smart routing table which adds liveliness probe capabilities
	nodeContext.ContactNodeTable = CreateRoutingTableHandler(commHandler, treeRoutingTable)

	// Create a new routing table refresher
	nodeContext.RefreshPeriod = refreshPeriod
	refresher := CreateRoutingTableRefresher(commHandler, nodeContext.ContactNodeTable, nodeContext.RefreshPeriod,
		nodeContext.Config, nodeInfo.NodeID)
	nodeContext.Refresher = refresher

	return nodeContext, nil
}

// StartNodeContext starts the Rpc server and if it is not in bootstrap mode then attempts to
// join the Kademlia network with the given NodeID and Address. If the node is a bootstrap node then
// given join addresses are ignored since it must be the first node in the network. When it is not,
// join addresses are contacted to obtain the network configuration and the node context is populated
func (ctx *NodeContext) StartNodeContext(isBootstrap bool, joinAddresses []string) error {
	// If this is not a bootstrap node then try to obtain configuration
	// from one of the nodes already in the network.
	if !isBootstrap {
		if len(joinAddresses) == 0 {
			return errors.New("At least one join address must be specified")
		}
		// Try joining the cluster by trying the addresses one by one. If one of them
		// return the configuration then set it and stop.
		joinedNetwork := false
		for _, joinAddr := range joinAddresses {
			if clusterConfig, err := ctx.CommHandler.JoinNetwork(joinAddr); err != nil {
				log.Printf("Unable to obtain configuration through %s. Reason=%s", joinAddr, err.Error())
			} else {
				joinedNetwork = true
				ctx.Config = clusterConfig
				log.Printf("Obtained configuration from %s successfully", joinAddr)
				log.Printf("Concurrency=%d, Replication=%d",
					clusterConfig.ConcurrencyFactor, clusterConfig.ReplicationFactor)
				break
			}
		}
		if !joinedNetwork {
			log.Printf("Unable to join the network. Terminating...")
			return errors.New("Cannot join network")
		}
	}

	// Start the table refresh job too
	log.Printf("Starting routing table refresher")
	ctx.Refresher.Start()

	// Configure gRPC server and try to start it. Note that if the server starts successfully,
	// then this thread blocks. So if there is anything else to be processed make sure this is
	// called asynchronously.
	listener, listenerErr := net.Listen("tcp", fmt.Sprintf("%s:%d", ctx.CurrentNodeInfo.IPAddress, ctx.CurrentNodeInfo.Port))
	if listenerErr != nil {
		return listenerErr
	}
	grpcServer := grpc.NewServer()
	pb.RegisterKademliaProtocolServer(grpcServer, ctx.MessagesHandler)
	reflection.Register(grpcServer)

	log.Printf("Starting RPC server at %s:%d", ctx.CurrentNodeInfo.IPAddress, ctx.CurrentNodeInfo.Port)
	if serveErr := grpcServer.Serve(listener); serveErr != nil {
		return serveErr
	}
	return nil
}
