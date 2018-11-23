package service

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/go-chi/render"
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

	// ClosestNodeLocator is responsible for finding closest
	// node for a given key/nodeID
	Locator *ClosestNodeLocator

	// RESTConfig contains the configuration of the REST
	// server for the clients to contact.
	RESTConfig           *config.RESTServerConfiguration
	ClientRequestHandler RESTHandler

	// NodeDataContext is responsible for handling data
	// related operations like storage and retrieval,
	// rebalancing and garbage collecting stale data etc.
	*NodeDataContext
}

// CreateNodeContext creates and initializes all the necessary components required
// for a Kademlia node to function.
func CreateNodeContext(netConfig *config.Configuration,
	nodeInfo *config.NodeInfo,
	refreshPeriod time.Duration,
	restConfig *config.RESTServerConfiguration,
) (*NodeContext, error) {
	nodeContext := &NodeContext{
		Config:          netConfig,
		CurrentNodeInfo: nodeInfo,
		RESTConfig:      restConfig,
	}

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

	// Create closest node locator
	nodeContext.Locator = &ClosestNodeLocator{NodeCtx: nodeContext}

	// Create Node data context
	nodeContext.NodeDataContext = CreateNodeDataContext(nodeContext)

	// Create REST Server handler
	nodeContext.ClientRequestHandler = CreateKademliaRESTHandler(nodeContext)

	return nodeContext, nil
}

// StartNodeContext starts the Rpc server and if it is not in bootstrap mode then attempts to
// join the Kademlia network with the given NodeID and Address. If the node is a bootstrap node then
// given join addresses are ignored since it must be the first node in the network. When it is not,
// join addresses are contacted to obtain the network configuration and the node context is populated
func (ctx *NodeContext) StartNodeContext(isBootstrap bool, joinAddresses []string) error {
	// If this is not a bootstrap node then try to obtain configuration
	// from one of the nodes already in the network.
	if len(joinAddresses) == 0 && !isBootstrap {
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
		if !isBootstrap {
			return errors.New("Cannot join network")
		} else {
			log.Printf("This can be a bootstrap node. Continue..")
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

	// Once RPC listener is up, start the REST server as well.
	ctx.startRESTServer()

	// Start the data related services
	ctx.NodeDataContext.Start()

	log.Printf("Starting RPC server at %s:%d", ctx.CurrentNodeInfo.IPAddress, ctx.CurrentNodeInfo.Port)
	if serveErr := grpcServer.Serve(listener); serveErr != nil {
		return serveErr
	}
	return nil
}

// startRESTServer starts the REST server at the specified port in this node's
// IP Address. Note that RPC and REST Server ports must be different.
func (ctx *NodeContext) startRESTServer() {
	chiRouter := ctx.getChiRouter()
	restServerListenAddress := fmt.Sprintf("%s:%d", ctx.CurrentNodeInfo.IPAddress, ctx.RESTConfig.RESTPort)
	httpListener, httpListenerErr := net.Listen("tcp", restServerListenAddress)
	if httpListenerErr != nil {
		log.Fatalf("Error while starting REST server at address %s. Reason=%s", restServerListenAddress, httpListenerErr.Error())
		return
	}

	// Once the HTTP listener is up, start serving client requests.
	// This must be called asynchronously because this should not block
	// starting of RPC server.
	log.Printf("Starting REST server at address %s", restServerListenAddress)
	go func() {
		if httpServeErr := http.Serve(httpListener, chiRouter); httpServeErr != nil {
			log.Fatalf("Cannot bring up REST server. Reason=%s", httpServeErr.Error())
		}
	}()
}

// getChiRouter sets up routes and handlers for the REST server and returns
// the handle to the multiplexer.
func (ctx *NodeContext) getChiRouter() *chi.Mux {
	chiRouter := chi.NewRouter()
	chiRouter.Use(
		render.SetContentType(render.ContentTypeJSON),
		middleware.Recoverer,
		middleware.Timeout(60*time.Second),
	)

	chiRouter.Get("/data/{key}", ctx.ClientRequestHandler.GetData)
	chiRouter.Post("/data", ctx.ClientRequestHandler.PutData)
	return chiRouter
}
