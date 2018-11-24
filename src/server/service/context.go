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
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
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

	// KubernetesFlag is set if the node is running on
	// a Kubernetes cluster
	KubernetesFlag bool
}

// CreateNodeContext creates and initializes all the necessary components required
// for a Kademlia node to function.
func CreateNodeContext(netConfig *config.Configuration,
	nodeInfo *config.NodeInfo,
	refreshPeriod time.Duration,
	restConfig *config.RESTServerConfiguration,
	k8sFlag bool,
) (*NodeContext, error) {
	nodeContext := &NodeContext{
		Config:          netConfig,
		CurrentNodeInfo: nodeInfo,
		RESTConfig:      restConfig,
		KubernetesFlag:  k8sFlag,
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
	// If the node is running in a Kubernetes cluster, then attempt to obtain
	// addresses of other kademlia nodes
	if len(joinAddresses) == 0 && ctx.KubernetesFlag {
		joinAddresses = ctx.getJoinAddressFromKubernetesCluster()
	}

	// If this is not a bootstrap node then try to obtain configuration
	// from one of the nodes already in the network.
	if len(joinAddresses) == 0 && !isBootstrap {
		return errors.New("[JOIN] At least one join address must be specified")
	}
	// Try joining the cluster by trying the addresses one by one. If one of them
	// return the configuration then set it and stop.
	if joinedNetwork := ctx.joinNetwork(joinAddresses, uint32(3)); !joinedNetwork {
		if !isBootstrap {
			log.Printf("[JOIN] Unable to join the network. Terminating...")
			return errors.New("Cannot join network")
		} else {
			log.Printf("[JOIN] This can be a bootstrap node. Continue..")
		}
	}

	// Start the table refresh job too
	log.Printf("[NODE] Starting routing table refresher")
	ctx.Refresher.Start()

	// Configure gRPC server and try to start it. Note that if the server starts successfully,
	// then this thread blocks. So if there is anything else to be processed make sure this is
	// called asynchronously.
	rpcServerListenAddr := fmt.Sprintf(":%d", ctx.CurrentNodeInfo.Port)
	listener, listenerErr := net.Listen("tcp", rpcServerListenAddr)
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

	log.Printf("[NODE] Starting RPC server at %s:%d", ctx.CurrentNodeInfo.IPAddress, ctx.CurrentNodeInfo.Port)
	if serveErr := grpcServer.Serve(listener); serveErr != nil {
		return serveErr
	}
	return nil
}

// getJoinAddressFromKubernetesCluster gets the join addresses of other Kademlia nodes in
// the Kubernetes cluster and then attempts to join the cluster through one of them
func (ctx *NodeContext) getJoinAddressFromKubernetesCluster() []string {
	var kubeClient *kubernetes.Clientset
	var err error
	kubeClient, kubeErr := ctx.getKubernetesClient()
	if kubeErr != nil {
		log.Printf("[K8S-JOIN] Error while obtaining K8S apiserver contact info. Error=%s", kubeErr.Error())
		return []string{}
	}
	// Select all the pods with "kademlia-node" label and set the
	// limit to the number of such nodes.
	kademliaPodOptions := metaV1.ListOptions{
		LabelSelector: "kademlia-node",
		Limit:         10,
	}
	// Get the list of all nodes in namespace kademlia-k8s with label "kademlia-node"
	kademliaPodList, err := kubeClient.CoreV1().Pods("kademlia-k8s").List(kademliaPodOptions)
	if err != nil {
		log.Printf("[K8S-JOIN] Error while retrieving other kademlia nodes in the cluster. Error=%s", err.Error())
		return []string{}
	}
	// Once the list of pods with Kademlia label is obtained, find the running
	// nodes which can be contacted. TODO: Add pod status check and consider only
	// those which are currently running.
	kademliaPods := kademliaPodList.Items
	contactNodes := make([]string, 0)
	for _, kpod := range kademliaPods {
		// WARNING: There is an assumption here that all Kademlia nodes use
		// the same port for RPC which is true when running in a Kubernetes cluster.
		contactAddress := fmt.Sprintf("%s:%d", kpod.Status.PodIP, ctx.CurrentNodeInfo.Port)
		contactNodes = append(contactNodes, contactAddress)
	}
	return contactNodes
}

// getKubernetesClient gets the client to contact apiserver in the cluster
func (ctx *NodeContext) getKubernetesClient() (*kubernetes.Clientset, error) {
	var config *rest.Config
	var err error
	config, err = rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(config)
}

// joinNetwork is responsible for contacting a node and getting the cluster configuration. One of the mechanism
// is to contact one of the addresses specified.
func (ctx *NodeContext) joinNetwork(joinAddresses []string, maxAttempts uint32) bool {
	joinedNetwork := false
	// For each join address given, try connecting the node to get the configuration
	// Retry with exponential delay. If all attempts to join the addresses in the cluster
	// fail then return false. Otherwise it is true.
	for _, joinAddr := range joinAddresses {
		for attempt := uint32(1); attempt <= maxAttempts; attempt++ {
			log.Printf("[JOIN] Attempting to join on %s. Attempt %d", joinAddr, attempt)
			if clusterConfig, err := ctx.CommHandler.JoinNetwork(joinAddr); err != nil {
				log.Printf("[JOIN] Unable to obtain configuration through %s. Reason=%s", joinAddr, err.Error())
			} else {
				joinedNetwork = true
				ctx.Config = clusterConfig
				log.Printf("[JOIN] Obtained configuration from %s successfully", joinAddr)
				log.Printf("[JOIN] Concurrency=%d, Replication=%d",
					clusterConfig.ConcurrencyFactor, clusterConfig.ReplicationFactor)
				break
			}
			// If it is the last attempt then don't wait. Move one
			if attempt < maxAttempts {
				waitDurationInSecond := (1 << (attempt - 1)) * time.Second
				log.Printf("[JOIN] Attempt %d to join cluster through %s failed. Trying after %f seconds",
					attempt, joinAddr, waitDurationInSecond.Seconds())
				time.Sleep(waitDurationInSecond)
			}
		}
		if joinedNetwork {
			break
		}
	}
	return joinedNetwork
}

// startRESTServer starts the REST server at the specified port in this node's
// IP Address. Note that RPC and REST Server ports must be different.
func (ctx *NodeContext) startRESTServer() {
	chiRouter := ctx.getChiRouter()
	restServerListenAddress := fmt.Sprintf(":%d", ctx.RESTConfig.RESTPort)
	httpListener, httpListenerErr := net.Listen("tcp", restServerListenAddress)
	if httpListenerErr != nil {
		log.Fatalf("[NODE] Error while starting REST server at address %s. Reason=%s", restServerListenAddress, httpListenerErr.Error())
		return
	}

	// Once the HTTP listener is up, start serving client requests.
	// This must be called asynchronously because this should not block
	// starting of RPC server.
	log.Printf("[NODE] Starting REST server at address %s", restServerListenAddress)
	go func() {
		if httpServeErr := http.Serve(httpListener, chiRouter); httpServeErr != nil {
			log.Fatalf("[NODE] Cannot bring up REST server. Reason=%s", httpServeErr.Error())
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
