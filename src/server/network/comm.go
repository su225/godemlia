package network

import (
	"context"
	"log"
	"sync"
	"time"

	pb "github.com/su225/godemlia/src/kademliapb"
	"github.com/su225/godemlia/src/server/config"
	"github.com/su225/godemlia/src/server/utils"
	"google.golang.org/grpc"
)

// CommunicationHandler is responsible for handling the routing table
// and contacting other nodes when necessary
type CommunicationHandler struct {
	mutex            sync.RWMutex
	ContactNodeTable RoutingTable
	PeerInfo         map[uint64]*config.NodeInfo
}

// CreateCommunicationHandler creates a communication handler which is responsible
// for maintaining routing table, contacting other nodes when necessary and also
// to refresh the routing table every now and then.
func CreateCommunicationHandler(routingTable RoutingTable) *CommunicationHandler {
	return &CommunicationHandler{
		mutex:            sync.RWMutex{},
		ContactNodeTable: routingTable,
		PeerInfo:         make(map[uint64]*config.NodeInfo),
	}
}

// JoinNetwork contacts the join address specified and sends a join request. In return
// the contacted node should return the network wide configuration used which is then
// used to populate some configuration fields in this node's configuration
func (comm *CommunicationHandler) JoinNetwork(currentNodeInfo *config.NodeInfo, address string) (*config.Configuration, error) {
	grpcConn, client, err := comm.getClientWithConnection(address)
	if err != nil {
		return nil, err
	}
	ctx, cancel := comm.getContextWithCancelAndTimeout(2)
	defer cancel()
	defer grpcConn.Close()

	// Now send the join request, get the configuration. Then update the routing tables
	// if necessary and then return the configuration
	response, err := client.Join(ctx, &pb.JoinNetworkRequest{SenderNodeInfo: utils.GetProtoBufNodeInfo(currentNodeInfo)})
	if err != nil {
		return nil, err
	}
	kadNodeInfo := utils.GetKademliaNodeInfo(response.SenderNodeInfo)
	comm.updateContactNodeInfo(kadNodeInfo)
	return &config.Configuration{
		ReplicationFactor: response.ReplicationFactor,
		ConcurrencyFactor: response.ConcurrencyFactor,
	}, nil
}

// getClientWithConnection returns the client connection and the GRPC client. If there is any
// error while contacting it is reported to the caller.
func (comm *CommunicationHandler) getClientWithConnection(addr string) (*grpc.ClientConn, pb.KademliaProtocolClient, error) {
	grpcConn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, nil, err
	}
	client := pb.NewKademliaProtocolClient(grpcConn)
	return grpcConn, client, nil
}

// Returns the context with cancel function and timeout.
func (comm *CommunicationHandler) getContextWithCancelAndTimeout(timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), timeout*time.Second)
}

// updateContactNodeInfo updates the routing table and the mapping from nodeID to
// the corresponding node information (NodeID, IPAddress, Port)
func (comm *CommunicationHandler) updateContactNodeInfo(nodeInfo *config.NodeInfo) {
	comm.mutex.Lock()
	defer comm.mutex.Unlock()
	if addErr := comm.ContactNodeTable.AddNode(nodeInfo); addErr != nil {
		log.Printf("[WARN] Cannot add nodeID %d to the table. Reason=%s", nodeInfo.NodeID, addErr.Error())
	}
	comm.PeerInfo[nodeInfo.NodeID] = nodeInfo
}
