package network

import (
	"context"
	"log"
	"time"

	pb "github.com/su225/godemlia/src/kademliapb"
	"github.com/su225/godemlia/src/server/config"
	"github.com/su225/godemlia/src/server/utils"
	"google.golang.org/grpc"
)

// CommunicationHandler is responsible for handling the routing table
// and contacting other nodes when necessary
type CommunicationHandler struct {
	ContactNodeTable RoutingTable
	CurNodeInfo      *config.NodeInfo
}

// CreateCommunicationHandler creates a communication handler which is responsible
// for maintaining routing table, contacting other nodes when necessary and also
// to refresh the routing table every now and then.
func CreateCommunicationHandler(routingTable RoutingTable, curNodeInfo *config.NodeInfo) *CommunicationHandler {
	return &CommunicationHandler{
		ContactNodeTable: routingTable,
		CurNodeInfo:      curNodeInfo,
	}
}

// JoinNetwork contacts the join address specified and sends a join request. In return
// the contacted node should return the network wide configuration used which is then
// used to populate some configuration fields in this node's configuration
func (comm *CommunicationHandler) JoinNetwork(address string) (*config.Configuration, error) {
	result, err := comm.sendKademliaProtocolMessage(address, 2*time.Second, func(ctx context.Context, client pb.KademliaProtocolClient) (interface{}, error) {
		// Now send the join request, get the configuration. Then update the routing tables
		// if necessary and then return the configuration
		response, err := client.Join(ctx, &pb.JoinNetworkRequest{SenderNodeInfo: utils.GetProtoBufNodeInfo(comm.CurNodeInfo)})
		if err != nil {
			return nil, err
		}
		kadNodeInfo := utils.GetKademliaNodeInfo(response.SenderNodeInfo)
		comm.updateContactNodeInfo(kadNodeInfo)
		return &config.Configuration{
			ReplicationFactor: response.ReplicationFactor,
			ConcurrencyFactor: response.ConcurrencyFactor,
		}, nil
	})
	if err != nil {
		return nil, err
	}
	if configuration, ok := result.(*config.Configuration); ok {
		return configuration, nil
	}
	return nil, err
}

// Ping probes if the other node is alive
func (comm *CommunicationHandler) Ping(address string) (bool, error) {
	result, err := comm.sendKademliaProtocolMessage(address, 2*time.Second,
		func(ctx context.Context, client pb.KademliaProtocolClient) (interface{}, error) {
			response, err := client.Ping(ctx, &pb.PingRequest{SenderNodeInfo: utils.GetProtoBufNodeInfo(comm.CurNodeInfo)})
			if err != nil {
				return false, err
			}
			kadNodeInfo := utils.GetKademliaNodeInfo(response.SenderNodeInfo)
			comm.updateContactNodeInfo(kadNodeInfo)
			return true, nil
		})
	if err != nil {
		return false, err
	}
	if boolResult, ok := result.(bool); ok {
		return boolResult, nil
	}
	return false, err
}

// GetClosestNodes sends request to fetch some closest nodes for a given nodeID/key.
func (comm *CommunicationHandler) GetClosestNodes(address string, nodeID uint64) ([]*config.NodeInfo, error) {
	result, err := comm.sendKademliaProtocolMessage(address, 2*time.Second,
		func(ctx context.Context, client pb.KademliaProtocolClient) (interface{}, error) {
			response, err := client.FindNode(ctx, &pb.FindNodeRequest{
				SenderNodeInfo: utils.GetProtoBufNodeInfo(comm.CurNodeInfo),
				NodeId:         nodeID,
			})
			if err != nil {
				return []*config.NodeInfo{}, err
			}
			kadNodeInfo := utils.GetKademliaNodeInfo(response.SenderNodeInfo)
			comm.updateContactNodeInfo(kadNodeInfo)
			return response.Closest, nil
		})
	if err != nil {
		return []*config.NodeInfo{}, err
	}
	if closestNodes, ok := result.([]*pb.NodeInfo); ok {
		return utils.TrasnformToKademliaNodeInfo(closestNodes), nil
	}
	return []*config.NodeInfo{}, err
}

// sendKademliaProtocolMessage opens a connection to the destination node and
// then performs some action, usually sending a message to a remote node, as
// specified by the function fn.
func (comm *CommunicationHandler) sendKademliaProtocolMessage(
	address string, timeout time.Duration,
	fn func(context.Context, pb.KademliaProtocolClient) (interface{}, error),
) (interface{}, error) {
	grpcConn, client, err := comm.getClientWithConnection(address)
	if err != nil {
		return false, err
	}
	ctx, cancel := comm.getContextWithCancelAndTimeout(2)
	defer cancel()
	defer grpcConn.Close()
	return fn(ctx, client)
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
	if addErr := comm.ContactNodeTable.AddNode(nodeInfo); addErr != nil {
		log.Printf("[WARN] Cannot add nodeID %d to the table. Reason=%s", nodeInfo.NodeID, addErr.Error())
	}
}
