package service

import (
	"context"
	"fmt"
	"log"

	pb "github.com/su225/godemlia/src/kademliapb"
	"github.com/su225/godemlia/src/server/config"
	utils "github.com/su225/godemlia/src/server/utils"
)

// KademliaMessagesHandler handles protocol and storage
// related incoming messages. It implements the interface
// specified by the generated Protobuf file
type KademliaMessagesHandler struct {
	nodeContext *NodeContext
}

// CreateKademliaMessagesHandler creates a new instance of protobuf message handler
func CreateKademliaMessagesHandler(context *NodeContext) *KademliaMessagesHandler {
	return &KademliaMessagesHandler{nodeContext: context}
}

// Join message from another node indicates the willingness of another node to join the network.
// In that case, send the network wide settings which can be used by the node.
func (h *KademliaMessagesHandler) Join(ctx context.Context, req *pb.JoinNetworkRequest) (*pb.JoinNetworkResponse, error) {
	log.Printf("[%d @ %s:%d] Received JOIN request", req.SenderNodeInfo.NodeId,
		req.SenderNodeInfo.NodeAddress, req.SenderNodeInfo.UdpPort)
	if addNodeErr := h.nodeContext.ContactNodeTable.AddNode(utils.GetKademliaNodeInfo(req.SenderNodeInfo)); addNodeErr != nil {
		log.Printf("[ERROR]: %s", addNodeErr.Error())
	}
	return &pb.JoinNetworkResponse{
		SenderNodeInfo:    utils.GetProtoBufNodeInfo(h.nodeContext.CurrentNodeInfo),
		ReplicationFactor: h.nodeContext.Config.ReplicationFactor,
		ConcurrencyFactor: h.nodeContext.Config.ConcurrencyFactor,
	}, nil
}

// Ping message from another node is a probe to test if this node is alive or not cut off from the network.
// Send the response indicating that this node is alive and kicking
func (h *KademliaMessagesHandler) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	log.Printf("[%d @ %s:%d] Received PING", req.SenderNodeInfo.NodeId,
		req.SenderNodeInfo.NodeAddress, req.SenderNodeInfo.UdpPort)
	h.tryUpdateContactTableOrLogError(utils.GetKademliaNodeInfo(req.SenderNodeInfo))
	return &pb.PingResponse{SenderNodeInfo: utils.GetProtoBufNodeInfo(h.nodeContext.CurrentNodeInfo)}, nil
}

// Store message is used to store a key-value pair in this node.
func (h *KademliaMessagesHandler) Store(ctx context.Context, req *pb.StoreRequest) (*pb.StoreResponse, error) {
	log.Printf("[%d @ %s:%d] Received STORE %d", req.SenderNodeInfo.NodeId,
		req.SenderNodeInfo.NodeAddress, req.SenderNodeInfo.UdpPort, req.Key)
	h.tryUpdateContactTableOrLogError(utils.GetKademliaNodeInfo(req.SenderNodeInfo))
	if err := h.nodeContext.NodeDataContext.Store.AddOrReplace(req.Key, req.Value); err != nil {
		return nil, fmt.Errorf("Cannot store key %d", req.Key)
	}
	log.Printf("[STORE] Stored key %d", req.Key)
	return &pb.StoreResponse{
		SenderNodeInfo: utils.GetProtoBufNodeInfo(h.nodeContext.CurrentNodeInfo),
		Key:            req.Key,
	}, nil
}

// FindNode message is used to find nodes closest to the given nodeID
func (h *KademliaMessagesHandler) FindNode(ctx context.Context, req *pb.FindNodeRequest) (*pb.FindNodeResponse, error) {
	log.Printf("[%d @ %s:%d] Received FIND_NODE %d", req.SenderNodeInfo.NodeId,
		req.SenderNodeInfo.NodeAddress, req.SenderNodeInfo.UdpPort, req.NodeId)
	h.tryUpdateContactTableOrLogError(utils.GetKademliaNodeInfo(req.SenderNodeInfo))
	closestNodes, closestQueryErr := h.nodeContext.ContactNodeTable.GetClosestNodes(req.NodeId, h.nodeContext.Config.ReplicationFactor)
	if closestQueryErr != nil {
		log.Fatalf("[ERROR] Cannot get closest nodes. Reason=%s", closestQueryErr.Error())
		return &pb.FindNodeResponse{
			SenderNodeInfo: utils.GetProtoBufNodeInfo(h.nodeContext.CurrentNodeInfo),
			Closest:        []*pb.NodeInfo{},
		}, nil
	}
	return &pb.FindNodeResponse{
		SenderNodeInfo: utils.GetProtoBufNodeInfo(h.nodeContext.CurrentNodeInfo),
		Closest:        utils.TransformToProtobufNodeInfo(closestNodes),
	}, nil
}

// FindValue message is used to find the value stored corresponding to the given key.
func (h *KademliaMessagesHandler) FindValue(ctx context.Context, req *pb.FindValueRequest) (*pb.FindValueResponse, error) {
	log.Printf("[%d @ %s:%d] Received FIND_VALUE %d", req.SenderNodeInfo.NodeId,
		req.SenderNodeInfo.NodeAddress, req.SenderNodeInfo.UdpPort, req.Key)
	h.tryUpdateContactTableOrLogError(utils.GetKademliaNodeInfo(req.SenderNodeInfo))
	if foundValue, findErr := h.nodeContext.NodeDataContext.Store.Get(req.Key); findErr != nil {
		log.Printf("Cannot find value for key %d", req.Key)
		return nil, findErr
	} else {
		return &pb.FindValueResponse{
			SenderNodeInfo: utils.GetProtoBufNodeInfo(h.nodeContext.CurrentNodeInfo),
			Result: &pb.FindValueResponse_Value{
				Value: &pb.KeyValueResponse{
					SenderNodeInfo: utils.GetProtoBufNodeInfo(h.nodeContext.CurrentNodeInfo),
					Key:            req.Key,
					Value:          foundValue,
				},
			},
		}, nil
	}
}

// tryUpdateContactTableOrLogError tries to update the routing table with the information of the node
// If it is not successful, then the error is logged. It is not fatal.
func (h *KademliaMessagesHandler) tryUpdateContactTableOrLogError(nodeInfo *config.NodeInfo) {
	if addNodeErr := h.nodeContext.ContactNodeTable.AddNode(nodeInfo); addNodeErr != nil {
		log.Printf("[ERROR]: %s", addNodeErr.Error())
	}
}
