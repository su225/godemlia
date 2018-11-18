package utils

import (
	pb "github.com/su225/godemlia/src/kademliapb"
	"github.com/su225/godemlia/src/server/config"
)

// GetProtoBufNodeInfo is a utility method to convert the representation of NodeInfo from
// protobuf generated format to the one used by Kademlia's internal data structures.
func GetProtoBufNodeInfo(nodeInfo *config.NodeInfo) *pb.NodeInfo {
	return &pb.NodeInfo{
		NodeId:      nodeInfo.NodeID,
		NodeAddress: nodeInfo.IPAddress,
		UdpPort:     nodeInfo.Port,
	}
}

// GetKademliaNodeInfo does the opposite of getProtoBufNodeInfo
func GetKademliaNodeInfo(nodeInfo *pb.NodeInfo) *config.NodeInfo {
	return &config.NodeInfo{
		NodeID:    nodeInfo.NodeId,
		IPAddress: nodeInfo.NodeAddress,
		Port:      nodeInfo.UdpPort,
	}
}

// TransformToProtobufNodeInfo transforms a list of node info in Kademlia format to
// the format of generated protobuf file.
func TransformToProtobufNodeInfo(nodes []*config.NodeInfo) []*pb.NodeInfo {
	protobufNodeInfo := make([]*pb.NodeInfo, len(nodes))
	for idx, node := range nodes {
		protobufNodeInfo[idx] = GetProtoBufNodeInfo(node)
	}
	return protobufNodeInfo
}
