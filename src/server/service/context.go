package service

import (
	"github.com/su225/godemlia/src/server/network"
)

// NodeContext provides access to various parts of
// the software stack of the node.
type NodeContext struct {
	// Config represents the network-wide configuration
	// like ReplicationFactor, ConcurrencyFactor etc.
	Config *network.Configuration

	// CurrentNodeInfo represents the contact info
	// of the current node like IP address, UDP port
	// and Node identifier (NodeID) in the network
	CurrentNodeInfo *network.NodeInfo

	// Represents the routing table used by this node
	// This contains the knowledge of the node about
	// other nodes in the network.
	NodeMembership network.RoutingTable
}
