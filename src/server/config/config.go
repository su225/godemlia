package config

// Configuration represents the configuration which is
// common to all the nodes in the given network.
//
// Replication factor: defines the number of replicas of a
// given key-value pair stored, number of nearest nodes returned.
//
// Concurreny factor: defines the number of neighboring nodes
// picked to locate the nearest node for an ID in keyspace
//
// IsBootstrap flag is true when the node is the first node
// in the network. Otherwise the join address must be specified.
type Configuration struct {
	ReplicationFactor uint32
	ConcurrencyFactor uint32
}

// NodeInfo contains the information required to contact
// a given node and measure the distance to any other node
// for a given closeness metric -XOR in Kademlia.
type NodeInfo struct {
	NodeID    uint64
	IPAddress string
	Port      uint32
}
