package service

import (
	"fmt"
	"log"
	"sync"

	"github.com/su225/godemlia/src/server/config"
	"github.com/su225/godemlia/src/server/network"
)

// RoutingTableHandler is an implementation of RoutingTable which
// in addition to routing table operations also does liveliness check
// of least recently contacted node to see if it can be evicted.
type RoutingTableHandler struct {
	mutex            sync.RWMutex
	CommHandler      *network.CommunicationHandler
	ContactNodeTable network.RoutingTable
}

// CreateRoutingTableHandler creates a routing table structure wrapped with additional
// functionality like liveliness probe before eviction (which requires communications
// handler) since it needs to contact other nodes.
func CreateRoutingTableHandler(comm *network.CommunicationHandler, routingTable network.RoutingTable) *RoutingTableHandler {
	return &RoutingTableHandler{
		mutex:            sync.RWMutex{},
		CommHandler:      comm,
		ContactNodeTable: routingTable,
	}
}

// AddNode adds a new node to the routing table. If the node already exists then
// its timestamp is updated. If the table is full, then attempt is made to evict
// one of least recently contacted nodes by first checking if they are alive. If
// they are, then addition of new node fails, else the new one is added in place of old.
func (rtbl *RoutingTableHandler) AddNode(node *config.NodeInfo) error {
	rtbl.mutex.Lock()
	defer rtbl.mutex.Unlock()
	if addErr := rtbl.ContactNodeTable.AddNode(node); addErr != nil {
		if fullErr, ok := addErr.(*network.TableIsFullError); ok {
			leastRecentlyContactedNode := fullErr.LeastRecentlyAccessedNodeInfo
			log.Printf("Table is full. Attempting to evacuate node [%d,%s,%d]",
				leastRecentlyContactedNode.NodeID,
				leastRecentlyContactedNode.IPAddress,
				leastRecentlyContactedNode.Port)
			leastRecentlyContactedNodeAddress := fmt.Sprintf("%s:%d",
				leastRecentlyContactedNode.IPAddress,
				leastRecentlyContactedNode.Port)
			if isAlive, _ := rtbl.CommHandler.Ping(leastRecentlyContactedNodeAddress); !isAlive {
				log.Printf("Evacuated node %s", leastRecentlyContactedNodeAddress)
				rtbl.ContactNodeTable.RemoveNode(leastRecentlyContactedNode.NodeID)
				rtbl.ContactNodeTable.AddNode(node)
				return nil
			}
			return network.ErrorTableIsFull
		} else if addErr == network.ErrorNodeAlreadyExists {
			// Remove the node and add it back again so that
			// the accessed timestamp is updated.
			rtbl.ContactNodeTable.RemoveNode(node.NodeID)
			rtbl.ContactNodeTable.AddNode(node)
			return nil
		} else {
			return addErr
		}
	}
	return nil
}

// RemoveNode just delegates it to the routing table data structure it contains
// but makes access thread-safe
func (rtbl *RoutingTableHandler) RemoveNode(nodeID uint64) error {
	rtbl.mutex.Lock()
	defer rtbl.mutex.Unlock()
	return rtbl.ContactNodeTable.RemoveNode(nodeID)
}

// GetClosestNodes does not add any new functionality except that it makes access
// to the tree-based Kademlia routing table data structure thread-safe.
func (rtbl *RoutingTableHandler) GetClosestNodes(nodeID uint64, k uint32) ([]*config.NodeInfo, error) {
	rtbl.mutex.RLock()
	defer rtbl.mutex.RUnlock()
	return rtbl.ContactNodeTable.GetClosestNodes(nodeID, k)
}
