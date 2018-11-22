package service

import (
	"fmt"
	"log"
	"time"

	"github.com/su225/godemlia/src/server/config"
	"github.com/su225/godemlia/src/server/network"
)

// RoutingTableRefresher is responsible for keeping routing table
// upto date by periodically asking other nodes what are the closest
// nodes to the current node ID
type RoutingTableRefresher struct {
	CommHandler      *network.CommunicationHandler
	ContactNodeTable network.RoutingTable
	RefreshPeriod    time.Duration
	Configuration    *config.Configuration
	Ticker           *time.Ticker
	CurrentNodeID    uint64
}

// CreateRoutingTableRefresher creates routing table refresher which
// periodically queries other nodes about the nodes closest to it.
func CreateRoutingTableRefresher(
	comm *network.CommunicationHandler,
	routingTable network.RoutingTable,
	refPeriod time.Duration,
	configuration *config.Configuration,
	currentNodeID uint64,
) *RoutingTableRefresher {
	return &RoutingTableRefresher{
		CommHandler:      comm,
		ContactNodeTable: routingTable,
		RefreshPeriod:    refPeriod,
		Configuration:    configuration,
		CurrentNodeID:    currentNodeID,
	}
}

// Start starts the ticker and runs the refresh algorithm at an interval
// specified by RefreshPeriod.
func (refresher *RoutingTableRefresher) Start() {
	refresher.Ticker = time.NewTicker(refresher.RefreshPeriod * time.Second)
	go func() {
		for range refresher.Ticker.C {
			refresher.RefreshTable()
		}
	}()
}

// RefreshTable queries closest nodes and gets the closest nodes corresponding to
// the nodeID of the current node and updates the table accordingly. This is useful
// when a new node joins in the neighborhood and this node doesn't know about it yet.
func (refresher *RoutingTableRefresher) RefreshTable() {
	closestNodes, err := refresher.ContactNodeTable.GetClosestNodes(refresher.CurrentNodeID, refresher.Configuration.ReplicationFactor+1)
	if err != nil {
		log.Printf("Cannot get the closest nodes for this node")
	}
	for _, node := range closestNodes {
		if node.NodeID == refresher.CurrentNodeID {
			continue
		}
		// Query the neighbors for the nodes closest to the current nodeID. It would be interesting if there are any
		// new nodeIDs.
		neighborAddress := fmt.Sprintf("%s:%d", node.IPAddress, node.Port)
		closestForNeighbors, closestQueryErr := refresher.CommHandler.GetClosestNodes(neighborAddress, refresher.CurrentNodeID)
		if closestQueryErr != nil {
			log.Printf("Error while getting closest nodes from [%d,%s,%d]. Removing it from the table",
				node.NodeID, node.IPAddress, node.Port)
			refresher.ContactNodeTable.RemoveNode(node.NodeID)
		}
		for _, neighborClosest := range closestForNeighbors {
			if addErr := refresher.ContactNodeTable.AddNode(neighborClosest); addErr != nil {
				log.Printf("Cannot add node [%d,%s,%d], ERROR=%s",
					neighborClosest.NodeID,
					neighborClosest.IPAddress,
					neighborClosest.Port,
					addErr.Error())
			} else {
				log.Printf("Added node [%d,%s,%d] to the table", neighborClosest.NodeID, neighborClosest.IPAddress, neighborClosest.Port)
			}
		}
	}
}
