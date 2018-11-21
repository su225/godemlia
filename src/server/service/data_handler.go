package service

import (
	"fmt"
	"log"
)

// NodeDataContext handles data related functionality like retrieving, storing
// peridoically republishing and garbaging collecting stale data
type NodeDataContext struct {
	// NodeCtx to access other components
	// running in this node.
	NodeCtx *NodeContext

	// DataStore is the place where data
	// is actually stored.
	Store *DataStore

	// DataStorer is responsible for finding
	// and issuing STORE instruction to appropriate nodes
	DataStorer *DataStorageHandler

	// DataRetriever is responsible for retrieving
	// the value for a given key-value pair. It implements
	// FIND_VALUE instruction in Kademlia protocol
	DataRetriever *DataRetrievalHandler

	// DataRepublisher is responsible for periodically
	// republishing or moving data to newly joined nodes
	// to keep it up to date.
	DataRepublisher *DataRepublishHandler

	// GarbageCollector is responsible for GCing the data
	GarbageCollector *StaleDataHandler
}

// CreateNodeDataContext creates a new NodeDataContext which is responsible
// for handling all data related functionality of the node - like storing,
// retrieving, republishing and garbage collection.
func CreateNodeDataContext(nodeContext *NodeContext) *NodeDataContext {
	return &NodeDataContext{
		NodeCtx:          nodeContext,
		Store:            CreateDataStore(),
		DataStorer:       &DataStorageHandler{nodeContext},
		DataRetriever:    &DataRetrievalHandler{nodeContext},
		DataRepublisher:  &DataRepublishHandler{},
		GarbageCollector: &StaleDataHandler{},
	}
}

// DataStorageHandler is responsible for locating the
// right nodes and issuing STORE instruction to store the
// key-value pair.
type DataStorageHandler struct {
	*NodeContext
}

// StoreKVPair retrieves the appropriate nodes to store the key
// (K-closest nodes to the key) and issues STORE instruction to them.
func (ds *DataStorageHandler) StoreKVPair(key uint64, value []byte) error {
	closestNodes, locatorErr := ds.NodeContext.Locator.LocateClosestNodes(key)
	if locatorErr != nil {
		log.Printf("[DATA-STORAGE] Error while getting nodes to store %d", key)
		return locatorErr
	}
	numReplicas := 0
	for _, closestNode := range closestNodes {
		closestNodeAddress := fmt.Sprintf("%s:%d", closestNode.IPAddress, closestNode.Port)
		log.Printf("[DATA-STORAGE] Issuing STORE to node %s for key %d", closestNodeAddress, key)
		if storedKey, storeErr := ds.NodeContext.CommHandler.Store(closestNodeAddress, key, value); storeErr != nil || storedKey != key {
			log.Printf("[DATA-STORAGE] Error while storing key %d in node %d", key, closestNode.NodeID)
			continue
		}
		numReplicas++
	}
	if numReplicas == 0 {
		return fmt.Errorf("Failed to store key %d in the cluster", key)
	}
	log.Printf("[DATA-STORAGE] Key %d stored in %d replica(s)", key, numReplicas)
	return nil
}

// DataRetrievalHandler is responsible for retrieving data
// for a given key contacting appropriate nodes.
type DataRetrievalHandler struct {
	*NodeContext
}

// RetrieveKVPair is responsible for returning the key value pair if it
// is found in the cluster. Otherwise it returns an error complaining
// that the data is not found.
func (dr *DataRetrievalHandler) RetrieveKVPair(key uint64) ([]byte, error) {
	if candidateNodes, err := dr.NodeContext.Locator.LocateClosestNodes(key); err != nil {
		log.Printf("Unable to locate candidate nodes for key %d", key)
		return nil, err
	} else {
		for _, candidate := range candidateNodes {
			candidateAddress := fmt.Sprintf("%s:%d", candidate.IPAddress, candidate.Port)
			if foundVal, err := dr.NodeContext.CommHandler.FindValue(candidateAddress, key); err != nil {
				log.Printf("Cannot find value for %d in %s. Reason=%s", key, candidateAddress, err.Error())
			} else {
				return foundVal, nil
			}
		}
		return []byte{}, fmt.Errorf("Value not found for %d", key)
	}
}

// DataRepublishHandler is responsible for transferring data to
// newly joined nodes or periodically republish data to keep it fresh
type DataRepublishHandler struct {
}

// StaleDataHandler is responsible for removing the keys which are not
// being refreshed for a while.
type StaleDataHandler struct {
}
