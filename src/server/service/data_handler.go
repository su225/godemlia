package service

import (
	"fmt"
	"log"
	"time"

	"github.com/su225/godemlia/src/server/config"
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
	nodeDataContext := &NodeDataContext{
		NodeCtx:       nodeContext,
		Store:         CreateDataStore(),
		DataStorer:    &DataStorageHandler{nodeContext},
		DataRetriever: &DataRetrievalHandler{nodeContext},
	}
	// Initialize data republish handler to republish the data for specified interval in time
	// For now it is hardcoded to 10 seconds. Typically it is should be around an hour. It is
	// ideal to push this value to config.Configuration.
	nodeDataContext.DataRepublisher = CreateDataRepublishHandler(nodeContext, nodeDataContext, nodeContext.Config.ReplicationFactor+1, 10*time.Second)
	// Initialize the garbage collector. Garbage collection interval must be greater than data republishing interval so that
	// data is not declared stale early. Eventually, they will be configurable. For now, they are hardcoded.
	nodeDataContext.GarbageCollector = CreateStaleDataHandler(nodeDataContext, 10*time.Second, 20*time.Second)
	return nodeDataContext
}

// Start starts data republisher and garbage collector
func (dctx *NodeDataContext) Start() {
	go dctx.DataRepublisher.Start()
	go dctx.GarbageCollector.Start()
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
	*NodeContext
	*NodeDataContext
	// NeighborCount describes the number of neighbors to republish to
	// It is typically equal to the replication factor
	NeighborCount uint32
	// RepublishInterval describes the period of running
	// republish job.
	RepublishInterval time.Duration
	// RepublishTicker is the channel which receives signal periodically
	// to republish the data. Channel should be closed to stop that
	RepublishTicker *time.Ticker
}

// CreateDataRepublishHandler creates a new instance of data republisher. It needs the
// number of neighbors to republish for.
func CreateDataRepublishHandler(ctx *NodeContext, dctx *NodeDataContext, neighbors uint32, duration time.Duration) *DataRepublishHandler {
	return &DataRepublishHandler{
		NodeContext:       ctx,
		NodeDataContext:   dctx,
		NeighborCount:     neighbors,
		RepublishInterval: duration,
	}
}

// Start starts the data republishing service
func (dp *DataRepublishHandler) Start() {
	dp.RepublishTicker = time.NewTicker(dp.RepublishInterval)
	log.Printf("[REPUBLISHER] Starting republisher...")
	go func() {
		for range dp.RepublishTicker.C {
			dp.RepublishData()
		}
	}()
}

// RepublishData republishes data to the neighbor node if and only if this node is
// one of the top 3 closest nodes to that key and the neighbor is also among closest.
func (dp *DataRepublishHandler) RepublishData() {
	currentNodeID := dp.NodeContext.CurrentNodeInfo.NodeID
	closestNodes, err := dp.NodeContext.ContactNodeTable.GetClosestNodes(currentNodeID, dp.NeighborCount+1)
	if err != nil {
		log.Printf("[REPUBLISHER] Error: Unable to find the closest nodes. Reason=%s", err.Error())
		return
	}
	keysToRepublish := dp.NodeDataContext.Store.GetAllKeys()
	for _, closestNode := range closestNodes {
		go dp.publishToNode(dp.NodeContext.CurrentNodeInfo, closestNode, keysToRepublish)
	}
}

// publishToNode republishes selected keys to a particular node
func (dp *DataRepublishHandler) publishToNode(currentNodeInfo *config.NodeInfo, nodeInfo *config.NodeInfo, keysToRepublish []uint64) {
	nodeAddress := fmt.Sprintf("%s:%d", nodeInfo.IPAddress, nodeInfo.Port)
	for _, k := range keysToRepublish {
		closestNodesToKey, err := dp.NodeContext.ContactNodeTable.GetClosestNodes(k, dp.NeighborCount)
		if err != nil {
			log.Printf("[REPUBLISHER] Error: Cannot get closest nodes for key %d. Reason=%s", k, err.Error())
			continue
		}
		if dp.isPresentWithin(currentNodeInfo.NodeID, closestNodesToKey, 3) && dp.isPresentWithin(nodeInfo.NodeID, closestNodesToKey, len(closestNodesToKey)+1) {
			if value, err := dp.NodeDataContext.Store.Get(k); err != nil {
				log.Printf("[REPUBLISHER] Cannot get the value for key %d. Reason=%s", k, err.Error())
				continue
			} else {
				if storedKey, err := dp.NodeContext.CommHandler.Store(nodeAddress, k, value); err != nil {
					log.Printf("[REPUBLISHER] Failed to refresh value for key %d. Reason=%s", k, err.Error())
				} else {
					log.Printf("[REPUBLISHER] Successfully republished key %d to %s", storedKey, nodeAddress)
				}
			}
		}
	}
}

func (dp *DataRepublishHandler) isPresentWithin(requiredNodeID uint64, closestNodes []*config.NodeInfo, rank int) bool {
	for i, node := range closestNodes {
		if i >= rank {
			return false
		}
		if node.NodeID == requiredNodeID {
			return true
		}
	}
	return false
}

// StaleDataHandler is responsible for removing the keys which are not
// being refreshed for a while.
type StaleDataHandler struct {
	*NodeDataContext
	// GCTimeout specifies the period between two invocations
	// of stale data removal
	GCTimeout       time.Duration
	GCTimeoutTicker *time.Ticker
	// DataTimeout specifies the time period after
	// which the data is considered stale
	DataTimeout time.Duration
}

// CreateStaleDataHandler creates a new stale data handler
func CreateStaleDataHandler(dctx *NodeDataContext, gct time.Duration, dt time.Duration) *StaleDataHandler {
	return &StaleDataHandler{
		NodeDataContext: dctx,
		GCTimeout:       gct,
		DataTimeout:     dt,
	}
}

// Start starts the data republishing service
func (gc *StaleDataHandler) Start() {
	gc.GCTimeoutTicker = time.NewTicker(gc.GCTimeout)
	log.Printf("[GC] Starting Stale Data handler...")
	go func() {
		for range gc.GCTimeoutTicker.C {
			gc.RemoveStaleData()
		}
	}()
}

// RemoveStaleData removes cleans up stale data if any
func (gc *StaleDataHandler) RemoveStaleData() {
	for _, k := range gc.NodeDataContext.Store.GetAllKeys() {
		if lastRefreshTime, err := gc.NodeDataContext.Store.GetRefreshTime(k); err != nil {
			log.Printf("[GC] Cannot get last refreshed info for %d. Reason=%s", k, err.Error())
			continue
		} else {
			interval := time.Now().UnixNano() - lastRefreshTime
			if interval > gc.DataTimeout.Nanoseconds() {
				log.Printf("[GC] Remove key %d from the store as it is stale", k)
				gc.NodeDataContext.Store.Remove(k)
			}
		}
	}
}
