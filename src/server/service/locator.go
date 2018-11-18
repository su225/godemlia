package service

import (
	"fmt"
	"log"
	"sort"
	"sync"

	"github.com/su225/godemlia/src/server/config"
)

// ClosestNodeLocator locates the list of nodes closest
// to a given ID Why not simply get nodes from the routing
// table? It is because it may not be accurate
type ClosestNodeLocator struct {
	// NodeCtx provides access to various services needed by
	// ClosestNodeLocator like CommunicationHandler, routing table etc.
	NodeCtx *NodeContext
}

// LocateClosestNodes locates the closest nodes for a given key/nodeID.
func (cnl *ClosestNodeLocator) LocateClosestNodes(nodeID uint64) ([]*config.NodeInfo, error) {
	closestNodes, cnErr := cnl.NodeCtx.ContactNodeTable.GetClosestNodes(nodeID, cnl.NodeCtx.Config.ReplicationFactor)
	if cnErr != nil {
		log.Printf("[LOCATOR] Error while getting the initial list of closest nodes")
		return []*config.NodeInfo{}, nil
	}

	finalClosestNodeList := closestNodes
	discoveredNotVisitedNodes := make(map[uint64]*config.NodeInfo)
	visitedNodes := make(map[uint64]*config.NodeInfo)

	for _, closestNode := range closestNodes {
		discoveredNotVisitedNodes[closestNode.NodeID] = closestNode
	}

	candidateSet := finalClosestNodeList
	if uint32(len(candidateSet)) > cnl.NodeCtx.Config.ConcurrencyFactor {
		candidateSet = candidateSet[:cnl.NodeCtx.Config.ConcurrencyFactor]
	}
	minXORDistance := cnl.calculateMinXORDistance(nodeID, candidateSet)
	for {
		nextCandidateSet := make([]*config.NodeInfo, 0)
		var waitGroup sync.WaitGroup
		waitGroup.Add(len(candidateSet))

		for _, candidate := range candidateSet {
			candidateNodeID := candidate.NodeID
			candidateAddress := fmt.Sprintf("%s:%d", candidate.IPAddress, candidate.Port)

			// Send request to get the closest nodes to all candidate nodes in parallel
			// and wait till all of them respond to get better candidates (the ones with
			// distance less than the current minXORDistance)
			go func(cand *config.NodeInfo) {
				defer waitGroup.Done()
				log.Printf("[LOCATOR] Getting closest nodes from remote node [%d,%s]", candidateNodeID, candidateAddress)
				visitedNodes[candidateNodeID] = cand
				delete(discoveredNotVisitedNodes, candidateNodeID)
				remoteClosestNodes, remoteErr := cnl.NodeCtx.CommHandler.GetClosestNodes(candidateAddress, nodeID)
				if remoteErr != nil {
					log.Printf("[LOCATOR] Error while getting closest nodes from %s", candidateAddress)
					return
				}
				// Once the list of closest nodes is obtained from the remote node, check if that
				// node is already discovered. Node considered in previous round is not considered
				// again because in each iteration minXORDistance has to go down.
				for _, remoteClosest := range remoteClosestNodes {
					if _, isPresent := visitedNodes[remoteClosest.NodeID]; isPresent {
						continue
					}
					if _, isPresent := discoveredNotVisitedNodes[remoteClosest.NodeID]; isPresent {
						continue
					}
					// If the node returned by the remote node is closer than the minimum XOR distance
					// known so far then it would be a better candidate.
					candidateXORDistance := (remoteClosest.NodeID ^ nodeID)
					if candidateXORDistance < minXORDistance {
						discoveredNotVisitedNodes[remoteClosest.NodeID] = remoteClosest
						finalClosestNodeList = append(finalClosestNodeList, remoteClosest)
						nextCandidateSet = append(nextCandidateSet, remoteClosest)
						minXORDistance = candidateXORDistance
					}
				}
			}(candidate)
		}
		waitGroup.Wait()

		// No node closer than the nodes that are already known at this point
		// of time found. So there is no point iterating another time. So stop
		if len(nextCandidateSet) == 0 {
			break
		}

		// Otherwise, the next candidate set would be closer in XOR distance than
		// the current candidate set. So they should be candidates for the next round
		candidateSet = nextCandidateSet
	}

	// Sort the final list by XOR distance by ascending order and return the closest ones
	sort.Slice(finalClosestNodeList, func(i, j int) bool {
		xorDistOfI := (nodeID ^ finalClosestNodeList[i].NodeID)
		xorDistOfJ := (nodeID ^ finalClosestNodeList[j].NodeID)
		return xorDistOfI < xorDistOfJ
	})

	// Get only the required number of nodes.
	requiredClosestNodes := cnl.NodeCtx.Config.ReplicationFactor
	if uint32(len(finalClosestNodeList)) > requiredClosestNodes {
		finalClosestNodeList = finalClosestNodeList[:requiredClosestNodes]
	}

	return finalClosestNodeList, nil
}

func (cnl *ClosestNodeLocator) calculateMinXORDistance(key uint64, candidateSet []*config.NodeInfo) uint64 {
	minXORDistance := candidateSet[0].NodeID ^ key
	for _, candidate := range candidateSet {
		curXORDistance := candidate.NodeID ^ key
		if curXORDistance < minXORDistance {
			minXORDistance = curXORDistance
		}
	}
	return minXORDistance
}
