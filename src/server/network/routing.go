package network

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/su225/godemlia/src/server/config"
)

// RoutingTable defines the nodes known to this node in
// the Kademlia network. This node can contact/route requests
// only to those nodes present in this table.
type RoutingTable interface {
	// AddNode attempts to add a node to the table. Add can
	// fail because the table is already full or because the
	// provided node information is incorrect or nil
	AddNode(node *config.NodeInfo) error

	// RemoveNode attempts to remove the node with the given ID.
	// It may fail because the node with the given ID is not
	// present in the table.
	RemoveNode(nodeID uint64) error

	// For a given nodeID returns at most k nodes which are
	// closest to the given nodeID ordered by closest to the
	// farthest node by closeness metric (XOR in this case)
	GetClosestNodes(nodeID uint64, k uint32) ([]*config.NodeInfo, error)
}

var (
	// ErrorTableIsFull is raised when the k-bucket is full and hence
	// node cannot be added to the routing table. This is to prevent
	// the routing table from growing too big.
	ErrorTableIsFull = errors.New("K-bucket is full")

	// ErrorNodeAlreadyExists is raised when adding a node which already
	// exists in the routing table. How equality is defined is left to
	// the implementation.
	ErrorNodeAlreadyExists = errors.New("Node with the given ID already exists")

	// ErrorUnknownNode is raised when a node is requested to be removed
	// from the routing table, but no node with such ID exists
	ErrorUnknownNode = errors.New("Unknown node error")
)

// TreeRoutingTableNode represents the node in the Kademlia routing table.
type TreeRoutingTableNode interface {
	// GetZeroPathNode returns the node in the path whose prefix is the prefix
	// of the current path with 0 appended to it
	GetZeroPathNode() TreeRoutingTableNode

	// GetOnePathNode returns the node in the path whose prefix is the prefix
	// of the current path with 1 appended to it
	GetOnePathNode() TreeRoutingTableNode

	// Return the number of nodes known to this node with the prefix defined
	// by the path from the root to the current node.
	GetSubtreeSize() uint32
}

// nodeInfoWithRecentInfo represents the information of the node along with
// the local timestamp of last contact.
type nodeInfoWithRecentInfo struct {
	Info     *config.NodeInfo
	LastSeen uint64
}

// LeafRoutingTableNode implements TreeRoutingTableNode interface. It represents
// the leaf node in the tree containing information about the nodes it knows. This
// represents the k-Bucket abstraction in the paper.
type LeafRoutingTableNode struct {
	// ContactNodes is the list of all nodes known to this node.
	ContactNodes []*nodeInfoWithRecentInfo
}

// CreateLeafRoutingTableNode creates a new instance of the leaf node with an
// empty slice of contact list and returns it.
func CreateLeafRoutingTableNode() *LeafRoutingTableNode {
	return &LeafRoutingTableNode{ContactNodes: make([]*nodeInfoWithRecentInfo, 0)}
}

func createLeafRoutingTableNode(contacts []*nodeInfoWithRecentInfo) *LeafRoutingTableNode {
	return &LeafRoutingTableNode{ContactNodes: contacts}
}

// GetZeroPathNode for LeafRoutingTableNode is always nil
func (n *LeafRoutingTableNode) GetZeroPathNode() TreeRoutingTableNode {
	return nil
}

// GetOnePathNode for LeafRoutingTableNode is always nil
func (n *LeafRoutingTableNode) GetOnePathNode() TreeRoutingTableNode {
	return nil
}

// GetSubtreeSize for LeafRoutingTableNode is the number of nodes
// known to this node within some node ID range (k-bucket)
func (n *LeafRoutingTableNode) GetSubtreeSize() uint32 {
	return uint32(len(n.ContactNodes))
}

// ContainsContactWithID returns true, if there is a contact with the given ID
// or false otherwise
func (n *LeafRoutingTableNode) ContainsContactWithID(nodeID uint64) bool {
	for _, nodeInfo := range n.ContactNodes {
		if nodeInfo.Info.NodeID == nodeID {
			return true
		}
	}
	return false
}

// NonLeafRoutingTableNode implements TreeRoutingTableNode interface. It represents
// the non-leaf node. This must maintain the invariant that both ZeroPathNode and
// OnePathNode should not be nil and that the size of the subtree must be more than
// some specified lower limit. Otherwise this should be merged to create a leaf.
type NonLeafRoutingTableNode struct {
	// Node whose prefix is the prefix of this node appended with 0
	ZeroPathNode TreeRoutingTableNode

	// Node whose prefix is the prefix of this node appended with 1
	OnePathNode TreeRoutingTableNode
}

// CreateNonLeafRoutingTableNode creates a new non leaf node with given zeroPath and onePath nodes.
// Both of them should be not be nil. If both of them should be nil,  then user LeafRoutingTableNode.
func CreateNonLeafRoutingTableNode(zeroPathNode TreeRoutingTableNode, onePathNode TreeRoutingTableNode) *NonLeafRoutingTableNode {
	if zeroPathNode == nil || onePathNode == nil {
		return nil
	}
	return &NonLeafRoutingTableNode{zeroPathNode, onePathNode}
}

// GetZeroPathNode for NonLeafRoutingTableNode returns the node whose path prefix
// is the prefix of this node with 0 appended to the end (binary of nodeID)
func (n *NonLeafRoutingTableNode) GetZeroPathNode() TreeRoutingTableNode {
	return n.ZeroPathNode
}

// GetOnePathNode for NonLeafRoutingTableNode returns the node whose path prefix
// is the prefix of this node with 1 appended to the end (binary of nodeID)
func (n *NonLeafRoutingTableNode) GetOnePathNode() TreeRoutingTableNode {
	return n.OnePathNode
}

// GetSubtreeSize returns the number of nodes known to this node whose ID (in binary)
// have the prefix represented by the path to this node.
func (n *NonLeafRoutingTableNode) GetSubtreeSize() uint32 {
	return n.ZeroPathNode.GetSubtreeSize() + n.OnePathNode.GetSubtreeSize()
}

// TreeRoutingTable is the routing table implementation described in the
// Kademlia paper. Here nodes are dynamically allocated and split when there
// are too many nodes in a bucket. The splitting threshold and proximity
// threshold are specified.
type TreeRoutingTable struct {
	rootNode           TreeRoutingTableNode
	pivotLocation      uint64
	proximityThreshold uint64
	leafSplitThreshold uint32
	addressSize        uint
	mutex              sync.RWMutex
}

// TableIsFullError is returned when nodes can no longer be split
// for the given NodeID and there is no space left in the table to insert
type TableIsFullError struct {
	NodeID                        uint64
	LastAccessedTimestamp         uint64
	LeastRecentlyAccessedNodeInfo *config.NodeInfo
}

func (terr TableIsFullError) Error() string {
	return fmt.Sprintf("Insertion of %d failed. Candidate for removal = %d",
		terr.NodeID, terr.LeastRecentlyAccessedNodeInfo.Port)
}

var (
	// ErrorLeafMaxSizeIsZero is returned when the bucket size is specified as 0
	// It must be always greater than 0
	ErrorLeafMaxSizeIsZero = errors.New("Leaf max-size must be greater than 0")
)

// CreateTreeRoutingTable creates a new instance of the kademlia routing table. The parameters
// pivot - represents the current ID of the node from which XOR distance is measured
// proximity - represents the distance below which splitting is done indefinitely without
//             returning ErrorTableIsFull (accomodate any number of nodes within that distance)
// leafMaxSize - represents the maximum size of the leaf node. That is, the maximum number of
//               node contact information that can be stored before splitting.
// addressSize - number of bits in the nodeID and key. Represents the size of the keyspace.
func CreateTreeRoutingTable(pivot uint64, proximity uint64, leafMaxSize uint32) (*TreeRoutingTable, error) {
	if leafMaxSize <= 0 {
		return nil, ErrorLeafMaxSizeIsZero
	}
	return &TreeRoutingTable{
		rootNode:           CreateLeafRoutingTableNode(),
		pivotLocation:      pivot,
		proximityThreshold: proximity,
		leafSplitThreshold: leafMaxSize,
		addressSize:        64,
		mutex:              sync.RWMutex{},
	}, nil
}

// AddNode of TreeRoutingTable adds a new node to the routing table if it doesn't exist
// and there is more space in the table for addition. If the node with the given ID exists
// then ErrorNodeAlreadyExists is returned. If there is no space to add the node
// contact information then ErrorTableIsFull
func (rtbl *TreeRoutingTable) AddNode(node *config.NodeInfo) error {
	rtbl.mutex.Lock()
	defer rtbl.mutex.Unlock()
	if node == nil {
		return nil
	}
	return rtbl.doAddNode(rtbl.addressSize-1, nil, rtbl.rootNode, node)
}

// RemoveNode of TreeRoutingTable removes the given contact info from the routing table. If the
// node does not exist in the table then ErrorUnknownNode is thrown
func (rtbl *TreeRoutingTable) RemoveNode(nodeID uint64) error {
	rtbl.mutex.Lock()
	defer rtbl.mutex.Unlock()
	return rtbl.doRemoveNode(rtbl.addressSize-1, nil, rtbl.rootNode, nodeID)
}

// GetClosestNodes of TreeRoutingTable returns the k closest nodes for a given nodeID.
func (rtbl *TreeRoutingTable) GetClosestNodes(nodeID uint64, k uint32) ([]*config.NodeInfo, error) {
	rtbl.mutex.RLock()
	defer rtbl.mutex.RUnlock()
	if k == 0 {
		return []*config.NodeInfo{}, nil
	}
	return rtbl.doGatherClosestNodes(rtbl.rootNode, rtbl.addressSize-1, nodeID, k), nil
}

// doAddNode is responsible for actually adding the node if applicable or returning appropriate error
func (rtbl *TreeRoutingTable) doAddNode(bitIndex uint, prevNode TreeRoutingTableNode, curNode TreeRoutingTableNode, nodeInfo *config.NodeInfo) error {
	if curNode == nil {
		return nil
	}
	switch node := curNode.(type) {
	case *LeafRoutingTableNode:
		// First check if the node with given ID exists. If it does then
		// return ErrorNodeAlreadyExists.
		if node.ContainsContactWithID(nodeInfo.NodeID) {
			return ErrorNodeAlreadyExists
		}

		// If there are still slots left in the table to insert contact info,
		// then just insert it to the end of the list. The list must maintain
		// the invariant that the head of the list is the least recently accessed
		// and tail is the most recently accessed node.
		if node.GetSubtreeSize() < rtbl.leafSplitThreshold {
			nodeWithTimestamp := nodeInfoWithRecentInfo{Info: nodeInfo, LastSeen: uint64(time.Now().UnixNano())}
			node.ContactNodes = append(node.ContactNodes, &nodeWithTimestamp)
		} else if node.ContainsContactWithID(rtbl.pivotLocation) || rtbl.isWithinProximityBound(nodeInfo.NodeID) {
			// Split the leaf node into two nodes when there are no more slots left
			// and the pivot node is in this bucket or the distance of the nodeID from
			// the pivot ID is sufficiently close (specified by proximityThreshold)
			nonLeafNode := rtbl.splitLeafNode(node, bitIndex)
			if prevNode == nil {
				rtbl.rootNode = nonLeafNode
			} else if parent, ok := prevNode.(*NonLeafRoutingTableNode); ok {
				if parent.GetZeroPathNode() == curNode {
					parent.ZeroPathNode = nonLeafNode
				} else {
					parent.OnePathNode = nonLeafNode
				}
			}
			// Once the node is split, try to reinsert the contact information
			// into the leaves covering narrow range (contains fewer contacts and
			// hence will have some slots left)
			return rtbl.doAddNode(bitIndex, prevNode, nonLeafNode, nodeInfo)
		} else {
			// If the table is full, then return TableIsFullError along with the Node information
			// of the least recently contacted node so that it might be potentially evicted and
			// a slot can be freed. Also len(node.ContactNodes) > 0 should always be true, since it
			// doesn't make sense to have leafSplitThreshold as 0.
			tableIsFullError := &TableIsFullError{NodeID: nodeInfo.NodeID}
			if len(node.ContactNodes) > 0 {
				tableIsFullError.LastAccessedTimestamp = node.ContactNodes[0].LastSeen
				tableIsFullError.LeastRecentlyAccessedNodeInfo = node.ContactNodes[0].Info
			}
			return tableIsFullError
		}

	case *NonLeafRoutingTableNode:
		if (nodeInfo.NodeID & (1 << bitIndex)) == 0 {
			return rtbl.doAddNode(bitIndex-1, node, node.GetZeroPathNode(), nodeInfo)
		}
		return rtbl.doAddNode(bitIndex-1, node, node.GetOnePathNode(), nodeInfo)
	}
	return nil
}

// doRemoveNode does the real work of removing a contact node from the routing table. (node here is not TableNode)
func (rtbl *TreeRoutingTable) doRemoveNode(bitIndex uint, prevNode TreeRoutingTableNode, curNode TreeRoutingTableNode, nodeID uint64) error {
	if curNode == nil {
		return nil
	}
	switch node := curNode.(type) {
	case *LeafRoutingTableNode:
		indexOfNodeToBeRemoved := -1
		for contactIndex, contactNodeInfo := range node.ContactNodes {
			if contactNodeInfo.Info.NodeID == nodeID {
				indexOfNodeToBeRemoved = contactIndex
				break
			}
		}
		// If the node to be removed is not found, then return ErrorUnknownNode
		if indexOfNodeToBeRemoved == -1 {
			return ErrorUnknownNode
		}
		// If found, then remove it from the list
		if indexOfNodeToBeRemoved == len(node.ContactNodes) {
			node.ContactNodes = node.ContactNodes[:indexOfNodeToBeRemoved]
		} else if indexOfNodeToBeRemoved == 0 {
			node.ContactNodes = node.ContactNodes[1:]
		} else {
			node.ContactNodes = append(node.ContactNodes[:indexOfNodeToBeRemoved], node.ContactNodes[indexOfNodeToBeRemoved+1:]...)
		}

	case *NonLeafRoutingTableNode:
		if (nodeID & (1 << bitIndex)) == 0 {
			return rtbl.doRemoveNode(bitIndex-1, node, node.GetZeroPathNode(), nodeID)
		}
		return rtbl.doRemoveNode(bitIndex-1, node, node.GetOnePathNode(), nodeID)
	}
	return nil
}

// Get the nodes closest to the given nodeID.
func (rtbl *TreeRoutingTable) doGatherClosestNodes(curNode TreeRoutingTableNode, bitIndex uint, nodeID uint64, requiredCount uint32) []*config.NodeInfo {
	if curNode == nil {
		return []*config.NodeInfo{}
	}
	nodeList := make([]*config.NodeInfo, 0)
	switch node := curNode.(type) {
	case *LeafRoutingTableNode:
		// If it is a leaf node then gather all the nodes and get the
		// required number of nodes in sorted order (closest to farthest)
		for _, contactNodeInfo := range node.ContactNodes {
			nodeList = append(nodeList, contactNodeInfo.Info)
		}

	case *NonLeafRoutingTableNode:
		// If the size of the subtree rooted at this node is less than
		// the number of required nodes then return all of them.
		primaryPathNodeList := make([]*config.NodeInfo, 0)
		auxillaryPathNodeList := make([]*config.NodeInfo, 0)
		isOnePath := (nodeID & (1 << bitIndex)) > 0
		if isOnePath {
			primaryPathNodeList = rtbl.doGatherClosestNodes(node.GetOnePathNode(), bitIndex-1, nodeID, requiredCount)
		} else {
			primaryPathNodeList = rtbl.doGatherClosestNodes(node.GetZeroPathNode(), bitIndex-1, nodeID, requiredCount)
		}
		// If the subtree closer to the nodeID has required amount of nodes or
		// more then pick contact nodes from that subtree. Othewise visit
		// the other subtree to gather enough nodes.
		if uint32(len(primaryPathNodeList)) < requiredCount {
			primaryListSize := uint32(len(primaryPathNodeList))
			if isOnePath {
				auxillaryPathNodeList = rtbl.doGatherClosestNodes(node.GetZeroPathNode(), bitIndex-1, nodeID, requiredCount-primaryListSize)
			} else {
				auxillaryPathNodeList = rtbl.doGatherClosestNodes(node.GetOnePathNode(), bitIndex-1, nodeID, requiredCount-primaryListSize)
			}
		}
		nodeList = append(primaryPathNodeList, auxillaryPathNodeList...)

	}
	// Finally sort the contact nodes by their proximity to the given NodeID.
	sort.Slice(nodeList, func(i, j int) bool {
		xorDistI := (rtbl.pivotLocation ^ nodeList[i].NodeID)
		xorDistJ := (rtbl.pivotLocation ^ nodeList[j].NodeID)
		return xorDistI < xorDistJ
	})

	// If the list of nodes retrieved is more than the requiredCount then
	// get only the portion of the list required. Otherwise return the entire list.
	if requiredCount < uint32(len(nodeList)) {
		nodeList = nodeList[:requiredCount]
	}
	return nodeList
}

// isWithinProximityBound returns true if the nodeID is within the distance (measured
// by XOR metric) specified by proximityThreshold.
func (rtbl *TreeRoutingTable) isWithinProximityBound(nodeID uint64) bool {
	return (nodeID ^ rtbl.pivotLocation) <= rtbl.proximityThreshold
}

// Split the leaf node, into a non-leaf node with two children each covering smaller ranges. The
// split is carried out at the given bitIndex. The contacts are separated into two lists - those
// which have the bit specified by bitIndex as 0 and another one for 1.
func (rtbl *TreeRoutingTable) splitLeafNode(node *LeafRoutingTableNode, bitIndex uint) *NonLeafRoutingTableNode {
	zeroSide, oneSide := make([]*nodeInfoWithRecentInfo, 0), make([]*nodeInfoWithRecentInfo, 0)
	for _, contactNodeInfo := range node.ContactNodes {
		contactNodeID := contactNodeInfo.Info.NodeID
		if (contactNodeID & (1 << bitIndex)) == 0 {
			zeroSide = append(zeroSide, contactNodeInfo)
		} else {
			oneSide = append(oneSide, contactNodeInfo)
		}
	}
	return &NonLeafRoutingTableNode{
		ZeroPathNode: createLeafRoutingTableNode(zeroSide),
		OnePathNode:  createLeafRoutingTableNode(oneSide),
	}
}
