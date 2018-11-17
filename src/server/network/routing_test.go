package network_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	network "github.com/su225/godemlia/src/server/network"
)

var _ = Describe("RoutingTable", func() {
	var (
		rtbl network.RoutingTable

		Setting_ProximityThreshold uint64 = 0
		Setting_LeafSplitThreshold uint32 = 5
		Setting_PivotID            uint64 = 1024

		ContactToBeAdded = &network.NodeInfo{
			NodeID:    uint64(1025),
			IPAddress: "127.0.0.1",
			UDPPort:   12345,
		}
	)

	BeforeEach(func() {
		rtbl, _ = network.CreateTreeRoutingTable(
			Setting_PivotID,
			Setting_ProximityThreshold,
			Setting_LeafSplitThreshold,
		)
	})

	It("should add new item if it does not exist, but should return error otherwise", func() {
		addErr := rtbl.AddNode(ContactToBeAdded)
		Expect(addErr).To(BeNil())

		addAgainErr := rtbl.AddNode(ContactToBeAdded)
		Expect(addAgainErr).To(Equal(network.ErrorNodeAlreadyExists))
	})

	It("should remove the node if it exists or return an error", func() {
		if addErr := rtbl.AddNode(ContactToBeAdded); addErr == nil {
			removeErr := rtbl.RemoveNode(ContactToBeAdded.NodeID)
			Expect(removeErr).To(BeNil())

			removeAgainErr := rtbl.RemoveNode(ContactToBeAdded.NodeID)
			Expect(removeAgainErr).To(Equal(network.ErrorUnknownNode))

			removeUnrelatedErr := rtbl.RemoveNode(12345)
			Expect(removeUnrelatedErr).To(Equal(network.ErrorUnknownNode))
		} else {
			Fail("Error while populating nodeInfo to the table")
		}
	})
})
