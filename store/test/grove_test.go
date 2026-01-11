package store_test

import (
	"testing"

	"github.com/vixac/bullet/store/ram"
	"github.com/vixac/bullet/store/store_interface"
)

// groveStores contains all Grove implementations to test
// Add new implementations here as they're completed
var groveStores = map[string]store_interface.GroveStore{
	"ram": ram.NewRamStore(),
}

func TestGroveBasicOperations(t *testing.T) {
	for name, store := range groveStores {
		testGroveBasicOperations(store, name, t)
	}
}

func testGroveBasicOperations(store store_interface.GroveStore, name string, t *testing.T) {
	t.Run(name, func(t *testing.T) {
		space := store_interface.TenancySpace{AppId: 1, TenancyId: 1}

		// Test creating root node
		rootID := store_interface.NodeID("root")
		err := store.CreateNode(space, rootID, nil, nil, nil)
		if err != nil {
			t.Fatalf("Failed to create root node: %v", err)
		}

		// Test node exists
		exists, err := store.Exists(space, rootID)
		if err != nil {
			t.Fatalf("Failed to check existence: %v", err)
		}
		if !exists {
			t.Fatal("Root node should exist")
		}

		// Test get node info
		info, err := store.GetNodeInfo(space, rootID)
		if err != nil {
			t.Fatalf("Failed to get node info: %v", err)
		}
		if info.ID != rootID {
			t.Errorf("Expected ID %s, got %s", rootID, info.ID)
		}
		if info.Parent != nil {
			t.Error("Root node should have no parent")
		}
		if info.Depth != 0 {
			t.Errorf("Root node should have depth 0, got %d", info.Depth)
		}

		// Test creating child nodes
		child1ID := store_interface.NodeID("child1")
		err = store.CreateNode(space, child1ID, &rootID, nil, nil)
		if err != nil {
			t.Fatalf("Failed to create child1: %v", err)
		}

		child2ID := store_interface.NodeID("child2")
		err = store.CreateNode(space, child2ID, &rootID, nil, nil)
		if err != nil {
			t.Fatalf("Failed to create child2: %v", err)
		}

		// Verify child depth
		child1Info, err := store.GetNodeInfo(space, child1ID)
		if err != nil {
			t.Fatalf("Failed to get child1 info: %v", err)
		}
		if child1Info.Depth != 1 {
			t.Errorf("Child1 should have depth 1, got %d", child1Info.Depth)
		}

		// Test get children
		children, _, err := store.GetChildren(space, rootID, nil)
		if err != nil {
			t.Fatalf("Failed to get children: %v", err)
		}
		if len(children) != 2 {
			t.Errorf("Expected 2 children, got %d", len(children))
		}

		// Test get ancestors
		ancestors, _, err := store.GetAncestors(space, child1ID, nil)
		if err != nil {
			t.Fatalf("Failed to get ancestors: %v", err)
		}
		if len(ancestors) != 1 {
			t.Errorf("Expected 1 ancestor, got %d", len(ancestors))
		}
		if len(ancestors) > 0 && ancestors[0] != rootID {
			t.Errorf("Expected ancestor to be root, got %s", ancestors[0])
		}

		// Test delete node already exists error
		err = store.CreateNode(space, child1ID, &rootID, nil, nil)
		if err != store_interface.ErrNodeAlreadyExists {
			t.Errorf("Expected ErrNodeAlreadyExists, got %v", err)
		}
	})
}

func TestGroveDescendants(t *testing.T) {
	for name, store := range groveStores {
		testGroveDescendants(store, name, t)
	}
}

func testGroveDescendants(store store_interface.GroveStore, name string, t *testing.T) {
	t.Run(name, func(t *testing.T) {
		space := store_interface.TenancySpace{AppId: 2, TenancyId: 1}

		// Create tree:
		//     root
		//     /  \
		//    a    b
		//   / \
		//  c   d

		root := store_interface.NodeID("root2")
		a := store_interface.NodeID("a2")
		b := store_interface.NodeID("b2")
		c := store_interface.NodeID("c2")
		d := store_interface.NodeID("d2")

		store.CreateNode(space, root, nil, nil, nil)
		store.CreateNode(space, a, &root, nil, nil)
		store.CreateNode(space, b, &root, nil, nil)
		store.CreateNode(space, c, &a, nil, nil)
		store.CreateNode(space, d, &a, nil, nil)

		// Get all descendants of root
		descendants, _, err := store.GetDescendants(space, root, nil)
		if err != nil {
			t.Fatalf("Failed to get descendants: %v", err)
		}
		if len(descendants) != 4 {
			t.Errorf("Expected 4 descendants, got %d", len(descendants))
		}

		// Get descendants of 'a' with max depth 1
		maxDepth := 1
		opts := &store_interface.DescendantOptions{MaxDepth: &maxDepth}
		descendants, _, err = store.GetDescendants(space, a, opts)
		if err != nil {
			t.Fatalf("Failed to get descendants: %v", err)
		}
		if len(descendants) != 2 {
			t.Errorf("Expected 2 descendants (c, d), got %d", len(descendants))
		}

		// Verify relative depths
		for _, desc := range descendants {
			if desc.Depth != 1 {
				t.Errorf("Expected relative depth 1, got %d for node %s", desc.Depth, desc.NodeID)
			}
		}
	})
}

func TestGroveMoveNode(t *testing.T) {
	for name, store := range groveStores {
		testGroveMoveNode(store, name, t)
	}
}

func testGroveMoveNode(store store_interface.GroveStore, name string, t *testing.T) {
	t.Run(name, func(t *testing.T) {
		space := store_interface.TenancySpace{AppId: 3, TenancyId: 1}

		// Create tree:
		//     root
		//     /  \
		//    a    b
		//   /
		//  c

		root := store_interface.NodeID("root3")
		a := store_interface.NodeID("a3")
		b := store_interface.NodeID("b3")
		c := store_interface.NodeID("c3")

		store.CreateNode(space, root, nil, nil, nil)
		store.CreateNode(space, a, &root, nil, nil)
		store.CreateNode(space, b, &root, nil, nil)
		store.CreateNode(space, c, &a, nil, nil)

		// Verify initial structure
		cInfo, _ := store.GetNodeInfo(space, c)
		if *cInfo.Parent != a {
			t.Errorf("Expected c's parent to be a, got %s", *cInfo.Parent)
		}
		if cInfo.Depth != 2 {
			t.Errorf("Expected c's depth to be 2, got %d", cInfo.Depth)
		}

		// Move c from a to b
		err := store.MoveNode(space, c, &b, nil)
		if err != nil {
			t.Fatalf("Failed to move node: %v", err)
		}

		// Verify new structure
		cInfo, _ = store.GetNodeInfo(space, c)
		if *cInfo.Parent != b {
			t.Errorf("Expected c's parent to be b after move, got %s", *cInfo.Parent)
		}
		if cInfo.Depth != 2 {
			t.Errorf("Expected c's depth to still be 2, got %d", cInfo.Depth)
		}

		// Verify ancestors
		ancestors, _, _ := store.GetAncestors(space, c, nil)
		if len(ancestors) != 2 {
			t.Errorf("Expected 2 ancestors (b, root), got %d", len(ancestors))
		}

		// Test cycle detection: try to move b under c (should fail)
		err = store.MoveNode(space, b, &c, nil)
		if err != store_interface.ErrCycleDetected {
			t.Errorf("Expected ErrCycleDetected, got %v", err)
		}
	})
}

func TestGroveAggregates(t *testing.T) {
	for name, store := range groveStores {
		testGroveAggregates(store, name, t)
	}
}

func testGroveAggregates(store store_interface.GroveStore, name string, t *testing.T) {
	t.Run(name, func(t *testing.T) {
		space := store_interface.TenancySpace{AppId: 4, TenancyId: 1}

		// Create tree:
		//     root
		//     /  \
		//    a    b

		root := store_interface.NodeID("root4")
		a := store_interface.NodeID("a4")
		b := store_interface.NodeID("b4")

		store.CreateNode(space, root, nil, nil, nil)
		store.CreateNode(space, a, &root, nil, nil)
		store.CreateNode(space, b, &root, nil, nil)

		// Apply mutations
		mutation1 := store_interface.MutationID("m1")
		deltas1 := store_interface.AggregateDeltas{
			store_interface.AggregateKey("count"): 5,
			store_interface.AggregateKey("value"): 100,
		}
		err := store.ApplyAggregateMutation(space, mutation1, a, deltas1)
		if err != nil {
			t.Fatalf("Failed to apply mutation: %v", err)
		}

		mutation2 := store_interface.MutationID("m2")
		deltas2 := store_interface.AggregateDeltas{
			store_interface.AggregateKey("count"): 3,
			store_interface.AggregateKey("value"): 50,
		}
		err = store.ApplyAggregateMutation(space, mutation2, b, deltas2)
		if err != nil {
			t.Fatalf("Failed to apply mutation: %v", err)
		}

		// Test local aggregates
		localAgg, err := store.GetNodeLocalAggregates(space, a)
		if err != nil {
			t.Fatalf("Failed to get local aggregates: %v", err)
		}
		if localAgg[store_interface.AggregateKey("count")] != 5 {
			t.Errorf("Expected count=5, got %d", localAgg[store_interface.AggregateKey("count")])
		}

		// Test subtree aggregates
		subtreeAgg, err := store.GetNodeWithDescendantsAggregates(space, root)
		if err != nil {
			t.Fatalf("Failed to get subtree aggregates: %v", err)
		}
		if subtreeAgg[store_interface.AggregateKey("count")] != 8 {
			t.Errorf("Expected total count=8, got %d", subtreeAgg[store_interface.AggregateKey("count")])
		}
		if subtreeAgg[store_interface.AggregateKey("value")] != 150 {
			t.Errorf("Expected total value=150, got %d", subtreeAgg[store_interface.AggregateKey("value")])
		}

		// Test idempotency: applying same mutation should fail
		err = store.ApplyAggregateMutation(space, mutation1, a, deltas1)
		if err != store_interface.ErrMutationConflict {
			t.Errorf("Expected ErrMutationConflict, got %v", err)
		}
	})
}

func TestGroveSoftDelete(t *testing.T) {
	for name, store := range groveStores {
		testGroveSoftDelete(store, name, t)
	}
}

func testGroveSoftDelete(store store_interface.GroveStore, name string, t *testing.T) {
	t.Run(name, func(t *testing.T) {
		space := store_interface.TenancySpace{AppId: 5, TenancyId: 1}

		root := store_interface.NodeID("root5")
		child := store_interface.NodeID("child5")

		store.CreateNode(space, root, nil, nil, nil)
		store.CreateNode(space, child, &root, nil, nil)

		// Soft delete child
		err := store.DeleteNode(space, child, true)
		if err != nil {
			t.Fatalf("Failed to soft delete: %v", err)
		}

		// Node should not exist after soft delete
		exists, _ := store.Exists(space, child)
		if exists {
			t.Error("Soft deleted node should not exist")
		}

		// Note: RestoreNode is not yet enabled in the interface
	})
}

func TestGroveMultiTenancy(t *testing.T) {
	for name, store := range groveStores {
		testGroveMultiTenancy(store, name, t)
	}
}

func testGroveMultiTenancy(store store_interface.GroveStore, name string, t *testing.T) {
	t.Run(name, func(t *testing.T) {
		space1 := store_interface.TenancySpace{AppId: 6, TenancyId: 1}
		space2 := store_interface.TenancySpace{AppId: 6, TenancyId: 2}

		nodeID := store_interface.NodeID("node6")

		// Create same node ID in different tenancy spaces
		err := store.CreateNode(space1, nodeID, nil, nil, nil)
		if err != nil {
			t.Fatalf("Failed to create node in space1: %v", err)
		}

		err = store.CreateNode(space2, nodeID, nil, nil, nil)
		if err != nil {
			t.Fatalf("Failed to create node in space2: %v", err)
		}

		// Both should exist independently
		exists1, _ := store.Exists(space1, nodeID)
		exists2, _ := store.Exists(space2, nodeID)

		if !exists1 || !exists2 {
			t.Error("Nodes should exist in both tenancy spaces")
		}

		// Delete from space1 shouldn't affect space2
		store.DeleteNode(space1, nodeID, false)

		exists1, _ = store.Exists(space1, nodeID)
		exists2, _ = store.Exists(space2, nodeID)

		if exists1 {
			t.Error("Node should be deleted from space1")
		}
		if !exists2 {
			t.Error("Node should still exist in space2")
		}
	})
}
