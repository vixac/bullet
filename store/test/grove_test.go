package store_test

import (
	"os"
	"testing"

	"github.com/vixac/bullet/store/boltdb"
	"github.com/vixac/bullet/store/ram"
	sqlite_store "github.com/vixac/bullet/store/sqlite"
	"github.com/vixac/bullet/store/store_interface"
)

// groveStores contains all Grove implementations to test
// Add new implementations here as they're completed
var groveStores = map[string]store_interface.GroveStore{
	"ram": ram.NewRamStore(),
}

func init() {
	// Create SQLite store for testing
	sqliteStore, err := sqlite_store.NewSQLiteStore(":memory:")
	if err != nil {
		panic(err)
	}
	groveStores["sqlite"] = sqliteStore

	// Create BoltDB store for testing
	boltStore, err := boltdb.NewBoltStore("test-grove.db")
	if err != nil {
		panic(err)
	}
	groveStores["boltdb"] = boltStore
}

func TestMain(m *testing.M) {
	code := m.Run()
	// Clean up test databases
	os.Remove("test-grove.db")
	os.Remove("test-track.db")
	os.Exit(code)
}

func TestGroveBasicOperations(t *testing.T) {
	for name, store := range groveStores {
		testGroveBasicOperations(store, name, t)
	}
}

func testGroveBasicOperations(store store_interface.GroveStore, name string, t *testing.T) {
	t.Run(name, func(t *testing.T) {
		space := store_interface.TenancySpace{AppId: 1, TenancyId: 1}
		treeID := store_interface.TreeID("tree1")

		// Test creating root node
		rootID := store_interface.NodeID("root")
		err := store.CreateNode(space, treeID, rootID, nil, nil, nil)
		if err != nil {
			t.Fatalf("Failed to create root node: %v", err)
		}

		// Test node exists
		exists, err := store.Exists(space, treeID, rootID)
		if err != nil {
			t.Fatalf("Failed to check existence: %v", err)
		}
		if !exists {
			t.Fatal("Root node should exist")
		}

		// Test get node info
		info, err := store.GetNodeInfo(space, treeID, rootID)
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
		err = store.CreateNode(space, treeID, child1ID, &rootID, nil, nil)
		if err != nil {
			t.Fatalf("Failed to create child1: %v", err)
		}

		child2ID := store_interface.NodeID("child2")
		err = store.CreateNode(space, treeID, child2ID, &rootID, nil, nil)
		if err != nil {
			t.Fatalf("Failed to create child2: %v", err)
		}

		// Verify child depth
		child1Info, err := store.GetNodeInfo(space, treeID, child1ID)
		if err != nil {
			t.Fatalf("Failed to get child1 info: %v", err)
		}
		if child1Info.Depth != 1 {
			t.Errorf("Child1 should have depth 1, got %d", child1Info.Depth)
		}

		// Test get children
		children, _, err := store.GetChildren(space, treeID, rootID, nil)
		if err != nil {
			t.Fatalf("Failed to get children: %v", err)
		}
		if len(children) != 2 {
			t.Errorf("Expected 2 children, got %d", len(children))
		}

		// Test get ancestors
		ancestors, _, err := store.GetAncestors(space, treeID, child1ID, nil)
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
		err = store.CreateNode(space, treeID, child1ID, &rootID, nil, nil)
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
		treeID := store_interface.TreeID("tree2")

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

		store.CreateNode(space, treeID, root, nil, nil, nil)
		store.CreateNode(space, treeID, a, &root, nil, nil)
		store.CreateNode(space, treeID, b, &root, nil, nil)
		store.CreateNode(space, treeID, c, &a, nil, nil)
		store.CreateNode(space, treeID, d, &a, nil, nil)

		// Get all descendants of root
		descendants, _, err := store.GetDescendants(space, treeID, root, nil)
		if err != nil {
			t.Fatalf("Failed to get descendants: %v", err)
		}
		if len(descendants) != 4 {
			t.Errorf("Expected 4 descendants, got %d", len(descendants))
		}

		// Get descendants of 'a' with max depth 1
		maxDepth := 1
		opts := &store_interface.DescendantOptions{MaxDepth: &maxDepth}
		descendants, _, err = store.GetDescendants(space, treeID, a, opts)
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
		treeID := store_interface.TreeID("tree3")

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

		store.CreateNode(space, treeID, root, nil, nil, nil)
		store.CreateNode(space, treeID, a, &root, nil, nil)
		store.CreateNode(space, treeID, b, &root, nil, nil)
		store.CreateNode(space, treeID, c, &a, nil, nil)

		// Verify initial structure
		cInfo, _ := store.GetNodeInfo(space, treeID, c)
		if *cInfo.Parent != a {
			t.Errorf("Expected c's parent to be a, got %s", *cInfo.Parent)
		}
		if cInfo.Depth != 2 {
			t.Errorf("Expected c's depth to be 2, got %d", cInfo.Depth)
		}

		// Move c from a to b
		err := store.MoveNode(space, treeID, c, &b, nil)
		if err != nil {
			t.Fatalf("Failed to move node: %v", err)
		}

		// Verify new structure
		cInfo, _ = store.GetNodeInfo(space, treeID, c)
		if *cInfo.Parent != b {
			t.Errorf("Expected c's parent to be b after move, got %s", *cInfo.Parent)
		}
		if cInfo.Depth != 2 {
			t.Errorf("Expected c's depth to still be 2, got %d", cInfo.Depth)
		}

		// Verify ancestors
		ancestors, _, _ := store.GetAncestors(space, treeID, c, nil)
		if len(ancestors) != 2 {
			t.Errorf("Expected 2 ancestors (b, root), got %d", len(ancestors))
		}

		// Test cycle detection: try to move b under c (should fail)
		err = store.MoveNode(space, treeID, b, &c, nil)
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
		treeID := store_interface.TreeID("tree4")

		// Create tree:
		//     root
		//     /  \
		//    a    b

		root := store_interface.NodeID("root4")
		a := store_interface.NodeID("a4")
		b := store_interface.NodeID("b4")

		store.CreateNode(space, treeID, root, nil, nil, nil)
		store.CreateNode(space, treeID, a, &root, nil, nil)
		store.CreateNode(space, treeID, b, &root, nil, nil)

		// Apply mutations
		mutation1 := store_interface.MutationID("m1")
		deltas1 := store_interface.AggregateDeltas{
			store_interface.AggregateKey("count"): 5,
			store_interface.AggregateKey("value"): 100,
		}
		err := store.ApplyAggregateMutation(space, treeID, mutation1, a, deltas1)
		if err != nil {
			t.Fatalf("Failed to apply mutation: %v", err)
		}

		mutation2 := store_interface.MutationID("m2")
		deltas2 := store_interface.AggregateDeltas{
			store_interface.AggregateKey("count"): 3,
			store_interface.AggregateKey("value"): 50,
		}
		err = store.ApplyAggregateMutation(space, treeID, mutation2, b, deltas2)
		if err != nil {
			t.Fatalf("Failed to apply mutation: %v", err)
		}

		// Test local aggregates
		localAgg, err := store.GetNodeLocalAggregates(space, treeID, a)
		if err != nil {
			t.Fatalf("Failed to get local aggregates: %v", err)
		}
		if localAgg[store_interface.AggregateKey("count")] != 5 {
			t.Errorf("Expected count=5, got %d", localAgg[store_interface.AggregateKey("count")])
		}

		// Test subtree aggregates
		subtreeAgg, err := store.GetNodeWithDescendantsAggregates(space, treeID, root)
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
		err = store.ApplyAggregateMutation(space, treeID, mutation1, a, deltas1)
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
		treeID := store_interface.TreeID("tree5")

		root := store_interface.NodeID("root5")
		child := store_interface.NodeID("child5")

		store.CreateNode(space, treeID, root, nil, nil, nil)
		store.CreateNode(space, treeID, child, &root, nil, nil)

		// Soft delete child
		err := store.DeleteNode(space, treeID, child, true)
		if err != nil {
			t.Fatalf("Failed to soft delete: %v", err)
		}

		// Node should not exist after soft delete
		exists, _ := store.Exists(space, treeID, child)
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
		treeID := store_interface.TreeID("tree6")

		nodeID := store_interface.NodeID("node6")

		// Create same node ID in different tenancy spaces
		err := store.CreateNode(space1, treeID, nodeID, nil, nil, nil)
		if err != nil {
			t.Fatalf("Failed to create node in space1: %v", err)
		}

		err = store.CreateNode(space2, treeID, nodeID, nil, nil, nil)
		if err != nil {
			t.Fatalf("Failed to create node in space2: %v", err)
		}

		// Both should exist independently
		exists1, _ := store.Exists(space1, treeID, nodeID)
		exists2, _ := store.Exists(space2, treeID, nodeID)

		if !exists1 || !exists2 {
			t.Error("Nodes should exist in both tenancy spaces")
		}

		// Delete from space1 shouldn't affect space2
		store.DeleteNode(space1, treeID, nodeID, false)

		exists1, _ = store.Exists(space1, treeID, nodeID)
		exists2, _ = store.Exists(space2, treeID, nodeID)

		if exists1 {
			t.Error("Node should be deleted from space1")
		}
		if !exists2 {
			t.Error("Node should still exist in space2")
		}
	})
}

func TestGroveTreeIsolation(t *testing.T) {
	for name, store := range groveStores {
		testGroveTreeIsolation(store, name, t)
	}
}

func testGroveTreeIsolation(store store_interface.GroveStore, name string, t *testing.T) {
	t.Run(name, func(t *testing.T) {
		space := store_interface.TenancySpace{AppId: 7, TenancyId: 1}
		tree1 := store_interface.TreeID("fileSystemA")
		tree2 := store_interface.TreeID("fileSystemB")

		// Create same node structure in two different trees
		rootID := store_interface.NodeID("root")
		childID := store_interface.NodeID("child")

		// Create nodes in tree1
		err := store.CreateNode(space, tree1, rootID, nil, nil, nil)
		if err != nil {
			t.Fatalf("Failed to create root in tree1: %v", err)
		}
		err = store.CreateNode(space, tree1, childID, &rootID, nil, nil)
		if err != nil {
			t.Fatalf("Failed to create child in tree1: %v", err)
		}

		// Create same node IDs in tree2 (should work - different tree)
		err = store.CreateNode(space, tree2, rootID, nil, nil, nil)
		if err != nil {
			t.Fatalf("Failed to create root in tree2: %v", err)
		}
		err = store.CreateNode(space, tree2, childID, &rootID, nil, nil)
		if err != nil {
			t.Fatalf("Failed to create child in tree2: %v", err)
		}

		// Both trees should have independent structures
		exists1, _ := store.Exists(space, tree1, rootID)
		exists2, _ := store.Exists(space, tree2, rootID)
		if !exists1 || !exists2 {
			t.Error("Root should exist in both trees")
		}

		// Get children from both trees
		children1, _, err := store.GetChildren(space, tree1, rootID, nil)
		if err != nil {
			t.Fatalf("Failed to get children from tree1: %v", err)
		}
		children2, _, err := store.GetChildren(space, tree2, rootID, nil)
		if err != nil {
			t.Fatalf("Failed to get children from tree2: %v", err)
		}

		if len(children1) != 1 || len(children2) != 1 {
			t.Errorf("Each tree should have 1 child, got tree1=%d, tree2=%d", len(children1), len(children2))
		}

		// Apply different aggregates to same node ID in different trees
		mutation1 := store_interface.MutationID("mut1")
		deltas1 := store_interface.AggregateDeltas{
			store_interface.AggregateKey("count"): 10,
		}
		err = store.ApplyAggregateMutation(space, tree1, mutation1, rootID, deltas1)
		if err != nil {
			t.Fatalf("Failed to apply mutation to tree1: %v", err)
		}

		mutation2 := store_interface.MutationID("mut2")
		deltas2 := store_interface.AggregateDeltas{
			store_interface.AggregateKey("count"): 20,
		}
		err = store.ApplyAggregateMutation(space, tree2, mutation2, rootID, deltas2)
		if err != nil {
			t.Fatalf("Failed to apply mutation to tree2: %v", err)
		}

		// Verify aggregates are independent
		agg1, err := store.GetNodeLocalAggregates(space, tree1, rootID)
		if err != nil {
			t.Fatalf("Failed to get aggregates from tree1: %v", err)
		}
		agg2, err := store.GetNodeLocalAggregates(space, tree2, rootID)
		if err != nil {
			t.Fatalf("Failed to get aggregates from tree2: %v", err)
		}

		if agg1[store_interface.AggregateKey("count")] != 10 {
			t.Errorf("Tree1 count should be 10, got %d", agg1[store_interface.AggregateKey("count")])
		}
		if agg2[store_interface.AggregateKey("count")] != 20 {
			t.Errorf("Tree2 count should be 20, got %d", agg2[store_interface.AggregateKey("count")])
		}

		// Delete from tree1 shouldn't affect tree2
		err = store.DeleteNode(space, tree1, childID, false)
		if err != nil {
			t.Fatalf("Failed to delete child from tree1: %v", err)
		}

		exists1, _ = store.Exists(space, tree1, childID)
		exists2, _ = store.Exists(space, tree2, childID)

		if exists1 {
			t.Error("Child should be deleted from tree1")
		}
		if !exists2 {
			t.Error("Child should still exist in tree2")
		}
	})
}
