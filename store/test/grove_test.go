package store_test

import (
	"testing"

	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/store/store_interface"
)

// groveStores is defined and populated in stores.go
// TestMain is defined in stores.go

func TestGroveBasicOperations(t *testing.T) {
	for name, store := range groveStores {
		testGroveBasicOperations(store, name, t)
	}
}

func testGroveBasicOperations(store store_interface.GroveStore, name string, t *testing.T) {
	t.Run(name, func(t *testing.T) {
		space := model.TenancySpace{AppId: 1, TenancyId: 1}
		treeID := model.TreeID("tree1")

		// Test creating root node
		rootID := model.NodeID("root")
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
		child1ID := model.NodeID("child1")
		err = store.CreateNode(space, treeID, child1ID, &rootID, nil, nil)
		if err != nil {
			t.Fatalf("Failed to create child1: %v", err)
		}

		child2ID := model.NodeID("child2")
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
		if err != model.ErrNodeAlreadyExists {
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
		space := model.TenancySpace{AppId: 2, TenancyId: 1}
		treeID := model.TreeID("tree2")

		// Create tree:
		//     root
		//     /  \
		//    a    b
		//   / \
		//  c   d

		root := model.NodeID("root2")
		a := model.NodeID("a2")
		b := model.NodeID("b2")
		c := model.NodeID("c2")
		d := model.NodeID("d2")

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
		opts := &model.DescendantOptions{MaxDepth: &maxDepth}
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
		space := model.TenancySpace{AppId: 3, TenancyId: 1}
		treeID := model.TreeID("tree3")

		// Create tree:
		//     root
		//     /  \
		//    a    b
		//   /
		//  c

		root := model.NodeID("root3")
		a := model.NodeID("a3")
		b := model.NodeID("b3")
		c := model.NodeID("c3")

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
		if err != model.ErrCycleDetected {
			t.Errorf("Expected ErrCycleDetected, got %v", err)
		}
	})
}

func TestGroveMoveNodeWithDescendants(t *testing.T) {
	for name, store := range groveStores {
		testGroveMoveNodeWithDescendants(store, name, t)
	}
}

func testGroveMoveNodeWithDescendants(store store_interface.GroveStore, name string, t *testing.T) {
	t.Run(name, func(t *testing.T) {
		space := model.TenancySpace{AppId: 11, TenancyId: 1}
		treeID := model.TreeID("tree11")

		// Create tree:
		//     root
		//     /  \
		//    A    Z
		//   / \
		//  B   C
		//     / \
		//    D   E

		root := model.NodeID("root11")
		A := model.NodeID("A11")
		B := model.NodeID("B11")
		C := model.NodeID("C11")
		D := model.NodeID("D11")
		E := model.NodeID("E11")
		Z := model.NodeID("Z11")

		store.CreateNode(space, treeID, root, nil, nil, nil)
		store.CreateNode(space, treeID, A, &root, nil, nil)
		store.CreateNode(space, treeID, B, &A, nil, nil)
		store.CreateNode(space, treeID, C, &A, nil, nil)
		store.CreateNode(space, treeID, D, &C, nil, nil)
		store.CreateNode(space, treeID, E, &C, nil, nil)
		store.CreateNode(space, treeID, Z, &root, nil, nil)

		// Move C (with its descendants D and E) from A to Z.
		// Expected tree after move:
		//     root
		//     /  \
		//    A    Z
		//    |    |
		//    B    C
		//        / \
		//       D   E

		err := store.MoveNode(space, treeID, C, &Z, nil)
		if err != nil {
			t.Fatalf("Failed to move node: %v", err)
		}

		// C should now be a child of Z at depth 2
		cInfo, err := store.GetNodeInfo(space, treeID, C)
		if err != nil {
			t.Fatalf("Failed to get C info: %v", err)
		}
		if *cInfo.Parent != Z {
			t.Errorf("Expected C's parent to be Z, got %s", *cInfo.Parent)
		}
		if cInfo.Depth != 2 {
			t.Errorf("Expected C's depth to be 2, got %d", cInfo.Depth)
		}

		// D should still be a child of C (not a child of Z)
		dInfo, err := store.GetNodeInfo(space, treeID, D)
		if err != nil {
			t.Fatalf("Failed to get D info: %v", err)
		}
		if *dInfo.Parent != C {
			t.Errorf("Expected D's parent to still be C after move, got %s", *dInfo.Parent)
		}
		if dInfo.Depth != 3 {
			t.Errorf("Expected D's depth to be 3, got %d", dInfo.Depth)
		}

		// E should still be a child of C (not a child of Z)
		eInfo, err := store.GetNodeInfo(space, treeID, E)
		if err != nil {
			t.Fatalf("Failed to get E info: %v", err)
		}
		if *eInfo.Parent != C {
			t.Errorf("Expected E's parent to still be C after move, got %s", *eInfo.Parent)
		}
		if eInfo.Depth != 3 {
			t.Errorf("Expected E's depth to be 3, got %d", eInfo.Depth)
		}

		// C's descendants should be exactly D and E (at relative depth 1)
		cDescendants, _, err := store.GetDescendants(space, treeID, C, nil)
		if err != nil {
			t.Fatalf("Failed to get descendants of C: %v", err)
		}
		if len(cDescendants) != 2 {
			t.Errorf("Expected C to have 2 descendants (D, E), got %d", len(cDescendants))
		}
		for _, desc := range cDescendants {
			if desc.NodeID != D && desc.NodeID != E {
				t.Errorf("Unexpected descendant of C: %s", desc.NodeID)
			}
			if desc.Depth != 1 {
				t.Errorf("Expected D/E to be at relative depth 1 under C, got %d for %s", desc.Depth, desc.NodeID)
			}
		}

		// Z's descendants should be C (depth 1), D (depth 2), and E (depth 2)
		zDescendants, _, err := store.GetDescendants(space, treeID, Z, nil)
		if err != nil {
			t.Fatalf("Failed to get descendants of Z: %v", err)
		}
		if len(zDescendants) != 3 {
			t.Errorf("Expected Z to have 3 descendants (C, D, E), got %d", len(zDescendants))
		}
		zDescMap := make(map[model.NodeID]int)
		for _, desc := range zDescendants {
			zDescMap[desc.NodeID] = desc.Depth
		}
		if zDescMap[C] != 1 {
			t.Errorf("Expected C at depth 1 under Z, got %d", zDescMap[C])
		}
		if zDescMap[D] != 2 {
			t.Errorf("Expected D at depth 2 under Z, got %d", zDescMap[D])
		}
		if zDescMap[E] != 2 {
			t.Errorf("Expected E at depth 2 under Z, got %d", zDescMap[E])
		}

		// A's descendants should only be B — C, D, E were moved away
		aDescendants, _, err := store.GetDescendants(space, treeID, A, nil)
		if err != nil {
			t.Fatalf("Failed to get descendants of A: %v", err)
		}
		if len(aDescendants) != 1 {
			t.Errorf("Expected A to have 1 descendant (B only), got %d", len(aDescendants))
		}
		if len(aDescendants) == 1 && aDescendants[0].NodeID != B {
			t.Errorf("Expected A's only descendant to be B, got %s", aDescendants[0].NodeID)
		}

		// D's ancestors should be C, Z, root (in root-first order)
		dAncestors, _, err := store.GetAncestors(space, treeID, D, nil)
		if err != nil {
			t.Fatalf("Failed to get ancestors of D: %v", err)
		}
		if len(dAncestors) != 3 {
			t.Errorf("Expected D to have 3 ancestors (root, Z, C), got %d: %v", len(dAncestors), dAncestors)
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
		space := model.TenancySpace{AppId: 4, TenancyId: 1}
		treeID := model.TreeID("tree4")

		// Create tree:
		//     root
		//     /  \
		//    a    b

		root := model.NodeID("root4")
		a := model.NodeID("a4")
		b := model.NodeID("b4")

		store.CreateNode(space, treeID, root, nil, nil, nil)
		store.CreateNode(space, treeID, a, &root, nil, nil)
		store.CreateNode(space, treeID, b, &root, nil, nil)

		// Apply mutations
		mutation1 := model.MutationID("m1")
		deltas1 := model.AggregateDeltas{
			model.AggregateKey("count"): 5,
			model.AggregateKey("value"): 100,
		}
		err := store.ApplyAggregateMutation(space, treeID, mutation1, a, deltas1)
		if err != nil {
			t.Fatalf("Failed to apply mutation: %v", err)
		}

		mutation2 := model.MutationID("m2")
		deltas2 := model.AggregateDeltas{
			model.AggregateKey("count"): 3,
			model.AggregateKey("value"): 50,
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
		if localAgg[model.AggregateKey("count")] != 5 {
			t.Errorf("Expected count=5, got %d", localAgg[model.AggregateKey("count")])
		}

		// Test subtree aggregates
		subtreeAgg, err := store.GetNodeWithDescendantsAggregates(space, treeID, root)
		if err != nil {
			t.Fatalf("Failed to get subtree aggregates: %v", err)
		}
		if subtreeAgg[model.AggregateKey("count")] != 8 {
			t.Errorf("Expected total count=8, got %d", subtreeAgg[model.AggregateKey("count")])
		}
		if subtreeAgg[model.AggregateKey("value")] != 150 {
			t.Errorf("Expected total value=150, got %d", subtreeAgg[model.AggregateKey("value")])
		}

		// Test idempotency: applying same mutation should fail
		err = store.ApplyAggregateMutation(space, treeID, mutation1, a, deltas1)
		if err != model.ErrMutationConflict {
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
		space := model.TenancySpace{AppId: 5, TenancyId: 1}
		treeID := model.TreeID("tree5")

		root := model.NodeID("root5")
		child := model.NodeID("child5")

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
		space1 := model.TenancySpace{AppId: 6, TenancyId: 1}
		space2 := model.TenancySpace{AppId: 6, TenancyId: 2}
		treeID := model.TreeID("tree6")

		nodeID := model.NodeID("node6")

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

func TestGroveGetAncestorsBulk(t *testing.T) {
	for name, store := range groveStores {
		testGroveGetAncestorsBulk(store, name, t)
	}
}

func testGroveGetAncestorsBulk(store store_interface.GroveStore, name string, t *testing.T) {
	t.Run(name, func(t *testing.T) {
		space := model.TenancySpace{AppId: 8, TenancyId: 1}
		treeID := model.TreeID("tree8")

		// Tree:
		//       root
		//      /    \
		//     a      b
		//    / \
		//   c   d

		root := model.NodeID("root8")
		a := model.NodeID("a8")
		b := model.NodeID("b8")
		c := model.NodeID("c8")
		d := model.NodeID("d8")

		store.CreateNode(space, treeID, root, nil, nil, nil)
		store.CreateNode(space, treeID, a, &root, nil, nil)
		store.CreateNode(space, treeID, b, &root, nil, nil)
		store.CreateNode(space, treeID, c, &a, nil, nil)
		store.CreateNode(space, treeID, d, &a, nil, nil)

		t.Run("bulk lookup of leaf nodes", func(t *testing.T) {
			result, notFound, err := store.GetAncestorsBulk(space, treeID, []model.NodeID{c, d})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(notFound) != 0 {
				t.Errorf("expected no missing nodes, got %v", notFound)
			}
			// c's ancestors: a, root (root-first order)
			cAnc := result[c]
			if len(cAnc) != 2 {
				t.Errorf("c: expected 2 ancestors, got %d: %v", len(cAnc), cAnc)
			} else if cAnc[0] != root || cAnc[1] != a {
				t.Errorf("c: expected [root, a], got %v", cAnc)
			}
			// d's ancestors: a, root (root-first order)
			dAnc := result[d]
			if len(dAnc) != 2 {
				t.Errorf("d: expected 2 ancestors, got %d: %v", len(dAnc), dAnc)
			} else if dAnc[0] != root || dAnc[1] != a {
				t.Errorf("d: expected [root, a], got %v", dAnc)
			}
		})

		t.Run("root node has no ancestors", func(t *testing.T) {
			result, notFound, err := store.GetAncestorsBulk(space, treeID, []model.NodeID{root})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(notFound) != 0 {
				t.Errorf("expected no missing nodes, got %v", notFound)
			}
			ancestors, ok := result[root]
			if !ok {
				t.Fatal("root should be present in result map")
			}
			if len(ancestors) != 0 {
				t.Errorf("root should have 0 ancestors, got %v", ancestors)
			}
		})

		t.Run("mixed found and not found", func(t *testing.T) {
			missing := model.NodeID("doesNotExist8")
			result, notFound, err := store.GetAncestorsBulk(space, treeID, []model.NodeID{c, missing})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(notFound) != 1 || notFound[0] != missing {
				t.Errorf("expected [%s] in notFound, got %v", missing, notFound)
			}
			if _, ok := result[missing]; ok {
				t.Error("missing node should not appear in result map")
			}
			if _, ok := result[c]; !ok {
				t.Error("found node c should appear in result map")
			}
		})

		t.Run("empty input", func(t *testing.T) {
			result, notFound, err := store.GetAncestorsBulk(space, treeID, []model.NodeID{})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(result) != 0 {
				t.Errorf("expected empty result, got %v", result)
			}
			if len(notFound) != 0 {
				t.Errorf("expected no missing nodes, got %v", notFound)
			}
		})

		t.Run("all nodes not found", func(t *testing.T) {
			x := model.NodeID("x8")
			y := model.NodeID("y8")
			result, notFound, err := store.GetAncestorsBulk(space, treeID, []model.NodeID{x, y})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(result) != 0 {
				t.Errorf("expected empty result, got %v", result)
			}
			if len(notFound) != 2 {
				t.Errorf("expected 2 missing nodes, got %v", notFound)
			}
		})

		t.Run("mixed depths in single call", func(t *testing.T) {
			// b is at depth 1, c is at depth 2
			result, notFound, err := store.GetAncestorsBulk(space, treeID, []model.NodeID{b, c, root})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(notFound) != 0 {
				t.Errorf("expected no missing nodes, got %v", notFound)
			}
			if len(result[b]) != 1 || result[b][0] != root {
				t.Errorf("b: expected [root], got %v", result[b])
			}
			if len(result[c]) != 2 {
				t.Errorf("c: expected 2 ancestors, got %d", len(result[c]))
			}
			if len(result[root]) != 0 {
				t.Errorf("root: expected 0 ancestors, got %v", result[root])
			}
		})
	})
}

func TestGroveGetNodeLocalAggregatesBulk(t *testing.T) {
	for name, store := range groveStores {
		testGroveGetNodeLocalAggregatesBulk(store, name, t)
	}
}

func testGroveGetNodeLocalAggregatesBulk(store store_interface.GroveStore, name string, t *testing.T) {
	t.Run(name, func(t *testing.T) {
		space := model.TenancySpace{AppId: 9, TenancyId: 1}
		treeID := model.TreeID("tree9")

		// Tree:
		//       root
		//      /    \
		//     a      b
		//    /
		//   c

		root := model.NodeID("root9")
		a := model.NodeID("a9")
		b := model.NodeID("b9")
		c := model.NodeID("c9")

		store.CreateNode(space, treeID, root, nil, nil, nil)
		store.CreateNode(space, treeID, a, &root, nil, nil)
		store.CreateNode(space, treeID, b, &root, nil, nil)
		store.CreateNode(space, treeID, c, &a, nil, nil)

		// Apply aggregates: a has count=5,value=100; b has count=3
		store.ApplyAggregateMutation(space, treeID, "m1", a, model.AggregateDeltas{
			model.AggregateKey("count"): 5,
			model.AggregateKey("value"): 100,
		})
		store.ApplyAggregateMutation(space, treeID, "m2", b, model.AggregateDeltas{
			model.AggregateKey("count"): 3,
		})
		// root and c have no aggregates

		t.Run("bulk lookup returns correct aggregates", func(t *testing.T) {
			result, notFound, err := store.GetNodeLocalAggregatesBulk(space, treeID, []model.NodeID{a, b})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(notFound) != 0 {
				t.Errorf("expected no missing nodes, got %v", notFound)
			}
			if result[a][model.AggregateKey("count")] != 5 {
				t.Errorf("a: expected count=5, got %d", result[a][model.AggregateKey("count")])
			}
			if result[a][model.AggregateKey("value")] != 100 {
				t.Errorf("a: expected value=100, got %d", result[a][model.AggregateKey("value")])
			}
			if result[b][model.AggregateKey("count")] != 3 {
				t.Errorf("b: expected count=3, got %d", result[b][model.AggregateKey("count")])
			}
		})

		t.Run("node with no aggregates returns empty map", func(t *testing.T) {
			result, notFound, err := store.GetNodeLocalAggregatesBulk(space, treeID, []model.NodeID{root, c})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(notFound) != 0 {
				t.Errorf("expected no missing nodes, got %v", notFound)
			}
			if _, ok := result[root]; !ok {
				t.Error("root should be present in result map")
			}
			if len(result[root]) != 0 {
				t.Errorf("root: expected empty aggregates, got %v", result[root])
			}
			if len(result[c]) != 0 {
				t.Errorf("c: expected empty aggregates, got %v", result[c])
			}
		})

		t.Run("mixed found and not found", func(t *testing.T) {
			missing := model.NodeID("doesNotExist9")
			result, notFound, err := store.GetNodeLocalAggregatesBulk(space, treeID, []model.NodeID{a, missing})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(notFound) != 1 || notFound[0] != missing {
				t.Errorf("expected [%s] in notFound, got %v", missing, notFound)
			}
			if _, ok := result[missing]; ok {
				t.Error("missing node should not appear in result map")
			}
			if _, ok := result[a]; !ok {
				t.Error("found node a should appear in result map")
			}
		})

		t.Run("empty input", func(t *testing.T) {
			result, notFound, err := store.GetNodeLocalAggregatesBulk(space, treeID, []model.NodeID{})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(result) != 0 {
				t.Errorf("expected empty result, got %v", result)
			}
			if len(notFound) != 0 {
				t.Errorf("expected no missing nodes, got %v", notFound)
			}
		})

		t.Run("all nodes not found", func(t *testing.T) {
			x := model.NodeID("x9")
			y := model.NodeID("y9")
			result, notFound, err := store.GetNodeLocalAggregatesBulk(space, treeID, []model.NodeID{x, y})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(result) != 0 {
				t.Errorf("expected empty result, got %v", result)
			}
			if len(notFound) != 2 {
				t.Errorf("expected 2 missing nodes, got %v", notFound)
			}
		})

		t.Run("mixed nodes with and without aggregates", func(t *testing.T) {
			result, notFound, err := store.GetNodeLocalAggregatesBulk(space, treeID, []model.NodeID{a, b, c, root})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(notFound) != 0 {
				t.Errorf("expected no missing nodes, got %v", notFound)
			}
			if len(result) != 4 {
				t.Errorf("expected 4 nodes in result, got %d", len(result))
			}
			if result[a][model.AggregateKey("count")] != 5 {
				t.Errorf("a: expected count=5, got %d", result[a][model.AggregateKey("count")])
			}
			if len(result[c]) != 0 {
				t.Errorf("c: expected empty aggregates, got %v", result[c])
			}
		})
	})
}

func testGroveTreeIsolation(store store_interface.GroveStore, name string, t *testing.T) {
	t.Run(name, func(t *testing.T) {
		space := model.TenancySpace{AppId: 7, TenancyId: 1}
		tree1 := model.TreeID("fileSystemA")
		tree2 := model.TreeID("fileSystemB")

		// Create same node structure in two different trees
		rootID := model.NodeID("root")
		childID := model.NodeID("child")

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
		mutation1 := model.MutationID("mut1")
		deltas1 := model.AggregateDeltas{
			model.AggregateKey("count"): 10,
		}
		err = store.ApplyAggregateMutation(space, tree1, mutation1, rootID, deltas1)
		if err != nil {
			t.Fatalf("Failed to apply mutation to tree1: %v", err)
		}

		mutation2 := model.MutationID("mut2")
		deltas2 := model.AggregateDeltas{
			model.AggregateKey("count"): 20,
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

		if agg1[model.AggregateKey("count")] != 10 {
			t.Errorf("Tree1 count should be 10, got %d", agg1[model.AggregateKey("count")])
		}
		if agg2[model.AggregateKey("count")] != 20 {
			t.Errorf("Tree2 count should be 20, got %d", agg2[model.AggregateKey("count")])
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

func TestGroveGetNodeWithDescendantsAggregatesBulk(t *testing.T) {
	for name, store := range groveStores {
		if name == "boltdb" {
			continue
		}
		testGroveGetNodeWithDescendantsAggregatesBulk(store, name, t)
	}
}

func testGroveGetNodeWithDescendantsAggregatesBulk(store store_interface.GroveStore, name string, t *testing.T) {
	t.Run(name, func(t *testing.T) {
		space := model.TenancySpace{AppId: 10, TenancyId: 1}
		treeID := model.TreeID("tree10")

		// Tree:
		//       root
		//      /    \
		//     a      b
		//    /
		//   c
		//
		// Aggregates: a=count:5,value:100  b=count:3  c=count:2  root=none

		root := model.NodeID("root10")
		a := model.NodeID("a10")
		b := model.NodeID("b10")
		c := model.NodeID("c10")

		store.CreateNode(space, treeID, root, nil, nil, nil)
		store.CreateNode(space, treeID, a, &root, nil, nil)
		store.CreateNode(space, treeID, b, &root, nil, nil)
		store.CreateNode(space, treeID, c, &a, nil, nil)

		store.ApplyAggregateMutation(space, treeID, "m1", a, model.AggregateDeltas{
			model.AggregateKey("count"): 5,
			model.AggregateKey("value"): 100,
		})
		store.ApplyAggregateMutation(space, treeID, "m2", b, model.AggregateDeltas{
			model.AggregateKey("count"): 3,
		})
		store.ApplyAggregateMutation(space, treeID, "m3", c, model.AggregateDeltas{
			model.AggregateKey("count"): 2,
		})

		t.Run("subtree sums are correct", func(t *testing.T) {
			result, notFound, err := store.GetNodeWithDescendantsAggregatesBulk(space, treeID, []model.NodeID{root, a, b})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(notFound) != 0 {
				t.Errorf("expected no missing nodes, got %v", notFound)
			}
			// root subtree: a(5)+b(3)+c(2)=10 count, a(100) value
			if result[root][model.AggregateKey("count")] != 10 {
				t.Errorf("root: expected count=10, got %d", result[root][model.AggregateKey("count")])
			}
			if result[root][model.AggregateKey("value")] != 100 {
				t.Errorf("root: expected value=100, got %d", result[root][model.AggregateKey("value")])
			}
			// a subtree: a(5)+c(2)=7 count, a(100) value
			if result[a][model.AggregateKey("count")] != 7 {
				t.Errorf("a: expected count=7, got %d", result[a][model.AggregateKey("count")])
			}
			if result[a][model.AggregateKey("value")] != 100 {
				t.Errorf("a: expected value=100, got %d", result[a][model.AggregateKey("value")])
			}
			// b subtree: b(3) count only
			if result[b][model.AggregateKey("count")] != 3 {
				t.Errorf("b: expected count=3, got %d", result[b][model.AggregateKey("count")])
			}
		})

		t.Run("node with no aggregates returns empty map not notFound", func(t *testing.T) {
			result, notFound, err := store.GetNodeWithDescendantsAggregatesBulk(space, treeID, []model.NodeID{root})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			// root exists so must not be in notFound even though it has no local aggregates
			if len(notFound) != 0 {
				t.Errorf("expected no missing nodes, got %v", notFound)
			}
			if _, ok := result[root]; !ok {
				t.Error("root should be present in result map")
			}
		})

		t.Run("non-existent node goes to notFound", func(t *testing.T) {
			missing := model.NodeID("doesNotExist10")
			result, notFound, err := store.GetNodeWithDescendantsAggregatesBulk(space, treeID, []model.NodeID{a, missing})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(notFound) != 1 || notFound[0] != missing {
				t.Errorf("expected [%s] in notFound, got %v", missing, notFound)
			}
			if _, ok := result[missing]; ok {
				t.Error("missing node should not appear in result map")
			}
			if _, ok := result[a]; !ok {
				t.Error("found node a should appear in result map")
			}
		})

		t.Run("empty input", func(t *testing.T) {
			result, notFound, err := store.GetNodeWithDescendantsAggregatesBulk(space, treeID, []model.NodeID{})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(result) != 0 {
				t.Errorf("expected empty result, got %v", result)
			}
			if len(notFound) != 0 {
				t.Errorf("expected no missing nodes, got %v", notFound)
			}
		})

		t.Run("overlapping subtrees are independent", func(t *testing.T) {
			// root and a have overlapping subtrees; each should reflect its own subtree sum
			result, notFound, err := store.GetNodeWithDescendantsAggregatesBulk(space, treeID, []model.NodeID{root, a})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(notFound) != 0 {
				t.Errorf("expected no missing nodes, got %v", notFound)
			}
			if result[root][model.AggregateKey("count")] != 10 {
				t.Errorf("root: expected count=10, got %d", result[root][model.AggregateKey("count")])
			}
			if result[a][model.AggregateKey("count")] != 7 {
				t.Errorf("a: expected count=7, got %d", result[a][model.AggregateKey("count")])
			}
		})
	})
}
