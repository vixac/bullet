package sqlite_store

import (
	"fmt"
	"testing"

	"github.com/vixac/bullet/store/store_interface"
)

func TestGetNodeWithDescendantsAggregatesBulkChunksLargeRequests(t *testing.T) {
	store, err := NewSQLiteStore(":memory:")
	if err != nil {
		t.Fatalf("create SQLite store: %v", err)
	}
	defer store.db.Close()

	space := store_interface.TenancySpace{AppId: 1, TenancyId: 1}
	treeID := store_interface.TreeID("bulk-chunks")
	nodes := make([]store_interface.NodeID, sqliteAggregateBulkChunkSize*2+1)
	for i := range nodes {
		nodes[i] = store_interface.NodeID(fmt.Sprintf("node-%d", i))
		if err := store.CreateNode(space, treeID, nodes[i], nil, nil, nil); err != nil {
			t.Fatalf("create node %d: %v", i, err)
		}
	}
	if err := store.ApplyAggregateMutation(space, treeID, "aggregate", nodes[sqliteAggregateBulkChunkSize], store_interface.AggregateDeltas{"count": 7}); err != nil {
		t.Fatalf("apply aggregate: %v", err)
	}

	requested := append(nodes, store_interface.NodeID("missing"))
	result, notFound, err := store.GetNodeWithDescendantsAggregatesBulk(space, treeID, requested)
	if err != nil {
		t.Fatalf("bulk aggregates: %v", err)
	}
	if len(result) != len(nodes) {
		t.Fatalf("result contains %d nodes, want %d", len(result), len(nodes))
	}
	if got := result[nodes[sqliteAggregateBulkChunkSize]]["count"]; got != 7 {
		t.Errorf("aggregate value = %d, want 7", got)
	}
	if len(notFound) != 1 || notFound[0] != "missing" {
		t.Errorf("not found = %v, want [missing]", notFound)
	}
}
