package sqlite_store

import (
	"fmt"
	"testing"

	"github.com/vixac/bullet/model"
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
	nodes := make([]store_interface.NodeID, sqliteQueryChunkSize*2+1)
	for i := range nodes {
		nodes[i] = store_interface.NodeID(fmt.Sprintf("node-%d", i))
		if err := store.CreateNode(space, treeID, nodes[i], nil, nil, nil); err != nil {
			t.Fatalf("create node %d: %v", i, err)
		}
	}
	if err := store.ApplyAggregateMutation(space, treeID, "aggregate", nodes[sqliteQueryChunkSize], store_interface.AggregateDeltas{"count": 7}); err != nil {
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
	if got := result[nodes[sqliteQueryChunkSize]]["count"]; got != 7 {
		t.Errorf("aggregate value = %d, want 7", got)
	}
	if len(notFound) != 1 || notFound[0] != "missing" {
		t.Errorf("not found = %v, want [missing]", notFound)
	}
}

func TestTrackQueriesChunkLargeFilters(t *testing.T) {
	store, err := NewSQLiteStore(":memory:")
	if err != nil {
		t.Fatalf("create SQLite store: %v", err)
	}
	defer store.db.Close()

	space := store_interface.TenancySpace{AppId: 1, TenancyId: 1}
	const bucketID int32 = 1
	const itemCount = sqliteQueryChunkSize*2 + 1
	tag := int64(7)
	items := make([]model.TrackKeyValueItem, itemCount)
	keys := make([]string, itemCount)
	for i := range items {
		keys[i] = fmt.Sprintf("key-%d", i)
		items[i] = model.TrackKeyValueItem{
			Key:   keys[i],
			Value: model.TrackValue{Value: int64(i), Tag: &tag},
		}
	}
	if err := store.TrackPutMany(space, map[int32][]model.TrackKeyValueItem{bucketID: items}); err != nil {
		t.Fatalf("put items: %v", err)
	}

	requestedKeys := append(keys, "missing")
	values, missing, err := store.TrackGetMany(space, map[int32][]string{bucketID: requestedKeys})
	if err != nil {
		t.Fatalf("get many: %v", err)
	}
	if len(values[bucketID]) != itemCount {
		t.Errorf("found %d values, want %d", len(values[bucketID]), itemCount)
	}
	if got := missing[bucketID]; len(got) != 1 || got[0] != "missing" {
		t.Errorf("missing = %v, want [missing]", got)
	}

	tags := make([]int64, itemCount)
	tags[0] = tag
	for i := 1; i < len(tags); i++ {
		tags[i] = int64(i + 100)
	}
	byPrefix, err := store.GetItemsByKeyPrefix(space, bucketID, "key-", tags, nil, false)
	if err != nil {
		t.Fatalf("get by key prefix: %v", err)
	}
	if len(byPrefix) != itemCount {
		t.Errorf("prefix result has %d items, want %d", len(byPrefix), itemCount)
	}

	prefixes := make([]string, itemCount)
	prefixes[0] = "key-"
	for i := 1; i < len(prefixes); i++ {
		prefixes[i] = fmt.Sprintf("not-a-match-%d", i)
	}
	byPrefixes, err := store.GetItemsByKeyPrefixes(space, bucketID, prefixes, tags, nil, false)
	if err != nil {
		t.Fatalf("get by key prefixes: %v", err)
	}
	if len(byPrefixes) != itemCount {
		t.Errorf("prefixes result has %d items, want %d", len(byPrefixes), itemCount)
	}
}
