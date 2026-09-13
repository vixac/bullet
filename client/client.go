package client

import (
	"context"

	"github.com/vixac/bullet/model"
)

// Ledger provides append-only ledger operations in the client's configured tenancy.
type Ledger interface {
	LedgerAppend(ledgerID model.LedgerID, appendID model.LedgerAppendID, payload string) (model.LedgerRecord, error)
	LedgerAppendMany(ledgerID model.LedgerID, items []model.LedgerAppendItem) ([]model.LedgerRecord, error)
	LedgerReadBackward(selector model.LedgerSelector, cursor *string, limit int) (model.LedgerPage, error)
	// LedgerReadForward returns records in ascending position order. after is
	// exclusive. A nil through position makes the read live; otherwise through
	// is an inclusive upper boundary.
	LedgerReadForward(selector model.LedgerSelector, after model.LedgerPosition, through *model.LedgerPosition, limit int) ([]model.LedgerRecord, error)
}

// Track provides atomic data operations within the client's configured tenancy. Writes
// commit all changes or none; successful reads return complete results from one
// consistent snapshot, including all buckets, query chunks, and cursor batches.
// Read errors return no partial results. Atomicity applies to each call, not to
// a sequence of calls, and does not imply that reads use the latest snapshot.
// Callers must not modify inputs during a call; returned data is caller-owned.
type Track interface {
	// TrackMutate atomically applies all puts, then all deletes, across every
	// requested bucket in the client's configured tenancy space, and records MutationID
	// in the same commit. All items use this one space.
	// Either the entire mutation commits or none of it does. Mutation IDs are
	// store-wide: replaying an ID returns Applied=false without applying changes.
	// On success Applied=true means this call committed the mutation. A commit
	// or transport error can leave the outcome unknown; retry with the same ID.
	TrackMutate(req model.TrackMutation) (model.TrackMutationResult, error)

	// TrackPut atomically upserts the value, tag, and metric for one key.
	// A commit or transport error can leave the caller unsure whether it committed.
	TrackPut(bucketID int32, key string, value int64, tag *int64, metric *float64) error

	// TrackGet atomically reads one key's value from a consistent snapshot.
	TrackGet(bucketID int32, key string) (int64, error)

	// TrackDeleteMany atomically deletes the entire batch, including across buckets:
	// either all deletions commit or none do. No partial batch is committed.
	// A commit/transport error can leave the caller uncertain whether all or none
	// committed; an error does not necessarily mean nothing changed.
	TrackDeleteMany(items []model.TrackKey) error

	// TrackPutMany atomically upserts the entire batch, including across buckets:
	// either all updates commit or none do. No partial batch is committed.
	// A commit/transport error can leave the caller uncertain whether all or none
	// committed; an error does not necessarily mean nothing changed.
	TrackPutMany(items map[int32][]model.TrackKeyValueItem) error

	// TrackGetMany atomically reads all requested keys from one snapshot across
	// all buckets. Values and missing keys describe that same snapshot.
	TrackGetMany(keys map[int32][]string) (map[int32]map[string]model.TrackValue, map[int32][]string, error)

	// GetItemsByKeyPrefix atomically reads all matching items from one snapshot.
	GetItemsByKeyPrefix(
		bucketID int32,
		prefix string,
		tags []int64, // optional slice of tags
		metricValue *float64, // optional metric value
		metricIsGt bool, // "gt" or "lt"
	) ([]model.TrackKeyValueItem, error)

	// GetItemsByKeyPrefixes atomically reads all matching items from one snapshot
	// shared by every prefix, including when the query is split into chunks.
	GetItemsByKeyPrefixes(bucketID int32,
		prefixes []string,
		tags []int64,
		metricValue *float64,
		metricIsGt bool,
	) ([]model.TrackKeyValueItem, error)
}

// Depot provides payload operations in the client's configured tenancy.
type Depot interface {
	DepotCreate(bucketID int32, value string) (int64, error)
	DepotCreateMany(bucketID int32, values []string) ([]int64, error)

	DepotUpdate(id int64, value string) error

	DepotGet(id int64) (string, error)
	DepotGetMany(ids []int64) (map[int64]string, []int64, error)

	DepotDelete(id int64) error
	DepotDeleteByBucket(bucketID int32) error
	DepotGetAllByBucket(bucketID int32) (map[int64]string, error) //VX:Note this wants to become paginated at some point.
}

// Grove provides tree operations in the client's configured tenancy.
type Grove interface {
	// Single node operations
	CreateNode(treeID model.TreeID, node model.NodeID, parent *model.NodeID, position *model.ChildPosition, metadata *model.NodeMetadata) error
	DeleteNode(treeID model.TreeID, node model.NodeID, soft bool) error
	MoveNode(treeID model.TreeID, node model.NodeID, newParent *model.NodeID, newPosition *model.ChildPosition) error

	ApplyAggregateMutation(
		treeID model.TreeID,
		mutation model.MutationID,
		node model.NodeID,
		deltas model.AggregateDeltas,
	) error
	GetNodeLocalAggregates(treeID model.TreeID, node model.NodeID) (map[model.AggregateKey]model.AggregateValue, error)           // Node only
	GetNodeWithDescendantsAggregates(treeID model.TreeID, node model.NodeID) (map[model.AggregateKey]model.AggregateValue, error) // Node + all descendants
	GetNodeWithDescendantsAggregatesBulk(treeID model.TreeID, nodes []model.NodeID) (map[model.NodeID]map[model.AggregateKey]model.AggregateValue, []model.NodeID, error)

	Exists(treeID model.TreeID, node model.NodeID) (bool, error)
	GetNodeInfo(treeID model.TreeID, node model.NodeID) (*model.NodeInfo, error)
	GetChildren(treeID model.TreeID, node model.NodeID, pagination *model.PaginationParams) ([]model.NodeID, *model.PaginationResult, error)
	GetAncestors(treeID model.TreeID, node model.NodeID, pagination *model.PaginationParams) ([]model.NodeID, *model.PaginationResult, error)
	// GetAncestorsBulk returns ancestors for multiple nodes in a single call.
	// The returned map contains found nodes (key = node, value = ancestors ordered root-first).
	// The second return value lists node IDs that were not found.
	GetAncestorsBulk(treeID model.TreeID, nodes []model.NodeID) (map[model.NodeID][]model.NodeID, []model.NodeID, error)
	// GetNodeLocalAggregatesBulk returns local aggregates for multiple nodes in a single call.
	// The returned map contains found nodes (key = node, value = aggregates map).
	// The second return value lists node IDs that were not found.
	GetNodeLocalAggregatesBulk(treeID model.TreeID, nodes []model.NodeID) (map[model.NodeID]map[model.AggregateKey]model.AggregateValue, []model.NodeID, error)
	GetDescendants(treeID model.TreeID, node model.NodeID, opts *model.DescendantOptions) ([]model.NodeWithDepth, *model.PaginationResult, error)
}

// Client combines all tenant-scoped Bullet interfaces.
type Client interface {
	Warehouse
	Track
	Depot
	Grove
	Ledger
}

// Warehouse stores immutable blobs in the client's configured tenancy.
// Repeated PutIDs with identical data return the original blob; different data
// conflicts. GetMany omits missing IDs and returns a non-nil map. Values are
// caller-owned. Errors use the sentinels defined by package model.
type Warehouse interface {
	WarehousePut(context.Context, model.PutBlobRequest) (model.Blob, error)
	WarehouseGet(context.Context, model.BlobID) (model.Blob, error)
	WarehouseGetMany(context.Context, []model.BlobID) (map[model.BlobID]model.Blob, error)
}
