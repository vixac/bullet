package store_interface

import "github.com/vixac/bullet/model"

// LedgerStore provides append-only ledger operations.
type LedgerStore interface {
	LedgerAppend(space model.TenancySpace, ledgerID model.LedgerID, appendID model.LedgerAppendID, payload string) (model.LedgerRecord, error)
	LedgerAppendMany(space model.TenancySpace, ledgerID model.LedgerID, items []model.LedgerAppendItem) ([]model.LedgerRecord, error)
	LedgerReadBackward(space model.TenancySpace, selector model.LedgerSelector, cursor *string, limit int) (model.LedgerPage, error)
	// LedgerReadForward returns records in ascending position order. after is
	// exclusive. A nil through position makes the read live; otherwise through
	// is an inclusive upper boundary.
	LedgerReadForward(space model.TenancySpace, selector model.LedgerSelector, after model.LedgerPosition, through *model.LedgerPosition, limit int) ([]model.LedgerRecord, error)
}

// TrackStore provides atomic data operations within one backing store. Writes
// commit all changes or none; successful reads return complete results from one
// consistent snapshot, including all buckets, query chunks, and cursor batches.
// Read errors return no partial results. Atomicity applies to each call, not to
// a sequence of calls, and does not imply that reads use the latest snapshot.
// Callers must not modify inputs during a call; returned data is caller-owned.
type TrackStore interface {
	// TrackMutate atomically applies all puts, then all deletes, across every
	// requested bucket in the supplied tenancy space, and records MutationID
	// in the same commit. All items use this one space.
	// Either the entire mutation commits or none of it does. Mutation IDs are
	// store-wide: replaying an ID returns Applied=false without applying changes.
	// A put with IfAbsent=true fails with ErrTrackKeyAlreadyExists if its key
	// already exists; that failure rolls back every operation and does not record
	// the mutation ID. False retains the usual upsert behavior.
	// On success Applied=true means this call committed the mutation. A commit
	// or transport error can leave the outcome unknown; retry with the same ID.
	TrackMutate(space model.TenancySpace, req model.TrackMutation) (model.TrackMutationResult, error)

	// TrackPut atomically upserts the value, tag, and metric for one key.
	// A commit or transport error can leave the caller unsure whether it committed.
	TrackPut(space model.TenancySpace, bucketID int32, key string, value int64, tag *int64, metric *float64) error

	// TrackGet atomically reads one key's value from a consistent snapshot.
	TrackGet(space model.TenancySpace, bucketID int32, key string) (int64, error)

	// TrackDeleteMany atomically deletes the entire batch, including across buckets:
	// either all deletions commit or none do. No partial batch is committed.
	// A commit/transport error can leave the caller uncertain whether all or none
	// committed; an error does not necessarily mean nothing changed.
	TrackDeleteMany(space model.TenancySpace, items []model.TrackKey) error

	// TrackPutMany atomically upserts the entire batch, including across buckets:
	// either all updates commit or none do. No partial batch is committed.
	// A commit/transport error can leave the caller uncertain whether all or none
	// committed; an error does not necessarily mean nothing changed.
	TrackPutMany(space model.TenancySpace, items map[int32][]model.TrackKeyValueItem) error

	// TrackGetMany atomically reads all requested keys from one snapshot across
	// all buckets. Values and missing keys describe that same snapshot.
	TrackGetMany(space model.TenancySpace, keys map[int32][]string) (map[int32]map[string]model.TrackValue, map[int32][]string, error)

	// GetItemsByKeyPrefix atomically reads all matching items from one snapshot.
	GetItemsByKeyPrefix(
		space model.TenancySpace,
		bucketID int32,
		prefix string,
		tags []int64, // optional slice of tags
		metricValue *float64, // optional metric value
		metricIsGt bool, // "gt" or "lt"
	) ([]model.TrackKeyValueItem, error)

	// GetItemsByKeyPrefixes atomically reads all matching items from one snapshot
	// shared by every prefix, including when the query is split into chunks.
	GetItemsByKeyPrefixes(space model.TenancySpace,
		bucketID int32,
		prefixes []string,
		tags []int64,
		metricValue *float64,
		metricIsGt bool,
	) ([]model.TrackKeyValueItem, error)
}

type DepotStore interface {
	DepotCreate(space model.TenancySpace, bucketID int32, value string) (int64, error)
	DepotCreateMany(space model.TenancySpace, bucketID int32, values []string) ([]int64, error)

	DepotUpdate(space model.TenancySpace, id int64, value string) error

	DepotGet(space model.TenancySpace, id int64) (string, error)
	DepotGetMany(space model.TenancySpace, ids []int64) (map[int64]string, []int64, error)

	DepotDelete(space model.TenancySpace, id int64) error
	DepotDeleteByBucket(space model.TenancySpace, bucketID int32) error
	DepotGetAllByBucket(space model.TenancySpace, bucketID int32) (map[int64]string, error) //VX:Note this wants to become paginated at some point.
}

type GroveStore interface {
	// Single node operations
	CreateNode(space model.TenancySpace, treeID model.TreeID, node model.NodeID, parent *model.NodeID, position *model.ChildPosition, metadata *model.NodeMetadata) error
	DeleteNode(space model.TenancySpace, treeID model.TreeID, node model.NodeID, soft bool) error
	MoveNode(space model.TenancySpace, treeID model.TreeID, node model.NodeID, newParent *model.NodeID, newPosition *model.ChildPosition) error

	ApplyAggregateMutation(
		space model.TenancySpace,
		treeID model.TreeID,
		mutation model.MutationID,
		node model.NodeID,
		deltas model.AggregateDeltas,
	) error
	GetNodeLocalAggregates(space model.TenancySpace, treeID model.TreeID, node model.NodeID) (map[model.AggregateKey]model.AggregateValue, error)           // Node only
	GetNodeWithDescendantsAggregates(space model.TenancySpace, treeID model.TreeID, node model.NodeID) (map[model.AggregateKey]model.AggregateValue, error) // Node + all descendants
	GetNodeWithDescendantsAggregatesBulk(space model.TenancySpace, treeID model.TreeID, nodes []model.NodeID) (map[model.NodeID]map[model.AggregateKey]model.AggregateValue, []model.NodeID, error)

	Exists(space model.TenancySpace, treeID model.TreeID, node model.NodeID) (bool, error)
	GetNodeInfo(space model.TenancySpace, treeID model.TreeID, node model.NodeID) (*model.NodeInfo, error)
	GetChildren(space model.TenancySpace, treeID model.TreeID, node model.NodeID, pagination *model.PaginationParams) ([]model.NodeID, *model.PaginationResult, error)
	GetAncestors(space model.TenancySpace, treeID model.TreeID, node model.NodeID, pagination *model.PaginationParams) ([]model.NodeID, *model.PaginationResult, error)
	// GetAncestorsBulk returns ancestors for multiple nodes in a single call.
	// The returned map contains found nodes (key = node, value = ancestors ordered root-first).
	// The second return value lists node IDs that were not found.
	GetAncestorsBulk(space model.TenancySpace, treeID model.TreeID, nodes []model.NodeID) (map[model.NodeID][]model.NodeID, []model.NodeID, error)
	// GetNodeLocalAggregatesBulk returns local aggregates for multiple nodes in a single call.
	// The returned map contains found nodes (key = node, value = aggregates map).
	// The second return value lists node IDs that were not found.
	GetNodeLocalAggregatesBulk(space model.TenancySpace, treeID model.TreeID, nodes []model.NodeID) (map[model.NodeID]map[model.AggregateKey]model.AggregateValue, []model.NodeID, error)
	GetDescendants(space model.TenancySpace, treeID model.TreeID, node model.NodeID, opts *model.DescendantOptions) ([]model.NodeWithDepth, *model.PaginationResult, error)
}

type Store interface {
	WarehouseStore
	TrackStore
	DepotStore
	GroveStore
	LedgerStore
}
