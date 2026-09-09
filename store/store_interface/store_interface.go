package store_interface

import (
	"errors"
	"time"

	"github.com/vixac/bullet/model"
)

const (
	LedgerMaxPayloadBytes = 256 * 1024
	LedgerMaxPageSize     = 1000
	LedgerMaxSelected     = 100
)

type LedgerID string
type LedgerAppendID string
type LedgerPosition int64

type LedgerAppendItem struct {
	AppendID LedgerAppendID
	Payload  string
}

type LedgerRecord struct {
	LedgerID  LedgerID
	Position  LedgerPosition
	AppendID  LedgerAppendID
	CreatedAt time.Time
	Payload   string
}

// LedgerSelector selects either all ledgers in a tenancy space or an explicit
// set of ledgers. Exactly one of All and LedgerIDs must be supplied.
type LedgerSelector struct {
	All       bool
	LedgerIDs []LedgerID
}

type LedgerPage struct {
	Records    []LedgerRecord
	NextCursor *string
}

var (
	ErrLedgerUnsupported     = errors.New("ledger is not supported by this store")
	ErrLedgerInvalidID       = errors.New("invalid ledger id")
	ErrLedgerInvalidAppendID = errors.New("invalid ledger append id")
	ErrLedgerPayloadTooLarge = errors.New("ledger payload is too large")
	ErrLedgerAppendConflict  = errors.New("ledger append id already exists with a different payload")
	ErrLedgerBatchConflict   = errors.New("ledger batch mixes existing and new append ids")
	ErrLedgerInvalidSelector = errors.New("invalid ledger selector")
	ErrLedgerInvalidPageSize = errors.New("invalid ledger page size")
	ErrLedgerInvalidCursor   = errors.New("invalid ledger cursor")
)

type LedgerStore interface {
	LedgerAppend(space TenancySpace, ledgerID LedgerID, appendID LedgerAppendID, payload string) (LedgerRecord, error)
	LedgerAppendMany(space TenancySpace, ledgerID LedgerID, items []LedgerAppendItem) ([]LedgerRecord, error)
	LedgerReadBackward(space TenancySpace, selector LedgerSelector, cursor *string, limit int) (LedgerPage, error)
	// LedgerReadForward returns records in ascending position order. after is
	// exclusive. A nil through position makes the read live; otherwise through
	// is an inclusive upper boundary.
	LedgerReadForward(space TenancySpace, selector LedgerSelector, after LedgerPosition, through *LedgerPosition, limit int) ([]LedgerRecord, error)
	LedgerDelete(space TenancySpace, ledgerID LedgerID) error
}

type TenancySpace struct {
	AppId     int32
	TenancyId int64
}

type TrackKey struct {
	Space    TenancySpace
	BucketID int32
	Key      string
}

type TrackPut struct {
	Space    TenancySpace
	BucketID int32
	Key      string
	Value    int64
	Tag      *int64
	Metric   *float64
}

type TrackMutation struct {
	MutationID MutationID
	Puts       []TrackPut
	Deletes    []TrackKey
}

type TrackMutationResult struct {
	Applied bool
}

type TrackClientInterface interface {
	TrackMutate(req TrackMutation) (TrackMutationResult, error)
}

var ErrTrackMutationUnsupported = errors.New("track mutations are not supported by this store")

type TrackStore interface {
	TrackClientInterface
	TrackPut(space TenancySpace, bucketID int32, key string, value int64, tag *int64, metric *float64) error
	TrackGet(space TenancySpace, bucketID int32, key string) (int64, error)

	TrackDeleteMany(space TenancySpace, items []model.TrackBucketKeyPair) error
	TrackClose() error
	TrackPutMany(space TenancySpace, items map[int32][]model.TrackKeyValueItem) error
	TrackGetMany(space TenancySpace, keys map[int32][]string) (map[int32]map[string]model.TrackValue, map[int32][]string, error)
	GetItemsByKeyPrefix(
		space TenancySpace,
		bucketID int32,
		prefix string,
		tags []int64, // optional slice of tags
		metricValue *float64, // optional metric value
		metricIsGt bool, // "gt" or "lt"
	) ([]model.TrackKeyValueItem, error)

	//Slower. Advisable to keep the number of prefix strings < 30 as it is implemented via  $or clause
	GetItemsByKeyPrefixes(space TenancySpace,
		bucketID int32,
		prefixes []string,
		tags []int64,
		metricValue *float64,
		metricIsGt bool,
	) ([]model.TrackKeyValueItem, error)
}

type DepotStore interface {
	DepotCreate(space TenancySpace, bucketID int32, value string) (int64, error)
	DepotCreateMany(space TenancySpace, bucketID int32, values []string) ([]int64, error)

	DepotUpdate(space TenancySpace, id int64, value string) error

	DepotGet(space TenancySpace, id int64) (string, error)
	DepotGetMany(space TenancySpace, ids []int64) (map[int64]string, []int64, error)

	DepotDelete(space TenancySpace, id int64) error
	DepotDeleteByBucket(space TenancySpace, bucketID int32) error
	DepotGetAllByBucket(space TenancySpace, bucketID int32) (map[int64]string, error) //VX:Note this wants to become paginated at some point.
}

// Grove types
type TreeID string
type NodeID string
type AggregateKey string
type MutationID string
type AggregateValue int64
type AggregateDeltas map[AggregateKey]AggregateValue
type ChildPosition float64
type NodeMetadata map[string]interface{}
type TransactionID string

// Pagination
type PaginationParams struct {
	Limit  int
	Cursor *string // Cursor-based pagination for better performance at scale
}

type PaginationResult struct {
	NextCursor *string
}

// Node structures
type NodeCreation struct {
	NodeID   NodeID
	Parent   *NodeID
	Position *ChildPosition
	Metadata *NodeMetadata
}

type NodeMove struct {
	NodeID      NodeID
	NewParent   *NodeID
	NewPosition *ChildPosition
}

type NodeInfo struct {
	ID       NodeID
	Parent   *NodeID
	Position *ChildPosition
	Depth    int // Absolute depth from tree root (root = 0)
	Metadata *NodeMetadata
}

type NodeWithDepth struct {
	NodeID NodeID
	Depth  int // Relative depth from query node (query node = 0, children = 1, etc.)
}

// Query options
type DescendantOptions struct {
	MaxDepth     *int
	IncludeDepth bool // Return depth info with each node
	BreadthFirst bool // false = depth-first (default)
	Pagination   *PaginationParams
}

type NodeFilter struct {
	MetadataFilters map[string]interface{} // Key-value filters for metadata
	MinDepth        *int
	MaxDepth        *int
}

// Statistics
type TreeStats struct {
	TotalNodes         int64
	MaxDepth           int
	AvgBranchingFactor float64
	TotalLeaves        int64
}

var (
	ErrNodeNotFound      = errors.New("node not found")
	ErrNodeAlreadyExists = errors.New("node already exists")
	ErrCycleDetected     = errors.New("cycle detected")
	ErrMutationConflict  = errors.New("mutation already applied")
	ErrInvalidPosition   = errors.New("invalid child position")
	ErrInvalidFilter     = errors.New("invalid node filter")
)

type GroveStore interface {
	// Single node operations
	CreateNode(space TenancySpace, treeID TreeID, node NodeID, parent *NodeID, position *ChildPosition, metadata *NodeMetadata) error
	DeleteNode(space TenancySpace, treeID TreeID, node NodeID, soft bool) error
	MoveNode(space TenancySpace, treeID TreeID, node NodeID, newParent *NodeID, newPosition *ChildPosition) error

	ApplyAggregateMutation(
		space TenancySpace,
		treeID TreeID,
		mutation MutationID,
		node NodeID,
		deltas AggregateDeltas,
	) error
	GetNodeLocalAggregates(space TenancySpace, treeID TreeID, node NodeID) (map[AggregateKey]AggregateValue, error)           // Node only
	GetNodeWithDescendantsAggregates(space TenancySpace, treeID TreeID, node NodeID) (map[AggregateKey]AggregateValue, error) // Node + all descendants
	GetNodeWithDescendantsAggregatesBulk(space TenancySpace, treeID TreeID, nodes []NodeID) (map[NodeID]map[AggregateKey]AggregateValue, []NodeID, error)

	Exists(space TenancySpace, treeID TreeID, node NodeID) (bool, error)
	GetNodeInfo(space TenancySpace, treeID TreeID, node NodeID) (*NodeInfo, error)
	GetChildren(space TenancySpace, treeID TreeID, node NodeID, pagination *PaginationParams) ([]NodeID, *PaginationResult, error)
	GetAncestors(space TenancySpace, treeID TreeID, node NodeID, pagination *PaginationParams) ([]NodeID, *PaginationResult, error)
	// GetAncestorsBulk returns ancestors for multiple nodes in a single call.
	// The returned map contains found nodes (key = node, value = ancestors ordered root-first).
	// The second return value lists node IDs that were not found.
	GetAncestorsBulk(space TenancySpace, treeID TreeID, nodes []NodeID) (map[NodeID][]NodeID, []NodeID, error)
	// GetNodeLocalAggregatesBulk returns local aggregates for multiple nodes in a single call.
	// The returned map contains found nodes (key = node, value = aggregates map).
	// The second return value lists node IDs that were not found.
	GetNodeLocalAggregatesBulk(space TenancySpace, treeID TreeID, nodes []NodeID) (map[NodeID]map[AggregateKey]AggregateValue, []NodeID, error)
	GetDescendants(space TenancySpace, treeID TreeID, node NodeID, opts *DescendantOptions) ([]NodeWithDepth, *PaginationResult, error)
}

type Store interface {
	TrackStore
	DepotStore
	GroveStore
	LedgerStore
}

//Some extra Grove ideas

//TODO: Restore	RestoreNode(space TenancySpace, treeID TreeID, node NodeID) error

/*
	// Batch operations
	CreateNodes(space TenancySpace, treeID TreeID, nodes []NodeCreation) error
	DeleteNodes(space TenancySpace, treeID TreeID, nodes []NodeID, soft bool) error
	MoveNodes(space TenancySpace, treeID TreeID, moves []NodeMove) error
	ExistsMany(space TenancySpace, treeID TreeID, nodes []NodeID) (map[NodeID]bool, error)
*/
// Node queries

//GetParent(space TenancySpace, treeID TreeID, node NodeID) (*NodeID, error)

// Child ordering
//TODO: Restore	ReorderChild(space TenancySpace, treeID TreeID, node NodeID, newPosition ChildPosition) error

// Tree traversal (with pagination)

/*
	// Path and relationship queries
	GetPath(space TenancySpace, treeID TreeID, node NodeID) ([]NodeID, error) // Path from root to node
	GetDepth(space TenancySpace, treeID TreeID, node NodeID) (int, error)     // Depth from root
	IsAncestor(space TenancySpace, treeID TreeID, ancestor NodeID, descendant NodeID) (bool, error)
*/
// Advanced queries
//	FindNodes(space TenancySpace, treeID TreeID, filter NodeFilter, pagination *PaginationParams) ([]NodeInfo, *PaginationResult, error)
//ListDeleted(space TenancySpace, treeID TreeID, pagination *PaginationParams) ([]NodeID, *PaginationResult, error)

// Metadata operations
//	GetNodeMetadata(space TenancySpace, treeID TreeID, node NodeID) (*NodeMetadata, error)
//	UpdateNodeMetadata(space TenancySpace, treeID TreeID, node NodeID, metadata NodeMetadata) error
// Statistics
//	GetTreeStats(space TenancySpace, treeID TreeID, root NodeID) (*TreeStats, error)

//Not needed yet
/*
   RegisterAggregate(key AggregateKey) error
   UnregisterAggregate(key AggregateKey) error
   ListAggregates() ([]AggregateKey, error)
*/
