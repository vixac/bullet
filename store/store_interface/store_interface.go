package store_interface

import (
	"errors"

	"github.com/vixac/bullet/model"
)

type TenancySpace struct {
	AppId     int32
	TenancyId int64
}
type TrackStore interface {
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
	DepotPut(space TenancySpace, key int64, value string) error
	DepotGet(space TenancySpace, key int64) (string, error)
	DepotDelete(space TenancySpace, key int64) error
	DepotPutMany(space TenancySpace, items []model.DepotKeyValueItem) error
	DepotGetMany(space TenancySpace, keys []int64) (map[int64]string, []int64, error)
}

// using its own ids, wayfinder uses track and depot to provide a query to payload interface.
type WayFinderStore interface {
	WayFinderPut(space TenancySpace, bucketID int32, key string, payload string, tag *int64, metric *float64) (int64, error)
	WayFinderGetByPrefix(space TenancySpace, bucketID int32,
		prefix string,
		tags []int64, // optional slice of tags
		metricValue *float64, // optional metric value
		metricIsGt bool, // "gt" or "lt"
	) ([]model.WayFinderQueryItem, error)

	WayFinderGetOne(space TenancySpace, bucketID int32, key string) (*model.WayFinderGetResponse, error)
}

// Grove types
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
	CreateNode(space TenancySpace, node NodeID, parent *NodeID, position *ChildPosition, metadata *NodeMetadata) error
	DeleteNode(space TenancySpace, node NodeID, soft bool) error
	MoveNode(space TenancySpace, node NodeID, newParent *NodeID, newPosition *ChildPosition) error
	//TODO: Restore	RestoreNode(space TenancySpace, node NodeID) error

	/*
		// Batch operations
		CreateNodes(space TenancySpace, nodes []NodeCreation) error
		DeleteNodes(space TenancySpace, nodes []NodeID, soft bool) error
		MoveNodes(space TenancySpace, moves []NodeMove) error
		ExistsMany(space TenancySpace, nodes []NodeID) (map[NodeID]bool, error)
	*/
	// Node queries
	Exists(space TenancySpace, node NodeID) (bool, error)
	GetNodeInfo(space TenancySpace, node NodeID) (*NodeInfo, error)
	//GetParent(space TenancySpace, node NodeID) (*NodeID, error)

	// Child ordering
	//TODO: Restore	ReorderChild(space TenancySpace, node NodeID, newPosition ChildPosition) error

	// Tree traversal (with pagination)
	GetChildren(space TenancySpace, node NodeID, pagination *PaginationParams) ([]NodeID, *PaginationResult, error)
	GetAncestors(space TenancySpace, node NodeID, pagination *PaginationParams) ([]NodeID, *PaginationResult, error)
	GetDescendants(space TenancySpace, node NodeID, opts *DescendantOptions) ([]NodeWithDepth, *PaginationResult, error)

	/*
		// Path and relationship queries
		GetPath(space TenancySpace, node NodeID) ([]NodeID, error) // Path from root to node
		GetDepth(space TenancySpace, node NodeID) (int, error)     // Depth from root
		IsAncestor(space TenancySpace, ancestor NodeID, descendant NodeID) (bool, error)
	*/
	// Advanced queries
	//	FindNodes(space TenancySpace, filter NodeFilter, pagination *PaginationParams) ([]NodeInfo, *PaginationResult, error)
	//ListDeleted(space TenancySpace, pagination *PaginationParams) ([]NodeID, *PaginationResult, error)

	// Metadata operations
	//	GetNodeMetadata(space TenancySpace, node NodeID) (*NodeMetadata, error)
	//	UpdateNodeMetadata(space TenancySpace, node NodeID, metadata NodeMetadata) error

	// Aggregates (clarified semantics)
	ApplyAggregateMutation(
		space TenancySpace,
		mutation MutationID,
		node NodeID,
		deltas AggregateDeltas,
	) error
	GetNodeLocalAggregates(space TenancySpace, node NodeID) (map[AggregateKey]AggregateValue, error)           // Node only
	GetNodeWithDescendantsAggregates(space TenancySpace, node NodeID) (map[AggregateKey]AggregateValue, error) // Node + all descendants

	// Statistics
	//	GetTreeStats(space TenancySpace, root NodeID) (*TreeStats, error)

	//Not needed yet
	/*
	   RegisterAggregate(key AggregateKey) error
	   UnregisterAggregate(key AggregateKey) error
	   ListAggregates() ([]AggregateKey, error)
	*/
}

type Store interface {
	TrackStore
	DepotStore
	WayFinderStore
	GroveStore
}
