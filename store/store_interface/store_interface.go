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

	// Aggregates (clarified semantics)
	ApplyAggregateMutation(
		space TenancySpace,
		treeID TreeID,
		mutation MutationID,
		node NodeID,
		deltas AggregateDeltas,
	) error
	GetNodeLocalAggregates(space TenancySpace, treeID TreeID, node NodeID) (map[AggregateKey]AggregateValue, error)           // Node only
	GetNodeWithDescendantsAggregates(space TenancySpace, treeID TreeID, node NodeID) (map[AggregateKey]AggregateValue, error) // Node + all descendants
	Exists(space TenancySpace, treeID TreeID, node NodeID) (bool, error)
	GetNodeInfo(space TenancySpace, treeID TreeID, node NodeID) (*NodeInfo, error)
	GetChildren(space TenancySpace, treeID TreeID, node NodeID, pagination *PaginationParams) ([]NodeID, *PaginationResult, error)
	GetAncestors(space TenancySpace, treeID TreeID, node NodeID, pagination *PaginationParams) ([]NodeID, *PaginationResult, error)
	GetDescendants(space TenancySpace, treeID TreeID, node NodeID, opts *DescendantOptions) ([]NodeWithDepth, *PaginationResult, error)

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
}

type Store interface {
	TrackStore
	DepotStore
	WayFinderStore
	GroveStore
}
