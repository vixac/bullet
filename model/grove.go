package model

import (
	"errors"
)

// Grove types
type TreeID string
type NodeID string
type AggregateKey string
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
