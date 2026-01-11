package boltdb

import (
	"errors"

	"github.com/vixac/bullet/store/store_interface"
)

var ErrGroveNotImplemented = errors.New("grove operations not yet implemented for boltdb")

func (b *BoltStore) CreateNode(space store_interface.TenancySpace, node store_interface.NodeID, parent *store_interface.NodeID, position *store_interface.ChildPosition, metadata *store_interface.NodeMetadata) error {
	return ErrGroveNotImplemented
}

func (b *BoltStore) DeleteNode(space store_interface.TenancySpace, node store_interface.NodeID, soft bool) error {
	return ErrGroveNotImplemented
}

func (b *BoltStore) MoveNode(space store_interface.TenancySpace, node store_interface.NodeID, newParent *store_interface.NodeID, newPosition *store_interface.ChildPosition) error {
	return ErrGroveNotImplemented
}

func (b *BoltStore) Exists(space store_interface.TenancySpace, node store_interface.NodeID) (bool, error) {
	return false, ErrGroveNotImplemented
}

func (b *BoltStore) GetNodeInfo(space store_interface.TenancySpace, node store_interface.NodeID) (*store_interface.NodeInfo, error) {
	return nil, ErrGroveNotImplemented
}

func (b *BoltStore) GetChildren(space store_interface.TenancySpace, node store_interface.NodeID, pagination *store_interface.PaginationParams) ([]store_interface.NodeID, *store_interface.PaginationResult, error) {
	return nil, nil, ErrGroveNotImplemented
}

func (b *BoltStore) GetAncestors(space store_interface.TenancySpace, node store_interface.NodeID, pagination *store_interface.PaginationParams) ([]store_interface.NodeID, *store_interface.PaginationResult, error) {
	return nil, nil, ErrGroveNotImplemented
}

func (b *BoltStore) GetDescendants(space store_interface.TenancySpace, node store_interface.NodeID, opts *store_interface.DescendantOptions) ([]store_interface.NodeWithDepth, *store_interface.PaginationResult, error) {
	return nil, nil, ErrGroveNotImplemented
}

func (b *BoltStore) ApplyAggregateMutation(space store_interface.TenancySpace, mutation store_interface.MutationID, node store_interface.NodeID, deltas store_interface.AggregateDeltas) error {
	return ErrGroveNotImplemented
}

func (b *BoltStore) GetNodeLocalAggregates(space store_interface.TenancySpace, node store_interface.NodeID) (map[store_interface.AggregateKey]store_interface.AggregateValue, error) {
	return nil, ErrGroveNotImplemented
}

func (b *BoltStore) GetNodeWithDescendantsAggregates(space store_interface.TenancySpace, node store_interface.NodeID) (map[store_interface.AggregateKey]store_interface.AggregateValue, error) {
	return nil, ErrGroveNotImplemented
}
