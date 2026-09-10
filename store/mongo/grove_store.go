package mongodb

import (
	"errors"

	"github.com/vixac/bullet/model"
)

var ErrGroveNotImplemented = errors.New("grove operations not yet implemented for mongodb")

func (m *MongoStore) CreateNode(space model.TenancySpace, treeID model.TreeID, node model.NodeID, parent *model.NodeID, position *model.ChildPosition, metadata *model.NodeMetadata) error {
	return ErrGroveNotImplemented
}

func (m *MongoStore) DeleteNode(space model.TenancySpace, treeID model.TreeID, node model.NodeID, soft bool) error {
	return ErrGroveNotImplemented
}

func (m *MongoStore) MoveNode(space model.TenancySpace, treeID model.TreeID, node model.NodeID, newParent *model.NodeID, newPosition *model.ChildPosition) error {
	return ErrGroveNotImplemented
}

func (m *MongoStore) Exists(space model.TenancySpace, treeID model.TreeID, node model.NodeID) (bool, error) {
	return false, ErrGroveNotImplemented
}

func (m *MongoStore) GetNodeInfo(space model.TenancySpace, treeID model.TreeID, node model.NodeID) (*model.NodeInfo, error) {
	return nil, ErrGroveNotImplemented
}

func (m *MongoStore) GetChildren(space model.TenancySpace, treeID model.TreeID, node model.NodeID, pagination *model.PaginationParams) ([]model.NodeID, *model.PaginationResult, error) {
	return nil, nil, ErrGroveNotImplemented
}

func (m *MongoStore) GetAncestors(space model.TenancySpace, treeID model.TreeID, node model.NodeID, pagination *model.PaginationParams) ([]model.NodeID, *model.PaginationResult, error) {
	return nil, nil, ErrGroveNotImplemented
}

func (m *MongoStore) GetAncestorsBulk(space model.TenancySpace, treeID model.TreeID, nodes []model.NodeID) (map[model.NodeID][]model.NodeID, []model.NodeID, error) {
	return nil, nil, ErrGroveNotImplemented
}

func (m *MongoStore) GetDescendants(space model.TenancySpace, treeID model.TreeID, node model.NodeID, opts *model.DescendantOptions) ([]model.NodeWithDepth, *model.PaginationResult, error) {
	return nil, nil, ErrGroveNotImplemented
}

func (m *MongoStore) ApplyAggregateMutation(space model.TenancySpace, treeID model.TreeID, mutation model.MutationID, node model.NodeID, deltas model.AggregateDeltas) error {
	return ErrGroveNotImplemented
}

func (m *MongoStore) GetNodeLocalAggregates(space model.TenancySpace, treeID model.TreeID, node model.NodeID) (map[model.AggregateKey]model.AggregateValue, error) {
	return nil, ErrGroveNotImplemented
}

func (m *MongoStore) GetNodeWithDescendantsAggregates(space model.TenancySpace, treeID model.TreeID, node model.NodeID) (map[model.AggregateKey]model.AggregateValue, error) {
	return nil, ErrGroveNotImplemented
}

func (m *MongoStore) GetNodeLocalAggregatesBulk(space model.TenancySpace, treeID model.TreeID, nodes []model.NodeID) (map[model.NodeID]map[model.AggregateKey]model.AggregateValue, []model.NodeID, error) {
	return nil, nil, ErrGroveNotImplemented
}

func (m *MongoStore) GetNodeWithDescendantsAggregatesBulk(space model.TenancySpace, treeID model.TreeID, nodes []model.NodeID) (map[model.NodeID]map[model.AggregateKey]model.AggregateValue, []model.NodeID, error) {
	return nil, nil, ErrGroveNotImplemented
}
