// Package local binds a Bullet store to one immutable tenancy space.
package local

import (
	"context"
	"github.com/vixac/bullet/client"
	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/store/store_interface"
)

type Client struct {
	store store_interface.Store
	space model.TenancySpace
}

var _ client.Client = (*Client)(nil)

// New binds all operations to space. The caller retains ownership of store.
func New(store store_interface.Store, space model.TenancySpace) *Client {
	return &Client{store: store, space: space}
}
func (c *Client) LedgerAppend(ledgerID model.LedgerID, appendID model.LedgerAppendID, payload string) (model.LedgerRecord, error) {
	return c.store.LedgerAppend(c.space, ledgerID, appendID, payload)
}
func (c *Client) LedgerAppendMany(ledgerID model.LedgerID, items []model.LedgerAppendItem) ([]model.LedgerRecord, error) {
	return c.store.LedgerAppendMany(c.space, ledgerID, items)
}
func (c *Client) LedgerReadBackward(selector model.LedgerSelector, cursor *string, limit int) (model.LedgerPage, error) {
	return c.store.LedgerReadBackward(c.space, selector, cursor, limit)
}
func (c *Client) LedgerReadForward(selector model.LedgerSelector, after model.LedgerPosition, through *model.LedgerPosition, limit int) ([]model.LedgerRecord, error) {
	return c.store.LedgerReadForward(c.space, selector, after, through, limit)
}
func (c *Client) TrackMutate(req model.TrackMutation) (model.TrackMutationResult, error) {
	return c.store.TrackMutate(c.space, req)
}
func (c *Client) TrackPut(bucketID int32, key string, value model.TrackValue) error {
	return c.store.TrackPut(c.space, bucketID, key, value)
}
func (c *Client) TrackGet(bucketID int32, key string, opts model.TrackReadOptions) (model.TrackValue, error) {
	return c.store.TrackGet(c.space, bucketID, key, opts)
}
func (c *Client) TrackDeleteMany(items []model.TrackKey) error {
	return c.store.TrackDeleteMany(c.space, items)
}
func (c *Client) TrackPutMany(items map[int32][]model.TrackKeyValueItem) error {
	return c.store.TrackPutMany(c.space, items)
}
func (c *Client) TrackGetMany(keys map[int32][]string, opts model.TrackReadOptions) (map[int32]map[string]model.TrackValue, map[int32][]string, error) {
	return c.store.TrackGetMany(c.space, keys, opts)
}
func (c *Client) GetItemsByKeyPrefix(bucketID int32, prefix string, tags []int64, metricValue *float64, metricIsGt bool) ([]model.TrackKeyValueItem, error) {
	return c.store.GetItemsByKeyPrefix(c.space, bucketID, prefix, tags, metricValue, metricIsGt)
}
func (c *Client) GetItemsByKeyPrefixes(bucketID int32, prefixes []string, tags []int64, metricValue *float64, metricIsGt bool) ([]model.TrackKeyValueItem, error) {
	return c.store.GetItemsByKeyPrefixes(c.space, bucketID, prefixes, tags, metricValue, metricIsGt)
}
func (c *Client) DepotCreate(bucketID int32, value string) (int64, error) {
	return c.store.DepotCreate(c.space, bucketID, value)
}
func (c *Client) DepotCreateMany(bucketID int32, values []string) ([]int64, error) {
	return c.store.DepotCreateMany(c.space, bucketID, values)
}
func (c *Client) DepotUpdate(id int64, value string) error {
	return c.store.DepotUpdate(c.space, id, value)
}
func (c *Client) DepotGet(id int64) (string, error) { return c.store.DepotGet(c.space, id) }
func (c *Client) DepotGetMany(ids []int64) (map[int64]string, []int64, error) {
	return c.store.DepotGetMany(c.space, ids)
}
func (c *Client) DepotDelete(id int64) error { return c.store.DepotDelete(c.space, id) }
func (c *Client) DepotDeleteByBucket(bucketID int32) error {
	return c.store.DepotDeleteByBucket(c.space, bucketID)
}
func (c *Client) DepotGetAllByBucket(bucketID int32) (map[int64]string, error) {
	return c.store.DepotGetAllByBucket(c.space, bucketID)
}
func (c *Client) CreateNode(treeID model.TreeID, node model.NodeID, parent *model.NodeID, position *model.ChildPosition, metadata *model.NodeMetadata) error {
	return c.store.CreateNode(c.space, treeID, node, parent, position, metadata)
}
func (c *Client) DeleteNode(treeID model.TreeID, node model.NodeID, soft bool) error {
	return c.store.DeleteNode(c.space, treeID, node, soft)
}
func (c *Client) MoveNode(treeID model.TreeID, node model.NodeID, newParent *model.NodeID, newPosition *model.ChildPosition) error {
	return c.store.MoveNode(c.space, treeID, node, newParent, newPosition)
}
func (c *Client) ApplyAggregateMutation(treeID model.TreeID, mutation model.MutationID, node model.NodeID, deltas model.AggregateDeltas) error {
	return c.store.ApplyAggregateMutation(c.space, treeID, mutation, node, deltas)
}
func (c *Client) GetNodeLocalAggregates(treeID model.TreeID, node model.NodeID) (map[model.AggregateKey]model.AggregateValue, error) {
	return c.store.GetNodeLocalAggregates(c.space, treeID, node)
}
func (c *Client) GetNodeWithDescendantsAggregates(treeID model.TreeID, node model.NodeID) (map[model.AggregateKey]model.AggregateValue, error) {
	return c.store.GetNodeWithDescendantsAggregates(c.space, treeID, node)
}
func (c *Client) GetNodeWithDescendantsAggregatesBulk(treeID model.TreeID, nodes []model.NodeID) (map[model.NodeID]map[model.AggregateKey]model.AggregateValue, []model.NodeID, error) {
	return c.store.GetNodeWithDescendantsAggregatesBulk(c.space, treeID, nodes)
}
func (c *Client) Exists(treeID model.TreeID, node model.NodeID) (bool, error) {
	return c.store.Exists(c.space, treeID, node)
}
func (c *Client) GetNodeInfo(treeID model.TreeID, node model.NodeID) (*model.NodeInfo, error) {
	return c.store.GetNodeInfo(c.space, treeID, node)
}
func (c *Client) GetChildren(treeID model.TreeID, node model.NodeID, pagination *model.PaginationParams) ([]model.NodeID, *model.PaginationResult, error) {
	return c.store.GetChildren(c.space, treeID, node, pagination)
}
func (c *Client) GetAncestors(treeID model.TreeID, node model.NodeID, pagination *model.PaginationParams) ([]model.NodeID, *model.PaginationResult, error) {
	return c.store.GetAncestors(c.space, treeID, node, pagination)
}
func (c *Client) GetAncestorsBulk(treeID model.TreeID, nodes []model.NodeID) (map[model.NodeID][]model.NodeID, []model.NodeID, error) {
	return c.store.GetAncestorsBulk(c.space, treeID, nodes)
}
func (c *Client) GetNodeLocalAggregatesBulk(treeID model.TreeID, nodes []model.NodeID) (map[model.NodeID]map[model.AggregateKey]model.AggregateValue, []model.NodeID, error) {
	return c.store.GetNodeLocalAggregatesBulk(c.space, treeID, nodes)
}
func (c *Client) GetDescendants(treeID model.TreeID, node model.NodeID, opts *model.DescendantOptions) ([]model.NodeWithDepth, *model.PaginationResult, error) {
	return c.store.GetDescendants(c.space, treeID, node, opts)
}
func (c *Client) WarehousePut(arg0 context.Context, arg1 model.PutBlobRequest) (model.Blob, error) {
	return c.store.WarehousePut(arg0, c.space, arg1)
}
func (c *Client) WarehouseGet(arg0 context.Context, arg1 model.BlobID) (model.Blob, error) {
	return c.store.WarehouseGet(arg0, c.space, arg1)
}
func (c *Client) WarehouseGetMany(arg0 context.Context, arg1 []model.BlobID) (map[model.BlobID]model.Blob, error) {
	return c.store.WarehouseGetMany(arg0, c.space, arg1)
}
func (c *Client) WarehousePutCheckpoint(arg0 context.Context, arg1 model.PutCheckpointRequest) (model.CheckpointRef, error) {
	return c.store.WarehousePutCheckpoint(arg0, c.space, arg1)
}
func (c *Client) WarehouseGetLatestCheckpoint(arg0 context.Context, arg1 model.CheckpointSequenceID) (*model.CheckpointRef, error) {
	return c.store.WarehouseGetLatestCheckpoint(arg0, c.space, arg1)
}
func (c *Client) WarehouseFindCheckpoints(arg0 context.Context, arg1 model.CheckpointSequenceID, arg2 model.LedgerPosition, arg3 int) ([]model.CheckpointRef, error) {
	return c.store.WarehouseFindCheckpoints(arg0, c.space, arg1, arg2, arg3)
}
func (c *Client) WarehouseGetCheckpoint(arg0 context.Context, arg1 model.CheckpointID) (model.Checkpoint, error) {
	return c.store.WarehouseGetCheckpoint(arg0, c.space, arg1)
}
func (c *Client) WarehouseMarkCheckpointCorrupt(arg0 context.Context, arg1 model.CheckpointID) error {
	return c.store.WarehouseMarkCheckpointCorrupt(arg0, c.space, arg1)
}
