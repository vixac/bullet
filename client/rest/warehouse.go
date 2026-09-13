package rest

import (
	"context"
	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/protocol"
	"net/http"
	"net/url"
)

func (c *Client) WarehousePut(ctx context.Context, req model.PutBlobRequest) (model.Blob, error) {
	var r protocol.Blob
	err := c.do(ctx, http.MethodPost, "/warehouse/blobs", protocol.PutBlobRequestFromModel(req), &r, http.StatusOK)
	if err != nil {
		return model.Blob{}, err
	}
	return r.Model(), nil
}
func (c *Client) WarehouseGet(ctx context.Context, id model.BlobID) (model.Blob, error) {
	if err := ctx.Err(); err != nil {
		return model.Blob{}, err
	}
	if id == "" {
		return model.Blob{}, model.ErrBlobNotFound
	}
	var r protocol.Blob
	err := c.do(ctx, http.MethodGet, "/warehouse/blobs/"+url.PathEscape(string(id)), nil, &r, http.StatusOK)
	if err != nil {
		return model.Blob{}, err
	}
	return r.Model(), nil
}
func (c *Client) WarehouseGetMany(ctx context.Context, ids []model.BlobID) (map[model.BlobID]model.Blob, error) {
	var r protocol.WarehouseGetManyResponse
	if err := c.do(ctx, http.MethodPost, "/warehouse/blobs/batch-get", protocol.WarehouseGetManyRequest{IDs: ids}, &r, http.StatusOK); err != nil {
		return nil, err
	}
	result := make(map[model.BlobID]model.Blob, len(r))
	for id, b := range r {
		result[id] = b.Model()
	}
	return result, nil
}

// Checkpoint HTTP routes have not been added yet. Keep the REST client aligned
// with the public Warehouse interface while returning the capability error.
func (c *Client) WarehousePutCheckpoint(context.Context, model.PutCheckpointRequest) (model.CheckpointRef, error) {
	return model.CheckpointRef{}, model.ErrCheckpointUnsupported
}
func (c *Client) WarehouseGetLatestCheckpoint(context.Context, model.CheckpointSequenceID) (*model.CheckpointRef, error) {
	return nil, model.ErrCheckpointUnsupported
}
func (c *Client) WarehouseFindCheckpoints(context.Context, model.CheckpointSequenceID, model.LedgerPosition, int) ([]model.CheckpointRef, error) {
	return nil, model.ErrCheckpointUnsupported
}
func (c *Client) WarehouseGetCheckpoint(context.Context, model.CheckpointID) (model.Checkpoint, error) {
	return model.Checkpoint{}, model.ErrCheckpointUnsupported
}
func (c *Client) WarehouseMarkCheckpointCorrupt(context.Context, model.CheckpointID) error {
	return model.ErrCheckpointUnsupported
}
