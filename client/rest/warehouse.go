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
