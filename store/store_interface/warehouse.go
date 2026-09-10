package store_interface

import (
	"context"

	"github.com/vixac/bullet/model"
)

// WarehouseStore stores immutable blobs. Retrying a model.PutID with identical bytes
// and metadata returns the original blob; different bytes or metadata conflict.
// Get returns model.ErrBlobNotFound for missing IDs. GetMany omits missing IDs and
// returns a non-nil map, including for an empty request. Values are caller-owned.
type WarehouseStore interface {
	WarehousePut(context.Context, model.TenancySpace, model.PutBlobRequest) (model.Blob, error)
	WarehouseGet(context.Context, model.TenancySpace, model.BlobID) (model.Blob, error)
	WarehouseGetMany(context.Context, model.TenancySpace, []model.BlobID) (map[model.BlobID]model.Blob, error)
}
