package boltdb

import (
	"context"

	"github.com/vixac/bullet/model"
)

func (s *BoltStore) WarehousePut(context.Context, model.TenancySpace, model.PutBlobRequest) (model.Blob, error) {
	return model.Blob{}, model.ErrWarehouseUnsupported
}
func (s *BoltStore) WarehouseGet(context.Context, model.TenancySpace, model.BlobID) (model.Blob, error) {
	return model.Blob{}, model.ErrWarehouseUnsupported
}
func (s *BoltStore) WarehouseGetMany(context.Context, model.TenancySpace, []model.BlobID) (map[model.BlobID]model.Blob, error) {
	return nil, model.ErrWarehouseUnsupported
}
