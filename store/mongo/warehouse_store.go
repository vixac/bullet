package mongodb

import (
	"context"

	"github.com/vixac/bullet/model"
)

func (s *MongoStore) WarehousePut(context.Context, model.TenancySpace, model.PutBlobRequest) (model.Blob, error) {
	return model.Blob{}, model.ErrWarehouseUnsupported
}
func (s *MongoStore) WarehouseGet(context.Context, model.TenancySpace, model.BlobID) (model.Blob, error) {
	return model.Blob{}, model.ErrWarehouseUnsupported
}
func (s *MongoStore) WarehouseGetMany(context.Context, model.TenancySpace, []model.BlobID) (map[model.BlobID]model.Blob, error) {
	return nil, model.ErrWarehouseUnsupported
}
