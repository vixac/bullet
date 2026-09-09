package sqlite_store

import (
	"context"
	si "github.com/vixac/bullet/store/store_interface"
)

func (s *SQLiteStore) WarehousePut(context.Context, si.TenancySpace, si.PutBlobRequest) (si.Blob, error) {
	return si.Blob{}, si.ErrWarehouseUnsupported
}
func (s *SQLiteStore) WarehouseGet(context.Context, si.TenancySpace, si.BlobID) (si.Blob, error) {
	return si.Blob{}, si.ErrWarehouseUnsupported
}
func (s *SQLiteStore) WarehouseGetMany(context.Context, si.TenancySpace, []si.BlobID) (map[si.BlobID]si.Blob, error) {
	return nil, si.ErrWarehouseUnsupported
}
