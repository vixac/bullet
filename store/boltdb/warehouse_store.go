package boltdb

import (
	"context"
	si "github.com/vixac/bullet/store/store_interface"
)

func (s *BoltStore) WarehousePut(context.Context, si.TenancySpace, si.PutBlobRequest) (si.Blob, error) {
	return si.Blob{}, si.ErrWarehouseUnsupported
}
func (s *BoltStore) WarehouseGet(context.Context, si.TenancySpace, si.BlobID) (si.Blob, error) {
	return si.Blob{}, si.ErrWarehouseUnsupported
}
func (s *BoltStore) WarehouseGetMany(context.Context, si.TenancySpace, []si.BlobID) (map[si.BlobID]si.Blob, error) {
	return nil, si.ErrWarehouseUnsupported
}
