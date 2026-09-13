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
func (s *BoltStore) WarehousePutCheckpoint(context.Context, model.TenancySpace, model.PutCheckpointRequest) (model.CheckpointRef, error) {
	return model.CheckpointRef{}, model.ErrCheckpointUnsupported
}
func (s *BoltStore) WarehouseGetLatestCheckpoint(context.Context, model.TenancySpace, model.CheckpointSequenceID) (*model.CheckpointRef, error) {
	return nil, model.ErrCheckpointUnsupported
}
func (s *BoltStore) WarehouseFindCheckpoints(context.Context, model.TenancySpace, model.CheckpointSequenceID, model.LedgerPosition, int) ([]model.CheckpointRef, error) {
	return nil, model.ErrCheckpointUnsupported
}
func (s *BoltStore) WarehouseGetCheckpoint(context.Context, model.TenancySpace, model.CheckpointID) (model.Checkpoint, error) {
	return model.Checkpoint{}, model.ErrCheckpointUnsupported
}
func (s *BoltStore) WarehouseMarkCheckpointCorrupt(context.Context, model.TenancySpace, model.CheckpointID) error {
	return model.ErrCheckpointUnsupported
}
