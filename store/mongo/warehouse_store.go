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
func (s *MongoStore) WarehousePutCheckpoint(context.Context, model.TenancySpace, model.PutCheckpointRequest) (model.CheckpointRef, error) {
	return model.CheckpointRef{}, model.ErrCheckpointUnsupported
}
func (s *MongoStore) WarehouseGetLatestCheckpoint(context.Context, model.TenancySpace, model.CheckpointSequenceID) (*model.CheckpointRef, error) {
	return nil, model.ErrCheckpointUnsupported
}
func (s *MongoStore) WarehouseFindCheckpoints(context.Context, model.TenancySpace, model.CheckpointSequenceID, model.LedgerPosition, int) ([]model.CheckpointRef, error) {
	return nil, model.ErrCheckpointUnsupported
}
func (s *MongoStore) WarehouseGetCheckpoint(context.Context, model.TenancySpace, model.CheckpointID) (model.Checkpoint, error) {
	return model.Checkpoint{}, model.ErrCheckpointUnsupported
}
func (s *MongoStore) WarehouseMarkCheckpointCorrupt(context.Context, model.TenancySpace, model.CheckpointID) error {
	return model.ErrCheckpointUnsupported
}
