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

	// WarehousePutCheckpoint atomically stores checkpoint bytes and publishes
	// their immutable source-ledger boundary. WriteID retries return the first
	// checkpoint; a different request with the same WriteID conflicts.
	WarehousePutCheckpoint(context.Context, model.TenancySpace, model.PutCheckpointRequest) (model.CheckpointRef, error)
	// WarehouseGetLatestCheckpoint returns the newest ready checkpoint for a
	// sequence, or nil when no checkpoint has been published.
	WarehouseGetLatestCheckpoint(context.Context, model.TenancySpace, model.CheckpointSequenceID) (*model.CheckpointRef, error)
	// WarehouseFindCheckpoints returns ready candidates at or before a target
	// source position, newest first. It supports recovery from a bad snapshot.
	WarehouseFindCheckpoints(context.Context, model.TenancySpace, model.CheckpointSequenceID, model.LedgerPosition, int) ([]model.CheckpointRef, error)
	WarehouseGetCheckpoint(context.Context, model.TenancySpace, model.CheckpointID) (model.Checkpoint, error)
	// WarehouseMarkCheckpointCorrupt hides a bad representation from normal
	// lookup while retaining it for diagnostics and later repair.
	WarehouseMarkCheckpointCorrupt(context.Context, model.TenancySpace, model.CheckpointID) error
}
