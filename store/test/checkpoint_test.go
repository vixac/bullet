package store_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/store/store_interface"
)

func TestWarehouseCheckpoints(t *testing.T) {
	for name, store := range warehouseStores {
		t.Run(name, func(t *testing.T) {
			testWarehouseCheckpoints(t, store, name)
		})
	}
}

func testWarehouseCheckpoints(t *testing.T, store store_interface.WarehouseStore, name string) {
	t.Helper()
	ctx := context.Background()
	space := model.TenancySpace{AppId: 701, TenancyId: 702}
	prefix := fmt.Sprintf("shared-checkpoint-%s", name)
	base := model.PutCheckpointRequest{
		WriteID: model.CheckpointWriteID(prefix + "-100"), SequenceID: model.CheckpointSequenceID(prefix),
		SourceLedgerID: "workspace-events", StreamGeneration: 1, CoveredThrough: 100,
		ContentType: "application/octet-stream", Codec: "gzip", CodecVersion: "1", SchemaVersion: "1",
		Value: []byte("state-100"), Checksum: "stored-100", StateChecksum: "state-100",
	}
	first, err := store.WarehousePutCheckpoint(ctx, space, base)
	require.NoError(t, err)
	retry, err := store.WarehousePutCheckpoint(ctx, space, base)
	require.NoError(t, err)
	require.Equal(t, first, retry)

	later := base
	later.WriteID, later.CoveredThrough, later.Value = model.CheckpointWriteID(prefix+"-200"), 200, []byte("state-200")
	later.Checksum, later.StateChecksum = "stored-200", "state-200"
	second, err := store.WarehousePutCheckpoint(ctx, space, later)
	require.NoError(t, err)
	latest, err := store.WarehouseGetLatestCheckpoint(ctx, space, base.SequenceID)
	require.NoError(t, err)
	require.Equal(t, second, *latest)

	found, err := store.WarehouseFindCheckpoints(ctx, space, base.SequenceID, 200, 0)
	require.NoError(t, err)
	require.Equal(t, []model.CheckpointRef{second, first}, found)
	limited, err := store.WarehouseFindCheckpoints(ctx, space, base.SequenceID, 200, 1)
	require.NoError(t, err)
	require.Equal(t, []model.CheckpointRef{second}, limited)
	checkpoint, err := store.WarehouseGetCheckpoint(ctx, space, second.ID)
	require.NoError(t, err)
	require.Equal(t, second, checkpoint.Ref)
	require.Equal(t, []byte("state-200"), checkpoint.Value)
	checkpoint.Value[0] = 'X'
	again, err := store.WarehouseGetCheckpoint(ctx, space, second.ID)
	require.NoError(t, err)
	require.Equal(t, []byte("state-200"), again.Value)

	require.NoError(t, store.WarehouseMarkCheckpointCorrupt(ctx, space, second.ID))
	latest, err = store.WarehouseGetLatestCheckpoint(ctx, space, base.SequenceID)
	require.NoError(t, err)
	require.Equal(t, first, *latest)
	found, err = store.WarehouseFindCheckpoints(ctx, space, base.SequenceID, 200, 0)
	require.NoError(t, err)
	require.Equal(t, []model.CheckpointRef{first}, found)
	_, err = store.WarehouseGetCheckpoint(ctx, space, second.ID)
	require.ErrorIs(t, err, model.ErrCheckpointCorrupt)

	conflict := base
	conflict.WriteID, conflict.StateChecksum = model.CheckpointWriteID(prefix+"-conflict"), "different-state"
	_, err = store.WarehousePutCheckpoint(ctx, space, conflict)
	require.ErrorIs(t, err, model.ErrCheckpointConflict)
	otherLatest, err := store.WarehouseGetLatestCheckpoint(ctx, model.TenancySpace{AppId: 701, TenancyId: 703}, base.SequenceID)
	require.NoError(t, err)
	require.Nil(t, otherLatest)

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = store.WarehousePutCheckpoint(cancelled, space, base)
	require.ErrorIs(t, err, context.Canceled)
	_, err = store.WarehousePutCheckpoint(ctx, space, model.PutCheckpointRequest{})
	require.ErrorIs(t, err, model.ErrCheckpointInvalid)
	_, err = store.WarehouseFindCheckpoints(ctx, space, base.SequenceID, -1, 1)
	require.ErrorIs(t, err, model.ErrCheckpointInvalid)
}
