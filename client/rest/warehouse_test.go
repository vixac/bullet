package rest_test

import (
	"context"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/vixac/bullet/api"
	"github.com/vixac/bullet/store/boltdb"
	"github.com/vixac/bullet/store/ram"

	"github.com/vixac/bullet/client"
	local_bullet "github.com/vixac/bullet/client/local"
	rest_bullet "github.com/vixac/bullet/client/rest"
	"github.com/vixac/bullet/model"
	"net/http/httptest"
	"testing"
)

func TestWarehouseClients(t *testing.T) {
	s := ram.NewRamStore()
	server := httptest.NewServer(api.SetupWarehouseRouter(s, "/warehouse", gin.New()))
	defer server.Close()
	local := local_bullet.New(s, model.TenancySpace{AppId: 1, TenancyId: 2})
	rest := rest_bullet.New(server.URL, model.TenancySpace{AppId: 1, TenancyId: 2})
	ctx := context.Background()
	req := model.PutBlobRequest{PutID: "shared", Value: []byte{0, 255, 1}, ContentType: "application/octet-stream", Checksum: "opaque"}
	original, err := local.WarehousePut(ctx, req)
	require.NoError(t, err)
	for name, client := range map[string]client.Warehouse{"local": local, "rest": rest} {
		t.Run(name, func(t *testing.T) {
			retry, err := client.WarehousePut(ctx, req)
			require.NoError(t, err)
			require.Equal(t, original, retry)
			got, err := client.WarehouseGet(ctx, original.ID)
			require.NoError(t, err)
			require.Equal(t, original, got)
			many, err := client.WarehouseGetMany(ctx, []model.BlobID{original.ID, "missing"})
			require.NoError(t, err)
			require.Equal(t, map[model.BlobID]model.Blob{original.ID: original}, many)
			_, err = client.WarehouseGet(ctx, "missing")
			require.ErrorIs(t, err, model.ErrBlobNotFound)
			changed := req
			changed.Checksum = "different"
			_, err = client.WarehousePut(ctx, changed)
			require.ErrorIs(t, err, model.ErrWarehousePutConflict)
			_, err = client.WarehousePut(ctx, model.PutBlobRequest{})
			require.ErrorIs(t, err, model.ErrWarehouseInvalidPutID)
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			_, err = client.WarehouseGet(ctx, original.ID)
			require.NoError(t, err)
			_, err = client.WarehousePut(cancelled, req)
			require.ErrorIs(t, err, context.Canceled)
			_, err = client.WarehouseGet(cancelled, original.ID)
			require.ErrorIs(t, err, context.Canceled)
			_, err = client.WarehouseGetMany(cancelled, nil)
			require.ErrorIs(t, err, context.Canceled)
		})
	}
}

func TestCheckpointClients(t *testing.T) {
	tests := []struct {
		name string
		new  func(*testing.T) (client.Warehouse, client.Warehouse)
	}{
		{
			name: "local",
			new: func(t *testing.T) (client.Warehouse, client.Warehouse) {
				store := ram.NewRamStore()
				return local_bullet.New(store, model.TenancySpace{AppId: 11, TenancyId: 12}),
					local_bullet.New(store, model.TenancySpace{AppId: 11, TenancyId: 13})
			},
		},
		{
			name: "rest",
			new: func(t *testing.T) (client.Warehouse, client.Warehouse) {
				server := httptest.NewServer(api.SetupWarehouseRouter(ram.NewRamStore(), "/warehouse", gin.New()))
				t.Cleanup(server.Close)
				return rest_bullet.New(server.URL, model.TenancySpace{AppId: 11, TenancyId: 12}),
					rest_bullet.New(server.URL, model.TenancySpace{AppId: 11, TenancyId: 13})
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			primary, otherTenant := test.new(t)
			testCheckpointClient(t, primary, otherTenant)
		})
	}
}

func testCheckpointClient(t *testing.T, warehouse, otherTenant client.Warehouse) {
	t.Helper()
	ctx := context.Background()
	sequenceID := model.CheckpointSequenceID("workspace /?#%+雪")
	const basePosition = model.LedgerPosition(9007199254740993)

	latest, err := warehouse.WarehouseGetLatestCheckpoint(ctx, sequenceID)
	require.NoError(t, err)
	require.Nil(t, latest)
	empty, err := warehouse.WarehouseFindCheckpoints(ctx, sequenceID, basePosition, 0)
	require.NoError(t, err)
	require.NotNil(t, empty)
	require.Empty(t, empty)

	base := model.PutCheckpointRequest{
		WriteID: "write-100", SequenceID: sequenceID, SourceLedgerID: "workspace-events",
		StreamGeneration: 7, CoveredThrough: basePosition,
		ContentType: "application/octet-stream", Codec: "gzip", CodecVersion: "2", SchemaVersion: "3",
		Value: []byte{0, 255, 1, 2}, Checksum: "stored-100", StateChecksum: "state-100",
	}
	first, err := warehouse.WarehousePutCheckpoint(ctx, base)
	require.NoError(t, err)
	retry, err := warehouse.WarehousePutCheckpoint(ctx, base)
	require.NoError(t, err)
	require.Equal(t, first, retry)

	changedRetry := base
	changedRetry.CodecVersion = "different"
	_, err = warehouse.WarehousePutCheckpoint(ctx, changedRetry)
	require.ErrorIs(t, err, model.ErrCheckpointConflict)
	conflictingState := base
	conflictingState.WriteID = "write-conflicting-state"
	conflictingState.StateChecksum = "different-state"
	_, err = warehouse.WarehousePutCheckpoint(ctx, conflictingState)
	require.ErrorIs(t, err, model.ErrCheckpointConflict)

	later := base
	later.WriteID = "write-200"
	later.CoveredThrough = basePosition + 100
	later.Value = []byte("state-200")
	later.Checksum = "stored-200"
	later.StateChecksum = "state-200"
	second, err := warehouse.WarehousePutCheckpoint(ctx, later)
	require.NoError(t, err)

	latest, err = warehouse.WarehouseGetLatestCheckpoint(ctx, sequenceID)
	require.NoError(t, err)
	require.Equal(t, second, *latest)
	before, err := warehouse.WarehouseFindCheckpoints(ctx, sequenceID, basePosition-1, 0)
	require.NoError(t, err)
	require.NotNil(t, before)
	require.Empty(t, before)
	candidates, err := warehouse.WarehouseFindCheckpoints(ctx, sequenceID, later.CoveredThrough, 0)
	require.NoError(t, err)
	require.Equal(t, []model.CheckpointRef{second, first}, candidates)
	limited, err := warehouse.WarehouseFindCheckpoints(ctx, sequenceID, later.CoveredThrough, 1)
	require.NoError(t, err)
	require.Equal(t, []model.CheckpointRef{second}, limited)

	checkpoint, err := warehouse.WarehouseGetCheckpoint(ctx, second.ID)
	require.NoError(t, err)
	require.Equal(t, model.Checkpoint{Ref: second, Value: later.Value}, checkpoint)
	checkpoint.Value[0] = 'X'
	again, err := warehouse.WarehouseGetCheckpoint(ctx, second.ID)
	require.NoError(t, err)
	require.Equal(t, later.Value, again.Value)

	otherLatest, err := otherTenant.WarehouseGetLatestCheckpoint(ctx, sequenceID)
	require.NoError(t, err)
	require.Nil(t, otherLatest)
	_, err = otherTenant.WarehouseGetCheckpoint(ctx, second.ID)
	require.ErrorIs(t, err, model.ErrCheckpointNotFound)

	require.NoError(t, warehouse.WarehouseMarkCheckpointCorrupt(ctx, second.ID))
	require.NoError(t, warehouse.WarehouseMarkCheckpointCorrupt(ctx, second.ID))
	_, err = warehouse.WarehouseGetCheckpoint(ctx, second.ID)
	require.ErrorIs(t, err, model.ErrCheckpointCorrupt)
	latest, err = warehouse.WarehouseGetLatestCheckpoint(ctx, sequenceID)
	require.NoError(t, err)
	require.Equal(t, first, *latest)
	candidates, err = warehouse.WarehouseFindCheckpoints(ctx, sequenceID, later.CoveredThrough, 0)
	require.NoError(t, err)
	require.Equal(t, []model.CheckpointRef{first}, candidates)

	_, err = warehouse.WarehouseGetCheckpoint(ctx, "missing /?#%+雪")
	require.ErrorIs(t, err, model.ErrCheckpointNotFound)
	err = warehouse.WarehouseMarkCheckpointCorrupt(ctx, "missing /?#%+雪")
	require.ErrorIs(t, err, model.ErrCheckpointNotFound)
	_, err = warehouse.WarehousePutCheckpoint(ctx, model.PutCheckpointRequest{})
	require.ErrorIs(t, err, model.ErrCheckpointInvalid)
	_, err = warehouse.WarehouseFindCheckpoints(ctx, sequenceID, -1, 1)
	require.ErrorIs(t, err, model.ErrCheckpointInvalid)
	_, err = warehouse.WarehouseFindCheckpoints(ctx, sequenceID, basePosition, -1)
	require.ErrorIs(t, err, model.ErrCheckpointInvalid)

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = warehouse.WarehousePutCheckpoint(cancelled, base)
	require.ErrorIs(t, err, context.Canceled)
	_, err = warehouse.WarehouseGetLatestCheckpoint(cancelled, sequenceID)
	require.ErrorIs(t, err, context.Canceled)
	_, err = warehouse.WarehouseFindCheckpoints(cancelled, sequenceID, basePosition, 1)
	require.ErrorIs(t, err, context.Canceled)
	_, err = warehouse.WarehouseGetCheckpoint(cancelled, first.ID)
	require.ErrorIs(t, err, context.Canceled)
	err = warehouse.WarehouseMarkCheckpointCorrupt(cancelled, first.ID)
	require.ErrorIs(t, err, context.Canceled)
}

func TestCheckpointUnsupportedClients(t *testing.T) {
	store := &boltdb.BoltStore{}
	server := httptest.NewServer(api.SetupWarehouseRouter(store, "/warehouse", gin.New()))
	defer server.Close()
	space := model.TenancySpace{AppId: 21, TenancyId: 22}
	for name, warehouse := range map[string]client.Warehouse{
		"local": local_bullet.New(store, space),
		"rest":  rest_bullet.New(server.URL, space),
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			req := model.PutCheckpointRequest{
				WriteID: "write", SequenceID: "sequence", SourceLedgerID: "ledger",
				Checksum: "stored", StateChecksum: "state",
			}
			_, err := warehouse.WarehousePutCheckpoint(ctx, req)
			require.ErrorIs(t, err, model.ErrCheckpointUnsupported)
			_, err = warehouse.WarehouseGetLatestCheckpoint(ctx, "sequence")
			require.ErrorIs(t, err, model.ErrCheckpointUnsupported)
			_, err = warehouse.WarehouseFindCheckpoints(ctx, "sequence", 0, 0)
			require.ErrorIs(t, err, model.ErrCheckpointUnsupported)
			_, err = warehouse.WarehouseGetCheckpoint(ctx, "checkpoint")
			require.ErrorIs(t, err, model.ErrCheckpointUnsupported)
			err = warehouse.WarehouseMarkCheckpointCorrupt(ctx, "checkpoint")
			require.ErrorIs(t, err, model.ErrCheckpointUnsupported)
		})
	}
}
