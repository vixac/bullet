package rest_test

import (
	"context"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/vixac/bullet/api"
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
