package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/vixac/bullet/store/boltdb"
	mongodb "github.com/vixac/bullet/store/mongo"
	"github.com/vixac/bullet/store/ram"
	sqlite "github.com/vixac/bullet/store/sqlite"
	si "github.com/vixac/bullet/store/store_interface"
)

func warehouseRequest(t *testing.T, e *gin.Engine, method, path string, body any, headers bool) *httptest.ResponseRecorder {
	t.Helper()
	data, err := json.Marshal(body)
	require.NoError(t, err)
	req := httptest.NewRequest(method, path, bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	if headers {
		req.Header.Set("X-App-Id", "1")
		req.Header.Set("X-Tenancy-Id", "2")
	}
	w := httptest.NewRecorder()
	e.ServeHTTP(w, req)
	return w
}

func TestWarehouseHTTP(t *testing.T) {
	store, err := sqlite.NewSQLiteStore(filepath.Join(t.TempDir(), "warehouse.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.TrackClose()) })
	for name, store := range map[string]si.WarehouseStore{"ram": ram.NewRamStore(), "sqlite": store} {
		t.Run(name, func(t *testing.T) { testWarehouseHTTP(t, store) })
	}
}

func testWarehouseHTTP(t *testing.T, store si.WarehouseStore) {
	e := SetupWarehouseRouter(store, "/warehouse", gin.New())
	req := si.PutBlobRequest{PutID: "retry", Value: []byte{0, 255}, ContentType: "application/octet-stream", Checksum: "unchecked"}
	w := warehouseRequest(t, e, "POST", "/warehouse/blobs", req, true)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var b si.Blob
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &b))
	require.Equal(t, req.Value, b.Value)
	retry := warehouseRequest(t, e, "POST", "/warehouse/blobs", req, true)
	require.Equal(t, w.Body.String(), retry.Body.String())
	got := warehouseRequest(t, e, "GET", "/warehouse/blobs/"+string(b.ID), nil, true)
	require.Equal(t, http.StatusOK, got.Code)
	require.JSONEq(t, w.Body.String(), got.Body.String())
	many := warehouseRequest(t, e, "POST", "/warehouse/blobs/batch-get", gin.H{"ids": []string{string(b.ID), "missing"}}, true)
	require.Equal(t, http.StatusOK, many.Code)
	var blobs map[si.BlobID]si.Blob
	require.NoError(t, json.Unmarshal(many.Body.Bytes(), &blobs))
	require.Equal(t, map[si.BlobID]si.Blob{b.ID: b}, blobs)
	empty := warehouseRequest(t, e, "POST", "/warehouse/blobs/batch-get", gin.H{"ids": []string{}}, true)
	require.JSONEq(t, "{}", empty.Body.String())
	req.Checksum = "changed"
	require.Equal(t, http.StatusConflict, warehouseRequest(t, e, "POST", "/warehouse/blobs", req, true).Code)
	require.Equal(t, http.StatusBadRequest, warehouseRequest(t, e, "POST", "/warehouse/blobs", gin.H{"put_id": "bad", "value": "invalid-base64!"}, true).Code)
	require.Equal(t, http.StatusBadRequest, warehouseRequest(t, e, "POST", "/warehouse/blobs", gin.H{}, true).Code)
	require.Equal(t, http.StatusNotFound, warehouseRequest(t, e, "GET", "/warehouse/blobs/missing", nil, true).Code)
	for _, route := range []struct{ method, path string }{{"POST", "/warehouse/blobs"}, {"GET", "/warehouse/blobs/missing"}, {"POST", "/warehouse/blobs/batch-get"}} {
		require.Equal(t, http.StatusUnauthorized, warehouseRequest(t, e, route.method, route.path, req, false).Code)
	}
}

func TestWarehouseUnsupportedStores(t *testing.T) {
	for name, store := range map[string]si.Store{"bolt": &boltdb.BoltStore{}, "mongo": &mongodb.MongoStore{}} {
		t.Run(name, func(t *testing.T) {
			e := SetupWarehouseRouter(store, "/warehouse", gin.New())
			for _, route := range []struct{ method, path string }{{"POST", "/warehouse/blobs"}, {"GET", "/warehouse/blobs/missing"}, {"POST", "/warehouse/blobs/batch-get"}} {
				w := warehouseRequest(t, e, route.method, route.path, gin.H{"put_id": "p"}, true)
				require.Equal(t, http.StatusNotImplemented, w.Code, w.Body.String())
			}
		})
	}
}
