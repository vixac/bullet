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
	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/protocol"
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
	req := protocol.PutBlobRequest{PutID: "retry", Value: []byte{0, 255}, ContentType: "application/octet-stream", Checksum: "unchecked"}
	w := warehouseRequest(t, e, "POST", "/warehouse/blobs", req, true)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var b protocol.Blob
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &b))
	require.Equal(t, req.Value, b.Value)
	retry := warehouseRequest(t, e, "POST", "/warehouse/blobs", req, true)
	require.Equal(t, w.Body.String(), retry.Body.String())
	got := warehouseRequest(t, e, "GET", "/warehouse/blobs/"+string(b.ID), nil, true)
	require.Equal(t, http.StatusOK, got.Code)
	require.JSONEq(t, w.Body.String(), got.Body.String())
	many := warehouseRequest(t, e, "POST", "/warehouse/blobs/batch-get", gin.H{"ids": []string{string(b.ID), "missing"}}, true)
	require.Equal(t, http.StatusOK, many.Code)
	var blobs map[model.BlobID]protocol.Blob
	require.NoError(t, json.Unmarshal(many.Body.Bytes(), &blobs))
	require.Equal(t, map[model.BlobID]protocol.Blob{b.ID: b}, blobs)
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

func TestWarehouseCheckpointHTTP(t *testing.T) {
	e := SetupWarehouseRouter(ram.NewRamStore(), "/warehouse", gin.New())
	req := protocol.PutCheckpointRequest{
		WriteID: "write", SequenceID: "sequence /?#%+雪", SourceLedgerID: "ledger",
		StreamGeneration: 2, CoveredThrough: "9007199254740993",
		ContentType: "application/octet-stream", Codec: "gzip", CodecVersion: "1", SchemaVersion: "2",
		Value: []byte{0, 255, 1}, Checksum: "stored", StateChecksum: "state",
	}
	put := warehouseRequest(t, e, http.MethodPost, "/warehouse/checkpoints", req, true)
	require.Equal(t, http.StatusOK, put.Code, put.Body.String())
	var ref protocol.CheckpointRef
	require.NoError(t, json.Unmarshal(put.Body.Bytes(), &ref))
	require.Equal(t, req.CoveredThrough, ref.CoveredThrough)

	sequencePath := "/warehouse/checkpoint-sequences/sequence%20%2F%3F%23%25%2B%E9%9B%AA/checkpoints"
	latest := warehouseRequest(t, e, http.MethodGet, sequencePath+"/latest", nil, true)
	require.Equal(t, http.StatusOK, latest.Code, latest.Body.String())
	var latestResponse protocol.LatestCheckpointResponse
	require.NoError(t, json.Unmarshal(latest.Body.Bytes(), &latestResponse))
	require.Equal(t, ref, *latestResponse.Checkpoint)

	candidates := warehouseRequest(t, e, http.MethodGet, sequencePath+"?at_or_before=9007199254740993&limit=0", nil, true)
	require.Equal(t, http.StatusOK, candidates.Code, candidates.Body.String())
	var candidatesResponse protocol.CheckpointCandidatesResponse
	require.NoError(t, json.Unmarshal(candidates.Body.Bytes(), &candidatesResponse))
	require.Equal(t, []protocol.CheckpointRef{ref}, candidatesResponse.Checkpoints)

	get := warehouseRequest(t, e, http.MethodGet, "/warehouse/checkpoints/"+string(ref.ID), nil, true)
	require.Equal(t, http.StatusOK, get.Code, get.Body.String())
	var checkpoint protocol.Checkpoint
	require.NoError(t, json.Unmarshal(get.Body.Bytes(), &checkpoint))
	require.Equal(t, ref, checkpoint.Ref)
	require.Equal(t, req.Value, checkpoint.Value)

	corruptPath := "/warehouse/checkpoints/" + string(ref.ID) + "/corrupt"
	require.Equal(t, http.StatusNoContent, warehouseRequest(t, e, http.MethodPost, corruptPath, nil, true).Code)
	require.Equal(t, http.StatusNoContent, warehouseRequest(t, e, http.MethodPost, corruptPath, nil, true).Code)
	corrupt := warehouseRequest(t, e, http.MethodGet, "/warehouse/checkpoints/"+string(ref.ID), nil, true)
	require.Equal(t, http.StatusConflict, corrupt.Code)
	var failure protocol.ErrorResponse
	require.NoError(t, json.Unmarshal(corrupt.Body.Bytes(), &failure))
	require.Equal(t, "checkpoint_corrupt", failure.Code)

	emptyLatest := warehouseRequest(t, e, http.MethodGet, "/warehouse/checkpoint-sequences/missing/checkpoints/latest", nil, true)
	require.JSONEq(t, `{"checkpoint":null}`, emptyLatest.Body.String())
	emptyCandidates := warehouseRequest(t, e, http.MethodGet, "/warehouse/checkpoint-sequences/missing/checkpoints?at_or_before=0&limit=0", nil, true)
	require.JSONEq(t, `{"checkpoints":[]}`, emptyCandidates.Body.String())

	invalidCovered := req
	invalidCovered.CoveredThrough = "not-a-position"
	assertCheckpointHTTPError(t, warehouseRequest(t, e, http.MethodPost, "/warehouse/checkpoints", invalidCovered, true), http.StatusBadRequest, "checkpoint_invalid")
	require.Equal(t, http.StatusBadRequest, warehouseRequest(t, e, http.MethodPost, "/warehouse/checkpoints", gin.H{"value": "invalid-base64!"}, true).Code)
	for _, path := range []string{
		"/warehouse/checkpoint-sequences/sequence/checkpoints",
		"/warehouse/checkpoint-sequences/sequence/checkpoints?at_or_before=bad&limit=1",
		"/warehouse/checkpoint-sequences/sequence/checkpoints?at_or_before=-1&limit=1",
		"/warehouse/checkpoint-sequences/sequence/checkpoints?at_or_before=1",
		"/warehouse/checkpoint-sequences/sequence/checkpoints?at_or_before=1&limit=-1",
		"/warehouse/checkpoint-sequences/sequence/checkpoints?at_or_before=1&limit=999999999999999999999999",
	} {
		assertCheckpointHTTPError(t, warehouseRequest(t, e, http.MethodGet, path, nil, true), http.StatusBadRequest, "checkpoint_invalid")
	}

	for _, route := range []struct{ method, path string }{
		{http.MethodPost, "/warehouse/checkpoints"},
		{http.MethodGet, "/warehouse/checkpoint-sequences/sequence/checkpoints/latest"},
		{http.MethodGet, "/warehouse/checkpoint-sequences/sequence/checkpoints?at_or_before=0&limit=0"},
		{http.MethodGet, "/warehouse/checkpoints/checkpoint"},
		{http.MethodPost, "/warehouse/checkpoints/checkpoint/corrupt"},
	} {
		require.Equal(t, http.StatusUnauthorized, warehouseRequest(t, e, route.method, route.path, req, false).Code)
	}
}

func assertCheckpointHTTPError(t *testing.T, response *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	require.Equal(t, status, response.Code, response.Body.String())
	var failure protocol.ErrorResponse
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &failure))
	require.Equal(t, code, failure.Code)
}

func TestWarehouseUnsupportedStores(t *testing.T) {
	for name, store := range map[string]si.Store{"bolt": &boltdb.BoltStore{}, "mongo": &mongodb.MongoStore{}} {
		t.Run(name, func(t *testing.T) {
			e := SetupWarehouseRouter(store, "/warehouse", gin.New())
			for _, route := range []struct{ method, path string }{{"POST", "/warehouse/blobs"}, {"GET", "/warehouse/blobs/missing"}, {"POST", "/warehouse/blobs/batch-get"}} {
				w := warehouseRequest(t, e, route.method, route.path, gin.H{"put_id": "p"}, true)
				require.Equal(t, http.StatusNotImplemented, w.Code, w.Body.String())
			}
			checkpointReq := protocol.PutCheckpointRequest{
				WriteID: "write", SequenceID: "sequence", SourceLedgerID: "ledger", CoveredThrough: "0",
				Checksum: "stored", StateChecksum: "state",
			}
			for _, route := range []struct{ method, path string }{
				{http.MethodPost, "/warehouse/checkpoints"},
				{http.MethodGet, "/warehouse/checkpoint-sequences/sequence/checkpoints/latest"},
				{http.MethodGet, "/warehouse/checkpoint-sequences/sequence/checkpoints?at_or_before=0&limit=0"},
				{http.MethodGet, "/warehouse/checkpoints/checkpoint"},
				{http.MethodPost, "/warehouse/checkpoints/checkpoint/corrupt"},
			} {
				response := warehouseRequest(t, e, route.method, route.path, checkpointReq, true)
				assertCheckpointHTTPError(t, response, http.StatusNotImplemented, "checkpoint_unsupported")
			}
		})
	}
}
