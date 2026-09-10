package store_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/vixac/bullet/api"
	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/protocol"
	"github.com/vixac/bullet/store/postgresql"
)

func TestWarehouseImmutableAndIsolated(t *testing.T) {
	s := newWarehousePostgresStore(t, warehouseTestDSN(t))
	ctx := context.Background()
	space := model.TenancySpace{AppId: 1, TenancyId: 2}
	req := model.PutBlobRequest{PutID: "retry", ContentType: "application/octet-stream", Value: []byte{0, 255, 1}, Checksum: "opaque"}
	b, err := s.WarehousePut(ctx, space, req)
	require.NoError(t, err)
	original := b
	original.Value = bytes.Clone(b.Value)
	require.NotEmpty(t, b.ID)
	require.False(t, b.CreatedAt.IsZero())
	retry, err := s.WarehousePut(ctx, space, req)
	require.NoError(t, err)
	require.Equal(t, original, retry)
	req.Value[0] = 5
	b.Value[1] = 5
	retry.Value[2] = 5
	got, err := s.WarehouseGet(ctx, space, b.ID)
	require.NoError(t, err)
	require.Equal(t, original, got)
	got.Value[0] = 7
	many, err := s.WarehouseGetMany(ctx, space, []model.BlobID{b.ID, b.ID, "missing"})
	require.NoError(t, err)
	require.Len(t, many, 1)
	require.Equal(t, original, many[b.ID])
	many[b.ID].Value[0] = 8
	got, err = s.WarehouseGet(ctx, space, b.ID)
	require.NoError(t, err)
	require.Equal(t, original, got)
	for _, other := range []model.TenancySpace{{AppId: 2, TenancyId: 2}, {AppId: 1, TenancyId: 3}} {
		_, err := s.WarehouseGet(ctx, other, b.ID)
		require.ErrorIs(t, err, model.ErrBlobNotFound)
		otherBlob, err := s.WarehousePut(ctx, other, req)
		require.NoError(t, err)
		require.NotEqual(t, b.ID, otherBlob.ID)
	}
	for _, ids := range [][]model.BlobID{nil, {"missing"}} {
		got, err := s.WarehouseGetMany(ctx, space, ids)
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Empty(t, got)
	}
}

func TestWarehouseConflictsAndCancellation(t *testing.T) {
	s := newWarehousePostgresStore(t, warehouseTestDSN(t))
	ctx := context.Background()
	space := model.TenancySpace{}
	req := model.PutBlobRequest{PutID: "p", Value: []byte("hello"), ContentType: "text/plain", Checksum: "metadata"}
	original, err := s.WarehousePut(ctx, space, req)
	require.NoError(t, err)
	for _, field := range []string{"value", "content_type", "checksum"} {
		changed := req
		switch field {
		case "value":
			changed.Value = []byte("different")
		case "content_type":
			changed.ContentType = "other"
		case "checksum":
			changed.Checksum = "other"
		}
		_, err := s.WarehousePut(ctx, space, changed)
		require.ErrorIs(t, err, model.ErrWarehousePutConflict)
	}
	_, err = s.WarehousePut(ctx, space, model.PutBlobRequest{})
	require.ErrorIs(t, err, model.ErrWarehouseInvalidPutID)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = s.WarehousePut(cancelled, space, model.PutBlobRequest{PutID: "cancelled"})
	require.ErrorIs(t, err, context.Canceled)
	_, err = s.WarehouseGet(cancelled, space, original.ID)
	require.ErrorIs(t, err, context.Canceled)
	_, err = s.WarehouseGetMany(cancelled, space, nil)
	require.ErrorIs(t, err, context.Canceled)
	got, err := s.WarehouseGet(ctx, space, original.ID)
	require.NoError(t, err)
	require.Equal(t, original, got)
}

func TestWarehouseConcurrentRetries(t *testing.T) {
	s := newWarehousePostgresStore(t, warehouseTestDSN(t))
	var wg sync.WaitGroup
	ids := make(chan model.BlobID, 32)
	errs := make(chan error, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b, err := s.WarehousePut(context.Background(), model.TenancySpace{}, model.PutBlobRequest{PutID: "same", Value: []byte("value")})
			ids <- b.ID
			errs <- err
		}()
	}
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	var first model.BlobID
	for id := range ids {
		if first == "" {
			first = id
		}
		require.Equal(t, first, id)
	}
}

// Each test gets an isolated database in the suite's PostgreSQL container.
func warehouseTestDSN(t *testing.T) string {
	t.Helper()
	db, err := sql.Open("pgx", warehousePostgresDSN)
	require.NoError(t, err)
	defer db.Close()
	name := "warehouse_" + rand.Text()
	_, err = db.Exec(`CREATE DATABASE "` + name + `"`)
	require.NoError(t, err)
	u, err := url.Parse(warehousePostgresDSN)
	require.NoError(t, err)
	u.Path = "/" + name
	return u.String()
}
func newWarehousePostgresStore(t *testing.T, dsn string) *postgresql.PostgreSQLStore {
	t.Helper()
	s, err := postgresql.NewPostgreSQLStore(dsn)
	require.NoError(t, err)
	t.Cleanup(func() { s.TrackClose() })
	return s
}

func TestWarehousePostgresPersistenceAndBatch(t *testing.T) {
	dsn := warehouseTestDSN(t)
	// Model a database created before Warehouse existed.
	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE depot (id BIGSERIAL PRIMARY KEY, app_id INTEGER NOT NULL, tenancy_id BIGINT NOT NULL, bucket_id INTEGER NOT NULL, value TEXT NOT NULL);
 INSERT INTO depot (app_id, tenancy_id, bucket_id, value) VALUES (1,2,3,'existing')`)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	s := newWarehousePostgresStore(t, dsn)
	ctx := context.Background()
	space := model.TenancySpace{AppId: 1, TenancyId: 2}
	req := model.PutBlobRequest{PutID: "persist", Value: []byte{0, 255, 0}, Checksum: "opaque"}
	original, err := s.WarehousePut(ctx, space, req)
	require.NoError(t, err)
	require.NoError(t, s.TrackClose())
	s = newWarehousePostgresStore(t, dsn)
	got, err := s.WarehouseGet(ctx, space, original.ID)
	require.NoError(t, err)
	require.Equal(t, original, got)
	retry, err := s.WarehousePut(ctx, space, req)
	require.NoError(t, err)
	require.Equal(t, original, retry)
	req.Checksum = "changed"
	_, err = s.WarehousePut(ctx, space, req)
	require.ErrorIs(t, err, model.ErrWarehousePutConflict)
	depot, err := s.DepotGet(space, 1)
	require.NoError(t, err)
	require.Equal(t, "existing", depot)
	for _, value := range [][]byte{nil, {}} {
		b, err := s.WarehousePut(ctx, space, model.PutBlobRequest{PutID: "empty", Value: value})
		require.NoError(t, err)
		require.Empty(t, b.Value)
	}
	ids := make([]model.BlobID, 2100)
	for i := range ids {
		ids[i] = model.BlobID(fmt.Sprint(i))
	}
	expected := map[model.BlobID]model.Blob{original.ID: original}
	ids[0] = original.ID
	for _, i := range []int{999, 1000, 1999, 2000, 2099} {
		b, err := s.WarehousePut(ctx, space, model.PutBlobRequest{PutID: model.PutID(fmt.Sprint(i)), Value: []byte{255, 0}})
		require.NoError(t, err)
		ids[i] = b.ID
		expected[b.ID] = b
	}
	found, err := s.WarehouseGetMany(ctx, space, ids)
	require.NoError(t, err)
	require.Equal(t, expected, found)
}

func TestWarehousePostgresConcurrentInstances(t *testing.T) {
	dsn := warehouseTestDSN(t)
	stores := []*postgresql.PostgreSQLStore{newWarehousePostgresStore(t, dsn), newWarehousePostgresStore(t, dsn)}
	type outcome struct {
		blob  model.Blob
		err   error
		value string
	}
	results := make(chan outcome, 32)
	start := make(chan struct{})
	for i := 0; i < 32; i++ {
		go func(i int) {
			<-start
			value := fmt.Sprint(i % 2)
			b, err := stores[i%2].WarehousePut(context.Background(), model.TenancySpace{}, model.PutBlobRequest{PutID: "race", Value: []byte(value)})
			results <- outcome{b, err, value}
		}(i)
	}
	close(start)
	var winner model.Blob
	var outcomes []outcome
	for i := 0; i < 32; i++ {
		result := <-results
		outcomes = append(outcomes, result)
		if result.err == nil {
			winner = result.blob
		}
	}
	require.NotEmpty(t, winner.ID)
	for _, result := range outcomes {
		if result.value == string(winner.Value) {
			require.NoError(t, result.err)
			require.Equal(t, winner, result.blob)
		} else {
			require.ErrorIs(t, result.err, model.ErrWarehousePutConflict)
		}
	}
	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	defer db.Close()
	var count int
	require.NoError(t, db.QueryRow("SELECT count(*) FROM warehouse").Scan(&count))
	require.Equal(t, 1, count)
}

func TestWarehousePostgresHTTP(t *testing.T) {
	s := newWarehousePostgresStore(t, warehouseTestDSN(t))
	e := api.SetupWarehouseRouter(s, "/warehouse", gin.New())
	request := func(method, path string, body any) *httptest.ResponseRecorder {
		data, err := json.Marshal(body)
		require.NoError(t, err)
		req := httptest.NewRequest(method, path, bytes.NewReader(data))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-App-Id", "1")
		req.Header.Set("X-Tenancy-Id", "2")
		w := httptest.NewRecorder()
		e.ServeHTTP(w, req)
		return w
	}
	req := protocol.PutBlobRequest{PutID: "http", Value: []byte{255, 0}}
	w := request("POST", "/warehouse/blobs", req)
	require.Equal(t, 200, w.Code, w.Body.String())
	var b protocol.Blob
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &b))
	require.Equal(t, req.Value, b.Value)
	require.JSONEq(t, w.Body.String(), request("POST", "/warehouse/blobs", req).Body.String())
	get := request("GET", "/warehouse/blobs/"+string(b.ID), nil)
	require.Equal(t, 200, get.Code)
	require.JSONEq(t, w.Body.String(), get.Body.String())
	many := request("POST", "/warehouse/blobs/batch-get", gin.H{"ids": []model.BlobID{b.ID, "missing"}})
	require.Equal(t, 200, many.Code)
	var found map[model.BlobID]protocol.Blob
	require.NoError(t, json.Unmarshal(many.Body.Bytes(), &found))
	require.Equal(t, map[model.BlobID]protocol.Blob{b.ID: b}, found)
	req.Checksum = "changed"
	require.Equal(t, 409, request("POST", "/warehouse/blobs", req).Code)
	require.Equal(t, 404, request("GET", "/warehouse/blobs/missing", nil).Code)
	require.Equal(t, 400, request("POST", "/warehouse/blobs", protocol.PutBlobRequest{}).Code)
}
