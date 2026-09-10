package sqlite_store

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vixac/bullet/model"
)

func TestWarehouseImmutableAndIsolated(t *testing.T) {
	s := newWarehouseTestStore(t, ":memory:")
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
	s := newWarehouseTestStore(t, ":memory:")
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
	s := newWarehouseTestStore(t, ":memory:")
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

func newWarehouseTestStore(t *testing.T, path string) *SQLiteStore {
	t.Helper()
	s, err := NewSQLiteStore(path)
	require.NoError(t, err)
	t.Cleanup(func() { s.db.Close() })
	return s
}

func TestWarehousePersistenceAndExistingDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "warehouse.db")
	// Simulate an older database without Warehouse and verify existing Depot data.
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE depot (id INTEGER PRIMARY KEY AUTOINCREMENT, app_id INTEGER NOT NULL, tenancy_id INTEGER NOT NULL, bucket_id INTEGER NOT NULL, value TEXT NOT NULL);
 INSERT INTO depot (app_id, tenancy_id, bucket_id, value) VALUES (1,2,3,'existing')`)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	s := newWarehouseTestStore(t, path)
	ctx := context.Background()
	space := model.TenancySpace{AppId: 1, TenancyId: 2}
	req := model.PutBlobRequest{PutID: "persist", ContentType: "binary", Value: []byte{0, 255, 0}, Checksum: "opaque"}
	b, err := s.WarehousePut(ctx, space, req)
	require.NoError(t, err)
	require.NoError(t, s.db.Close())
	reopened := newWarehouseTestStore(t, path)
	got, err := reopened.WarehouseGet(ctx, space, b.ID)
	require.NoError(t, err)
	require.Equal(t, b, got)
	retry, err := reopened.WarehousePut(ctx, space, req)
	require.NoError(t, err)
	require.Equal(t, b, retry)
	req.Value = []byte("changed")
	_, err = reopened.WarehousePut(ctx, space, req)
	require.ErrorIs(t, err, model.ErrWarehousePutConflict)
	value, err := reopened.DepotGet(space, 1)
	require.NoError(t, err)
	require.Equal(t, "existing", value)
}

func TestWarehouseEmptyAndLargeBatch(t *testing.T) {
	s := newWarehouseTestStore(t, ":memory:")
	ctx := context.Background()
	space := model.TenancySpace{}
	for _, value := range [][]byte{nil, {}} {
		b, err := s.WarehousePut(ctx, space, model.PutBlobRequest{PutID: "empty", Value: value})
		require.NoError(t, err)
		require.Empty(t, b.Value)
	}
	ids := make([]model.BlobID, 1900)
	for i := range ids {
		ids[i] = model.BlobID(fmt.Sprintf("missing-%d", i))
	}
	expected := make(map[model.BlobID]model.Blob)
	for _, i := range []int{0, 899, 900, 1799, 1800, 1899} {
		b, err := s.WarehousePut(ctx, space, model.PutBlobRequest{PutID: model.PutID(fmt.Sprint(i)), Value: []byte{0, 255}})
		require.NoError(t, err)
		ids[i] = b.ID
		expected[b.ID] = b
	}
	found, err := s.WarehouseGetMany(ctx, space, ids)
	require.NoError(t, err)
	require.Equal(t, expected, found)
}

func TestWarehouseConcurrentStoreInstances(t *testing.T) {
	path := filepath.Join(t.TempDir(), "concurrent.db")
	stores := []*SQLiteStore{newWarehouseTestStore(t, path), newWarehouseTestStore(t, path)}
	ctx := context.Background()
	space := model.TenancySpace{}
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
			b, err := stores[i%2].WarehousePut(ctx, space, model.PutBlobRequest{PutID: "race", Value: []byte(value)})
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
	var count int
	require.NoError(t, stores[0].db.QueryRow("SELECT count(*) FROM warehouse").Scan(&count))
	require.Equal(t, 1, count)
}
