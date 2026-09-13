package ram

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vixac/bullet/model"
)

func TestWarehouseImmutableAndIsolated(t *testing.T) {
	s := NewRamStore()
	ctx := context.Background()
	space := model.TenancySpace{AppId: 1, TenancyId: 2}
	req := model.PutBlobRequest{PutID: "retry", ContentType: "application/octet-stream", Value: []byte{0, 255, 1}, Checksum: "opaque"}
	b, err := s.WarehousePut(ctx, space, req)
	require.NoError(t, err)
	original := cloneBlob(b)
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
	s := NewRamStore()
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
	s := NewRamStore()
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
