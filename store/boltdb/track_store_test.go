package boltdb

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vixac/bullet/model"
)

func TestTrackBatchRollback(t *testing.T) {
	s, err := NewBoltStore(filepath.Join(t.TempDir(), "track.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.TrackClose()) })
	space := model.TenancySpace{AppId: 1, TenancyId: 2}
	require.NoError(t, s.TrackPut(space, 1, "first", model.TrackValue{Value: 10, Payload: []byte("payload")}))
	withPayload, err := s.TrackGet(space, 1, "first", model.TrackReadOptions{IncludePayload: true})
	require.NoError(t, err)
	require.Equal(t, []byte("payload"), withPayload.Payload)
	err = s.TrackPutMany(space, map[int32][]model.TrackKeyValueItem{1: {
		{Key: "first", Value: model.TrackValue{Value: 99}},
		{Key: "new", Value: model.TrackValue{Value: 30}},
		{Key: "", Value: model.TrackValue{Value: 99}}, // bbolt rejects empty keys after the preceding writes.
	}})
	require.Error(t, err)
	got, err := s.TrackGet(space, 1, "first", model.TrackReadOptions{})
	require.NoError(t, err)
	require.Equal(t, int64(10), got.Value)
	_, err = s.TrackGet(space, 1, "new", model.TrackReadOptions{})
	require.Error(t, err)
	err = s.TrackDeleteMany(space, []model.TrackKey{{BucketID: 1, Key: "first"}, {BucketID: 2, Key: "missing-bucket"}})
	require.Error(t, err)
	got, err = s.TrackGet(space, 1, "first", model.TrackReadOptions{})
	require.NoError(t, err)
	require.Equal(t, int64(10), got.Value)
}
