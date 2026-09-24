package sqlite_store

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vixac/bullet/model"
)

func TestExistingTrackDatabaseAddsPayloadTable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "existing.db")
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE track (
		app_id INTEGER, tenancy_id INTEGER, bucket_id INTEGER, key TEXT,
		value INTEGER, tag INTEGER, metric REAL,
		PRIMARY KEY (app_id, tenancy_id, bucket_id, key)
	)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO track (app_id, tenancy_id, bucket_id, key, value) VALUES (1, 2, 3, 'existing', 4)`)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store, err := NewSQLiteStore(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.TrackClose()) })
	var table string
	require.NoError(t, store.db.QueryRow(`SELECT name FROM sqlite_master WHERE type='table' AND name='track_payload'`).Scan(&table))
	require.Equal(t, "track_payload", table)
	value, err := store.TrackGet(model.TenancySpace{AppId: 1, TenancyId: 2}, 3, "existing", model.TrackReadOptions{IncludePayload: true})
	require.NoError(t, err)
	require.Equal(t, int64(4), value.Value)
	require.Nil(t, value.Payload)
	require.NoError(t, store.TrackPut(model.TenancySpace{AppId: 1, TenancyId: 2}, 3, "empty", model.TrackValue{Payload: []byte{}}))
	empty, err := store.TrackGet(model.TenancySpace{AppId: 1, TenancyId: 2}, 3, "empty", model.TrackReadOptions{IncludePayload: true})
	require.NoError(t, err)
	require.NotNil(t, empty.Payload)
	require.Empty(t, empty.Payload)
}

func TestTrackBatchRollback(t *testing.T) {
	for _, operation := range []string{"put", "delete"} {
		t.Run(operation, func(t *testing.T) {
			s := newLedgerTestStore(t)
			space := model.TenancySpace{AppId: 1, TenancyId: 2}
			require.NoError(t, s.TrackPut(space, 1, "first", model.TrackValue{Value: 10}))
			require.NoError(t, s.TrackPut(space, 1, "fail", model.TrackValue{Value: 20}))
			event := "INSERT"
			if operation == "delete" {
				event = "DELETE"
			}
			row := "NEW"
			if operation == "delete" {
				row = "OLD"
			}
			_, err := s.db.Exec("CREATE TRIGGER reject_track BEFORE " + event + " ON track WHEN " + row + ".key = 'fail' BEGIN SELECT RAISE(ABORT, 'injected failure'); END")
			require.NoError(t, err)
			if operation == "put" {
				err = s.TrackPutMany(space, map[int32][]model.TrackKeyValueItem{1: {
					{Key: "first", Value: model.TrackValue{Value: 99}},
					{Key: "new", Value: model.TrackValue{Value: 30}},
					{Key: "fail", Value: model.TrackValue{Value: 99}},
				}})
			} else {
				err = s.TrackDeleteMany(space, []model.TrackKey{{BucketID: 1, Key: "first"}, {BucketID: 1, Key: "fail"}})
			}
			require.Error(t, err)
			for key, want := range map[string]int64{"first": 10, "fail": 20} {
				got, err := s.TrackGet(space, 1, key, model.TrackReadOptions{})
				require.NoError(t, err)
				require.Equal(t, want, got.Value)
			}
			_, err = s.TrackGet(space, 1, "new", model.TrackReadOptions{})
			require.Error(t, err)
		})
	}
}
