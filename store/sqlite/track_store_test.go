package sqlite_store

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vixac/bullet/model"
)

func TestTrackBatchRollback(t *testing.T) {
	for _, operation := range []string{"put", "delete"} {
		t.Run(operation, func(t *testing.T) {
			s := newLedgerTestStore(t)
			space := model.TenancySpace{AppId: 1, TenancyId: 2}
			require.NoError(t, s.TrackPut(space, 1, "first", 10, nil, nil))
			require.NoError(t, s.TrackPut(space, 1, "fail", 20, nil, nil))
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
				got, err := s.TrackGet(space, 1, key)
				require.NoError(t, err)
				require.Equal(t, want, got)
			}
			_, err = s.TrackGet(space, 1, "new")
			require.Error(t, err)
		})
	}
}
