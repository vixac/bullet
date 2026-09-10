package store_test

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vixac/bullet/model"
)

func TestPostgreSQLTrackBatchRollback(t *testing.T) {
	db, err := sql.Open("pgx", warehousePostgresDSN)
	require.NoError(t, err)
	defer db.Close()
	_, err = db.Exec(`CREATE FUNCTION reject_track_batch() RETURNS trigger LANGUAGE plpgsql AS $$
 BEGIN
 IF TG_OP = 'DELETE' THEN
  IF OLD.key = 'atomicity-fail' THEN RAISE EXCEPTION 'injected failure'; END IF;
  RETURN OLD;
 END IF;
 IF NEW.key = 'atomicity-fail' THEN RAISE EXCEPTION 'injected failure'; END IF;
 RETURN NEW;
 END $$`)
	require.NoError(t, err)
	defer db.Exec(`DROP FUNCTION reject_track_batch()`)
	s := trackStores["postgresql"]
	for _, operation := range []string{"put", "delete"} {
		t.Run(operation, func(t *testing.T) {
			space := model.TenancySpace{AppId: 9921, TenancyId: 1}
			require.NoError(t, s.TrackPut(space, 1, "first", 10, nil, nil))
			require.NoError(t, s.TrackPut(space, 1, "atomicity-fail", 20, nil, nil))
			_, err := db.Exec(`CREATE TRIGGER reject_track_batch BEFORE INSERT OR DELETE ON track FOR EACH ROW EXECUTE FUNCTION reject_track_batch()`)
			require.NoError(t, err)
			defer db.Exec(`DROP TRIGGER reject_track_batch ON track`)
			if operation == "put" {
				err = s.TrackPutMany(space, map[int32][]model.TrackKeyValueItem{1: {
					{Key: "first", Value: model.TrackValue{Value: 99}},
					{Key: "new", Value: model.TrackValue{Value: 30}},
					{Key: "atomicity-fail", Value: model.TrackValue{Value: 99}},
				}})
			} else {
				err = s.TrackDeleteMany(space, []model.TrackKey{{BucketID: 1, Key: "first"}, {BucketID: 1, Key: "atomicity-fail"}})
			}
			require.Error(t, err)
			for key, want := range map[string]int64{"first": 10, "atomicity-fail": 20} {
				got, err := s.TrackGet(space, 1, key)
				require.NoError(t, err)
				require.Equal(t, want, got)
			}
			_, err = s.TrackGet(space, 1, "new")
			require.Error(t, err)
		})
	}
}
