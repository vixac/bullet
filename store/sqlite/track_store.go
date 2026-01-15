package sqlite_store

import (
	"database/sql"
	"errors"

	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/store/store_interface"
)

func (s *SQLiteStore) TrackGet(
	space store_interface.TenancySpace,
	bucketID int32,
	key string,
) (int64, error) {

	var value int64
	err := s.db.QueryRow(`
		SELECT value FROM track
		WHERE app_id=? AND tenancy_id=? AND bucket_id=? AND key=?
	`,
		space.AppId, space.TenancyId, bucketID, key,
	).Scan(&value)

	if errors.Is(err, sql.ErrNoRows) {
		return 0, errors.New("not found")
	}
	return value, err
}

func (s *SQLiteStore) GetItemsByKeyPrefix(
	space store_interface.TenancySpace,
	bucketID int32,
	prefix string,
	tags []int64,
	metricValue *float64,
	metricIsGt bool,
) ([]model.TrackKeyValueItem, error) {

	query := `
		SELECT key, value, tag, metric
		FROM track
		WHERE app_id=? AND tenancy_id=? AND bucket_id=?
		  AND key >= ? AND key < ?
	`

	args := []any{
		space.AppId,
		space.TenancyId,
		bucketID,
		prefix,
		prefix + "\uffff",
	}

	if len(tags) > 0 {
		query += " AND tag IN (" + placeholders(len(tags)) + ")"
		for _, t := range tags {
			args = append(args, t)
		}
	}

	if metricValue != nil {
		if metricIsGt {
			query += " AND metric > ?"
		} else {
			query += " AND metric < ?"
		}
		args = append(args, *metricValue)
	}

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []model.TrackKeyValueItem
	for rows.Next() {
		var item model.TrackKeyValueItem
		err := rows.Scan(&item.Key, &item.Value.Value, &item.Value.Tag, &item.Value.Metric)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, nil
}

func (s *SQLiteStore) GetItemsByKeyPrefixes(
	space store_interface.TenancySpace,
	bucketID int32,
	prefixes []string,
	tags []int64,
	metricValue *float64,
	metricIsGt bool,
) ([]model.TrackKeyValueItem, error) {

	if len(prefixes) == 0 {
		return nil, nil
	}

	query := `
		SELECT key, value, tag, metric
		FROM track
		WHERE app_id=? AND tenancy_id=? AND bucket_id=?
		  AND (
	`
	args := []any{space.AppId, space.TenancyId, bucketID}

	for i, p := range prefixes {
		if i > 0 {
			query += " OR "
		}
		query += "(key >= ? AND key < ?)"
		args = append(args, p, p+"\uffff")
	}
	query += ")"

	if len(tags) > 0 {
		query += " AND tag IN (" + placeholders(len(tags)) + ")"
		for _, t := range tags {
			args = append(args, t)
		}
	}

	if metricValue != nil {
		if metricIsGt {
			query += " AND metric > ?"
		} else {
			query += " AND metric < ?"
		}
		args = append(args, *metricValue)
	}

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []model.TrackKeyValueItem
	for rows.Next() {
		var item model.TrackKeyValueItem
		err := rows.Scan(&item.Key, &item.Value.Value, &item.Value.Tag, &item.Value.Metric)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, nil
}

func (s *SQLiteStore) TrackClose() error {
	return s.db.Close()
}

func (s *SQLiteStore) TrackDeleteMany(
	space store_interface.TenancySpace,
	items []model.TrackBucketKeyPair,
) error {

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		DELETE FROM track
		WHERE app_id=? AND tenancy_id=? AND bucket_id=? AND key=?
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, item := range items {
		if _, err := stmt.Exec(
			space.AppId,
			space.TenancyId,
			item.BucketID,
			item.Key,
		); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (s *SQLiteStore) TrackPutMany(
	space store_interface.TenancySpace,
	items map[int32][]model.TrackKeyValueItem,
) error {

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO track
		(app_id, tenancy_id, bucket_id, key, value, tag, metric)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(app_id, tenancy_id, bucket_id, key)
		DO UPDATE SET
			value=excluded.value,
			tag=excluded.tag,
			metric=excluded.metric
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for bucketID, bucketItems := range items {
		for _, item := range bucketItems {
			if _, err := stmt.Exec(
				space.AppId,
				space.TenancyId,
				bucketID,
				item.Key,
				item.Value.Value,
				item.Value.Tag,
				item.Value.Metric,
			); err != nil {
				return err
			}
		}
	}

	return tx.Commit()
}

func (s *SQLiteStore) TrackGetMany(
	space store_interface.TenancySpace,
	keys map[int32][]string,
) (map[int32]map[string]model.TrackValue, map[int32][]string, error) {

	values := make(map[int32]map[string]model.TrackValue)
	missing := make(map[int32][]string)

	for bucketID, bucketKeys := range keys {
		if len(bucketKeys) == 0 {
			continue
		}

		query := `
			SELECT key, value, tag, metric
			FROM track
			WHERE app_id=? AND tenancy_id=? AND bucket_id=?
			  AND key IN (` + placeholders(len(bucketKeys)) + `)
		`

		args := []any{space.AppId, space.TenancyId, bucketID}
		for _, k := range bucketKeys {
			args = append(args, k)
		}

		rows, err := s.db.Query(query, args...)
		if err != nil {
			return nil, nil, err
		}

		found := make(map[string]struct{})
		if values[bucketID] == nil {
			values[bucketID] = make(map[string]model.TrackValue)
		}

		for rows.Next() {
			var key string
			var tv model.TrackValue
			if err := rows.Scan(&key, &tv.Value, &tv.Tag, &tv.Metric); err != nil {
				rows.Close()
				return nil, nil, err
			}
			values[bucketID][key] = tv
			found[key] = struct{}{}
		}
		rows.Close()

		for _, k := range bucketKeys {
			if _, ok := found[k]; !ok {
				missing[bucketID] = append(missing[bucketID], k)
			}
		}
	}

	return values, missing, nil
}

func (s *SQLiteStore) TrackPut(
	space store_interface.TenancySpace,
	bucketID int32,
	key string,
	value int64,
	tag *int64,
	metric *float64,
) error {

	_, err := s.db.Exec(`
		INSERT INTO track
			(app_id, tenancy_id, bucket_id, key, value, tag, metric)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(app_id, tenancy_id, bucket_id, key)
		DO UPDATE SET
			value  = excluded.value,
			tag    = excluded.tag,
			metric = excluded.metric
	`,
		space.AppId,
		space.TenancyId,
		bucketID,
		key,
		value,
		tag,
		metric,
	)

	return err
}
