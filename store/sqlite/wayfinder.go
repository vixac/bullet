package sqlite_store

import (
	"database/sql"

	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/store/store_interface"
)

func (s *SQLiteStore) WayFinderPut(
	space store_interface.TenancySpace,
	bucketID int32,
	key string,
	payload string,
	tag *int64,
	metric *float64,
) (int64, error) {

	res, err := s.db.Exec(`
		INSERT INTO wayfinder
		(app_id, tenancy_id, bucket_id, key, payload, tag, metric)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`,
		space.AppId, space.TenancyId, bucketID, key, payload, tag, metric,
	)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (s *SQLiteStore) WayFinderGetByPrefix(
	space store_interface.TenancySpace,
	bucketID int32,
	prefix string,
	tags []int64,
	metricValue *float64,
	metricIsGt bool,
) ([]model.WayFinderQueryItem, error) {

	query := `
		SELECT key, item_id, tag, metric, payload
		FROM wayfinder
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

	var result []model.WayFinderQueryItem
	for rows.Next() {
		var item model.WayFinderQueryItem
		if err := rows.Scan(
			&item.Key,
			&item.ItemId,
			&item.Tag,
			&item.Metric,
			&item.Payload,
		); err != nil {
			return nil, err
		}
		result = append(result, item)
	}

	return result, nil
}

func (s *SQLiteStore) WayFinderGetOne(
	space store_interface.TenancySpace,
	bucketID int32,
	key string,
) (*model.WayFinderGetResponse, error) {

	var resp model.WayFinderGetResponse

	err := s.db.QueryRow(`
		SELECT item_id, payload, tag, metric
		FROM wayfinder
		WHERE app_id=? AND tenancy_id=? AND bucket_id=? AND key=?
	`,
		space.AppId,
		space.TenancyId,
		bucketID,
		key,
	).Scan(
		&resp.ItemId,
		&resp.Payload,
		&resp.Tag,
		&resp.Metric,
	)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return &resp, nil
}
