package sqlite_store

import (
	"database/sql"
	"errors"

	"github.com/vixac/bullet/model"
)

type sqlQueryer interface {
	Query(query string, args ...any) (*sql.Rows, error)
}

func (s *SQLiteStore) TrackMutate(space model.TenancySpace, req model.TrackMutation) (model.TrackMutationResult, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return model.TrackMutationResult{}, err
	}
	defer tx.Rollback()

	result, err := tx.Exec(`INSERT OR IGNORE INTO track_mutations (mutation_id) VALUES (?)`, req.MutationID)
	if err != nil {
		return model.TrackMutationResult{}, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return model.TrackMutationResult{}, err
	}
	if rows == 0 {
		return model.TrackMutationResult{Applied: false}, nil
	}
	for _, put := range req.Puts {
		if err := model.ValidateTrackValue(put.Value); err != nil {
			return model.TrackMutationResult{}, err
		}
	}

	upsertStmt, err := tx.Prepare(`
		INSERT INTO track (app_id, tenancy_id, bucket_id, key, value, tag, metric)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(app_id, tenancy_id, bucket_id, key) DO UPDATE SET
			value=excluded.value, tag=excluded.tag, metric=excluded.metric`)
	if err != nil {
		return model.TrackMutationResult{}, err
	}
	defer upsertStmt.Close()
	insertStmt, err := tx.Prepare(`
		INSERT INTO track (app_id, tenancy_id, bucket_id, key, value, tag, metric)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(app_id, tenancy_id, bucket_id, key) DO NOTHING`)
	if err != nil {
		return model.TrackMutationResult{}, err
	}
	defer insertStmt.Close()
	for _, put := range req.Puts {
		stmt := upsertStmt
		if put.IfAbsent {
			stmt = insertStmt
		}
		result, err := stmt.Exec(space.AppId, space.TenancyId, put.BucketID, put.Key, put.Value.Value, put.Value.Tag, put.Value.Metric)
		if err != nil {
			return model.TrackMutationResult{}, err
		}
		if put.IfAbsent {
			rows, err := result.RowsAffected()
			if err != nil {
				return model.TrackMutationResult{}, err
			}
			if rows == 0 {
				return model.TrackMutationResult{}, model.ErrTrackKeyAlreadyExists
			}
		}
		if err := setSQLiteTrackPayload(tx, space, put.BucketID, put.Key, put.Value.Payload); err != nil {
			return model.TrackMutationResult{}, err
		}
	}

	deleteStmt, err := tx.Prepare(`DELETE FROM track WHERE app_id=? AND tenancy_id=? AND bucket_id=? AND key=?`)
	if err != nil {
		return model.TrackMutationResult{}, err
	}
	defer deleteStmt.Close()
	for _, key := range req.Deletes {
		if _, err := tx.Exec(`DELETE FROM track_payload WHERE app_id=? AND tenancy_id=? AND bucket_id=? AND key=?`, space.AppId, space.TenancyId, key.BucketID, key.Key); err != nil {
			return model.TrackMutationResult{}, err
		}
		if _, err := deleteStmt.Exec(space.AppId, space.TenancyId, key.BucketID, key.Key); err != nil {
			return model.TrackMutationResult{}, err
		}
	}

	if err := tx.Commit(); err != nil {
		return model.TrackMutationResult{}, err
	}
	return model.TrackMutationResult{Applied: true}, nil
}

func (s *SQLiteStore) TrackGet(
	space model.TenancySpace,
	bucketID int32,
	key string,
	opts model.TrackReadOptions,
) (model.TrackValue, error) {

	var value model.TrackValue
	query := `SELECT t.value, t.tag, t.metric FROM track t WHERE t.app_id=? AND t.tenancy_id=? AND t.bucket_id=? AND t.key=?`
	var err error
	if opts.IncludePayload {
		query = `SELECT t.value, t.tag, t.metric, p.payload FROM track t
			LEFT JOIN track_payload p ON p.app_id=t.app_id AND p.tenancy_id=t.tenancy_id AND p.bucket_id=t.bucket_id AND p.key=t.key
			WHERE t.app_id=? AND t.tenancy_id=? AND t.bucket_id=? AND t.key=?`
		err = s.db.QueryRow(query, space.AppId, space.TenancyId, bucketID, key).Scan(&value.Value, &value.Tag, &value.Metric, &value.Payload)
	} else {
		err = s.db.QueryRow(query, space.AppId, space.TenancyId, bucketID, key).Scan(&value.Value, &value.Tag, &value.Metric)
	}

	if errors.Is(err, sql.ErrNoRows) {
		return model.TrackValue{}, errors.New("not found")
	}
	if err != nil {
		return model.TrackValue{}, err
	}
	return value, nil
}

func (s *SQLiteStore) GetItemsByKeyPrefix(
	space model.TenancySpace,
	bucketID int32,
	prefix string,
	tags []int64,
	metricValue *float64,
	metricIsGt bool,
) ([]model.TrackKeyValueItem, error) {
	return s.getItemsByKeyPrefixChunks(space, bucketID, []string{prefix}, tags, metricValue, metricIsGt)
}

func (s *SQLiteStore) GetItemsByKeyPrefixes(
	space model.TenancySpace,
	bucketID int32,
	prefixes []string,
	tags []int64,
	metricValue *float64,
	metricIsGt bool,
) ([]model.TrackKeyValueItem, error) {

	if len(prefixes) == 0 {
		return nil, nil
	}

	return s.getItemsByKeyPrefixChunks(space, bucketID, prefixes, tags, metricValue, metricIsGt)
}

// getItemsByKeyPrefixChunks splits caller-provided prefix and tag filters so
// dynamically-generated OR and IN expressions stay below SQLite's expression
// depth limit. Results are de-duplicated because overlapping prefix chunks can
// match the same key.
func (s *SQLiteStore) getItemsByKeyPrefixChunks(
	space model.TenancySpace,
	bucketID int32,
	prefixes []string,
	tags []int64,
	metricValue *float64,
	metricIsGt bool,
) ([]model.TrackKeyValueItem, error) {
	prefixes = uniqueStrings(prefixes)
	tags = uniqueInt64s(tags)

	// Keep all chunks in one read snapshot.
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var out []model.TrackKeyValueItem
	seen := make(map[string]struct{})
	for prefixStart := 0; prefixStart < len(prefixes); prefixStart += sqliteQueryChunkSize {
		prefixEnd := prefixStart + sqliteQueryChunkSize
		if prefixEnd > len(prefixes) {
			prefixEnd = len(prefixes)
		}

		// A nil tag slice represents no tag filter, and thus one query for this
		// prefix chunk rather than zero queries.
		for tagStart := 0; tagStart < max(1, len(tags)); tagStart += sqliteQueryChunkSize {
			tagEnd := tagStart + sqliteQueryChunkSize
			if tagEnd > len(tags) {
				tagEnd = len(tags)
			}
			chunkTags := tags[tagStart:tagEnd]

			items, err := getItemsByKeyPrefixQuery(tx, space, bucketID, prefixes[prefixStart:prefixEnd], chunkTags, metricValue, metricIsGt)
			if err != nil {
				return nil, err
			}
			for _, item := range items {
				if _, ok := seen[item.Key]; ok {
					continue
				}
				seen[item.Key] = struct{}{}
				out = append(out, item)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return out, nil
}

func getItemsByKeyPrefixQuery(
	db sqlQueryer,
	space model.TenancySpace,
	bucketID int32,
	prefixes []string,
	tags []int64,
	metricValue *float64,
	metricIsGt bool,
) ([]model.TrackKeyValueItem, error) {
	query := `
		SELECT key, value, tag, metric
		FROM track
		WHERE app_id=? AND tenancy_id=? AND bucket_id=? AND (`
	args := []any{space.AppId, space.TenancyId, bucketID}
	for i, prefix := range prefixes {
		if i > 0 {
			query += " OR "
		}
		query += "(key >= ? AND key < ?)"
		args = append(args, prefix, prefix+"\uffff")
	}
	query += ")"

	if len(tags) > 0 {
		query += " AND tag IN (" + placeholders(len(tags)) + ")"
		for _, tag := range tags {
			args = append(args, tag)
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

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []model.TrackKeyValueItem
	for rows.Next() {
		var item model.TrackKeyValueItem
		if err := rows.Scan(&item.Key, &item.Value.Value, &item.Value.Tag, &item.Value.Metric); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	unique := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		unique = append(unique, value)
	}
	return unique
}

func uniqueInt64s(values []int64) []int64 {
	seen := make(map[int64]struct{}, len(values))
	unique := make([]int64, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		unique = append(unique, value)
	}
	return unique
}

func (s *SQLiteStore) TrackClose() error {
	return s.db.Close()
}

func (s *SQLiteStore) TrackDeleteMany(
	space model.TenancySpace,
	items []model.TrackKey,
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
	payloadStmt, err := tx.Prepare(`DELETE FROM track_payload WHERE app_id=? AND tenancy_id=? AND bucket_id=? AND key=?`)
	if err != nil {
		return err
	}
	defer payloadStmt.Close()

	for _, item := range items {
		if _, err := payloadStmt.Exec(space.AppId, space.TenancyId, item.BucketID, item.Key); err != nil {
			return err
		}
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
	space model.TenancySpace,
	items map[int32][]model.TrackKeyValueItem,
) error {
	for _, bucketItems := range items {
		for _, item := range bucketItems {
			if err := model.ValidateTrackValue(item.Value); err != nil {
				return err
			}
		}
	}

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
			if err := setSQLiteTrackPayload(tx, space, bucketID, item.Key, item.Value.Payload); err != nil {
				return err
			}
		}
	}

	return tx.Commit()
}

func (s *SQLiteStore) TrackGetMany(
	space model.TenancySpace,
	keys map[int32][]string,
	opts model.TrackReadOptions,
) (map[int32]map[string]model.TrackValue, map[int32][]string, error) {

	values := make(map[int32]map[string]model.TrackValue)
	missing := make(map[int32][]string)

	// Keep a single read snapshot while querying multiple buckets and chunks.
	tx, err := s.db.Begin()
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback()

	for bucketID, bucketKeys := range keys {
		if len(bucketKeys) == 0 {
			continue
		}

		found := make(map[string]struct{})
		if values[bucketID] == nil {
			values[bucketID] = make(map[string]model.TrackValue)
		}

		for start := 0; start < len(bucketKeys); start += sqliteQueryChunkSize {
			end := start + sqliteQueryChunkSize
			if end > len(bucketKeys) {
				end = len(bucketKeys)
			}

			selectSQL := `SELECT t.key, t.value, t.tag, t.metric FROM track t`
			if opts.IncludePayload {
				selectSQL = `SELECT t.key, t.value, t.tag, t.metric, p.payload FROM track t
					LEFT JOIN track_payload p ON p.app_id=t.app_id AND p.tenancy_id=t.tenancy_id AND p.bucket_id=t.bucket_id AND p.key=t.key`
			}
			query := selectSQL + ` WHERE t.app_id=? AND t.tenancy_id=? AND t.bucket_id=?
				  AND t.key IN (` + placeholders(end-start) + `)
			`
			args := []any{space.AppId, space.TenancyId, bucketID}
			for _, key := range bucketKeys[start:end] {
				args = append(args, key)
			}

			rows, err := tx.Query(query, args...)
			if err != nil {
				return nil, nil, err
			}
			for rows.Next() {
				var key string
				var tv model.TrackValue
				var scanErr error
				if opts.IncludePayload {
					scanErr = rows.Scan(&key, &tv.Value, &tv.Tag, &tv.Metric, &tv.Payload)
				} else {
					scanErr = rows.Scan(&key, &tv.Value, &tv.Tag, &tv.Metric)
				}
				if scanErr != nil {
					rows.Close()
					return nil, nil, scanErr
				}
				values[bucketID][key] = tv
				found[key] = struct{}{}
			}
			if err := rows.Err(); err != nil {
				rows.Close()
				return nil, nil, err
			}
			if err := rows.Close(); err != nil {
				return nil, nil, err
			}
		}

		for _, k := range bucketKeys {
			if _, ok := found[k]; !ok {
				missing[bucketID] = append(missing[bucketID], k)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, nil, err
	}
	return values, missing, nil
}

func (s *SQLiteStore) TrackPut(
	space model.TenancySpace,
	bucketID int32,
	key string,
	value model.TrackValue,
) error {
	if err := model.ValidateTrackValue(value); err != nil {
		return err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.Exec(`
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
		value.Value,
		value.Tag,
		value.Metric,
	)
	if err != nil {
		return err
	}
	if err := setSQLiteTrackPayload(tx, space, bucketID, key, value.Payload); err != nil {
		return err
	}
	return tx.Commit()
}

type sqliteTrackExecer interface {
	Exec(query string, args ...any) (sql.Result, error)
}

func setSQLiteTrackPayload(db sqliteTrackExecer, space model.TenancySpace, bucketID int32, key string, payload []byte) error {
	if payload == nil {
		_, err := db.Exec(`DELETE FROM track_payload WHERE app_id=? AND tenancy_id=? AND bucket_id=? AND key=?`, space.AppId, space.TenancyId, bucketID, key)
		return err
	}
	_, err := db.Exec(`INSERT INTO track_payload (app_id, tenancy_id, bucket_id, key, payload)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(app_id, tenancy_id, bucket_id, key) DO UPDATE SET payload=excluded.payload`,
		space.AppId, space.TenancyId, bucketID, key, payload)
	return err
}
