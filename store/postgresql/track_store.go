package postgresql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/vixac/bullet/model"
)

func (s *PostgreSQLStore) TrackMutate(space model.TenancySpace, req model.TrackMutation) (model.TrackMutationResult, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return model.TrackMutationResult{}, err
	}
	defer tx.Rollback()

	result, err := tx.Exec(`INSERT INTO track_mutations (mutation_id) VALUES ($1) ON CONFLICT DO NOTHING`, req.MutationID)
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
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT(app_id, tenancy_id, bucket_id, key) DO UPDATE SET
			value=excluded.value, tag=excluded.tag, metric=excluded.metric`)
	if err != nil {
		return model.TrackMutationResult{}, err
	}
	defer upsertStmt.Close()
	insertStmt, err := tx.Prepare(`
		INSERT INTO track (app_id, tenancy_id, bucket_id, key, value, tag, metric)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
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
		if err := setPostgresTrackPayload(tx, space, put.BucketID, put.Key, put.Value.Payload); err != nil {
			return model.TrackMutationResult{}, err
		}
	}

	deleteStmt, err := tx.Prepare(`DELETE FROM track WHERE app_id=$1 AND tenancy_id=$2 AND bucket_id=$3 AND key=$4`)
	if err != nil {
		return model.TrackMutationResult{}, err
	}
	defer deleteStmt.Close()
	for _, key := range req.Deletes {
		if _, err := tx.Exec(`DELETE FROM track_payload WHERE app_id=$1 AND tenancy_id=$2 AND bucket_id=$3 AND key=$4`, space.AppId, space.TenancyId, key.BucketID, key.Key); err != nil {
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

func (s *PostgreSQLStore) TrackGet(
	space model.TenancySpace,
	bucketID int32,
	key string,
	opts model.TrackReadOptions,
) (model.TrackValue, error) {

	var value model.TrackValue
	query := `SELECT t.value, t.tag, t.metric FROM track t WHERE t.app_id=$1 AND t.tenancy_id=$2 AND t.bucket_id=$3 AND t.key=$4`
	var err error
	if opts.IncludePayload {
		query = `SELECT t.value, t.tag, t.metric, p.payload FROM track t
			LEFT JOIN track_payload p ON p.app_id=t.app_id AND p.tenancy_id=t.tenancy_id AND p.bucket_id=t.bucket_id AND p.key=t.key
			WHERE t.app_id=$1 AND t.tenancy_id=$2 AND t.bucket_id=$3 AND t.key=$4`
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

func (s *PostgreSQLStore) GetItemsByKeyPrefix(
	space model.TenancySpace,
	bucketID int32,
	prefix string,
	tags []int64,
	metricValue *float64,
	metricIsGt bool,
) ([]model.TrackKeyValueItem, error) {

	// PostgreSQL rejects U+FFFF (a Unicode noncharacter) so we use LIKE for prefix matching.
	query := `
		SELECT key, value, tag, metric
		FROM track
		WHERE app_id=$1 AND tenancy_id=$2 AND bucket_id=$3
		  AND key LIKE $4
	`
	args := []any{space.AppId, space.TenancyId, bucketID, pgLikePrefix(prefix)}
	argIdx := 5

	if len(tags) > 0 {
		query += " AND tag IN (" + placeholders(argIdx, len(tags)) + ")"
		argIdx += len(tags)
		for _, t := range tags {
			args = append(args, t)
		}
	}

	if metricValue != nil {
		if metricIsGt {
			query += fmt.Sprintf(" AND metric > $%d", argIdx)
		} else {
			query += fmt.Sprintf(" AND metric < $%d", argIdx)
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
		if err := rows.Scan(&item.Key, &item.Value.Value, &item.Value.Tag, &item.Value.Metric); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *PostgreSQLStore) GetItemsByKeyPrefixes(
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

	query := `
		SELECT key, value, tag, metric
		FROM track
		WHERE app_id=$1 AND tenancy_id=$2 AND bucket_id=$3
		  AND (
	`
	args := []any{space.AppId, space.TenancyId, bucketID}
	argIdx := 4

	for i, p := range prefixes {
		if i > 0 {
			query += " OR "
		}
		query += fmt.Sprintf("key LIKE $%d", argIdx)
		argIdx++
		args = append(args, pgLikePrefix(p))
	}
	query += ")"

	if len(tags) > 0 {
		query += " AND tag IN (" + placeholders(argIdx, len(tags)) + ")"
		argIdx += len(tags)
		for _, t := range tags {
			args = append(args, t)
		}
	}

	if metricValue != nil {
		if metricIsGt {
			query += fmt.Sprintf(" AND metric > $%d", argIdx)
		} else {
			query += fmt.Sprintf(" AND metric < $%d", argIdx)
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
		if err := rows.Scan(&item.Key, &item.Value.Value, &item.Value.Tag, &item.Value.Metric); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// pgLikePrefix converts a plain prefix string into a LIKE pattern by escaping
// any special LIKE characters and appending the wildcard.
func pgLikePrefix(prefix string) string {
	// Escape LIKE metacharacters in the prefix itself.
	escaped := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(prefix)
	return escaped + "%"
}

func (s *PostgreSQLStore) TrackClose() error {
	return s.db.Close()
}

func (s *PostgreSQLStore) TrackDeleteMany(
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
		WHERE app_id=$1 AND tenancy_id=$2 AND bucket_id=$3 AND key=$4
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	payloadStmt, err := tx.Prepare(`DELETE FROM track_payload WHERE app_id=$1 AND tenancy_id=$2 AND bucket_id=$3 AND key=$4`)
	if err != nil {
		return err
	}
	defer payloadStmt.Close()

	for _, item := range items {
		if _, err := payloadStmt.Exec(space.AppId, space.TenancyId, item.BucketID, item.Key); err != nil {
			return err
		}
		if _, err := stmt.Exec(space.AppId, space.TenancyId, item.BucketID, item.Key); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *PostgreSQLStore) TrackPutMany(
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
		INSERT INTO track (app_id, tenancy_id, bucket_id, key, value, tag, metric)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT(app_id, tenancy_id, bucket_id, key)
		DO UPDATE SET
			value  = excluded.value,
			tag    = excluded.tag,
			metric = excluded.metric
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for bucketID, bucketItems := range items {
		for _, item := range bucketItems {
			if _, err := stmt.Exec(
				space.AppId, space.TenancyId, bucketID,
				item.Key, item.Value.Value, item.Value.Tag, item.Value.Metric,
			); err != nil {
				return err
			}
			if err := setPostgresTrackPayload(tx, space, bucketID, item.Key, item.Value.Payload); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

func (s *PostgreSQLStore) TrackGetMany(
	space model.TenancySpace,
	keys map[int32][]string,
	opts model.TrackReadOptions,
) (map[int32]map[string]model.TrackValue, map[int32][]string, error) {

	values := make(map[int32]map[string]model.TrackValue)
	missing := make(map[int32][]string)

	// All bucket queries must see the same snapshot, not one per statement.
	tx, err := s.db.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback()

	for bucketID, bucketKeys := range keys {
		if len(bucketKeys) == 0 {
			continue
		}

		selectSQL := `SELECT t.key, t.value, t.tag, t.metric FROM track t`
		if opts.IncludePayload {
			selectSQL = `SELECT t.key, t.value, t.tag, t.metric, p.payload FROM track t
				LEFT JOIN track_payload p ON p.app_id=t.app_id AND p.tenancy_id=t.tenancy_id AND p.bucket_id=t.bucket_id AND p.key=t.key`
		}
		query := selectSQL + ` WHERE t.app_id=$1 AND t.tenancy_id=$2 AND t.bucket_id=$3
			  AND t.key IN (` + placeholders(4, len(bucketKeys)) + `)`

		args := []any{space.AppId, space.TenancyId, bucketID}
		for _, k := range bucketKeys {
			args = append(args, k)
		}

		rows, err := tx.Query(query, args...)
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

func (s *PostgreSQLStore) TrackPut(
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
		INSERT INTO track (app_id, tenancy_id, bucket_id, key, value, tag, metric)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT(app_id, tenancy_id, bucket_id, key)
		DO UPDATE SET
			value  = excluded.value,
			tag    = excluded.tag,
			metric = excluded.metric
	`, space.AppId, space.TenancyId, bucketID, key, value.Value, value.Tag, value.Metric)
	if err != nil {
		return err
	}
	if err := setPostgresTrackPayload(tx, space, bucketID, key, value.Payload); err != nil {
		return err
	}
	return tx.Commit()
}

type postgresTrackExecer interface {
	Exec(query string, args ...any) (sql.Result, error)
}

func setPostgresTrackPayload(db postgresTrackExecer, space model.TenancySpace, bucketID int32, key string, payload []byte) error {
	if payload == nil {
		_, err := db.Exec(`DELETE FROM track_payload WHERE app_id=$1 AND tenancy_id=$2 AND bucket_id=$3 AND key=$4`, space.AppId, space.TenancyId, bucketID, key)
		return err
	}
	_, err := db.Exec(`INSERT INTO track_payload (app_id, tenancy_id, bucket_id, key, payload)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT(app_id, tenancy_id, bucket_id, key) DO UPDATE SET payload=excluded.payload`,
		space.AppId, space.TenancyId, bucketID, key, payload)
	return err
}
