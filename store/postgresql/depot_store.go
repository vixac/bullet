package postgresql

import (
	"github.com/vixac/bullet/store/store_interface"
)

func (s *PostgreSQLStore) DepotCreate(
	space store_interface.TenancySpace,
	bucketID int32,
	value string,
) (int64, error) {

	var id int64
	err := s.db.QueryRow(`
		INSERT INTO depot (app_id, tenancy_id, bucket_id, value)
		VALUES ($1, $2, $3, $4)
		RETURNING id
	`, space.AppId, space.TenancyId, bucketID, value).Scan(&id)
	return id, err
}

func (s *PostgreSQLStore) DepotDeleteByBucket(
	space store_interface.TenancySpace,
	bucketID int32,
) error {

	_, err := s.db.Exec(`
		DELETE FROM depot
		WHERE app_id=$1 AND tenancy_id=$2 AND bucket_id=$3
	`, space.AppId, space.TenancyId, bucketID)
	return err
}

func (s *PostgreSQLStore) DepotCreateMany(
	space store_interface.TenancySpace,
	bucketID int32,
	values []string,
) ([]int64, error) {

	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO depot (app_id, tenancy_id, bucket_id, value)
		VALUES ($1, $2, $3, $4)
		RETURNING id
	`)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	ids := make([]int64, 0, len(values))
	for _, v := range values {
		var id int64
		if err := stmt.QueryRow(space.AppId, space.TenancyId, bucketID, v).Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return ids, nil
}

func (s *PostgreSQLStore) DepotUpdate(
	space store_interface.TenancySpace,
	id int64,
	value string,
) error {

	_, err := s.db.Exec(`
		UPDATE depot
		SET value=$1
		WHERE id=$2 AND app_id=$3 AND tenancy_id=$4
	`, value, id, space.AppId, space.TenancyId)
	return err
}

func (s *PostgreSQLStore) DepotDelete(
	space store_interface.TenancySpace,
	id int64,
) error {

	_, err := s.db.Exec(`
		DELETE FROM depot
		WHERE id=$1 AND app_id=$2 AND tenancy_id=$3
	`, id, space.AppId, space.TenancyId)
	return err
}

func (s *PostgreSQLStore) DepotGet(
	space store_interface.TenancySpace,
	id int64,
) (string, error) {

	var value string
	err := s.db.QueryRow(`
		SELECT value FROM depot
		WHERE id=$1 AND app_id=$2 AND tenancy_id=$3
	`, id, space.AppId, space.TenancyId).Scan(&value)
	return value, err
}

func (s *PostgreSQLStore) DepotGetMany(
	space store_interface.TenancySpace,
	ids []int64,
) (map[int64]string, []int64, error) {

	result := make(map[int64]string)
	if len(ids) == 0 {
		return result, nil, nil
	}

	query := `
		SELECT id, value FROM depot
		WHERE app_id=$1 AND tenancy_id=$2
		  AND id IN (` + placeholders(3, len(ids)) + `)`

	args := []any{space.AppId, space.TenancyId}
	for _, id := range ids {
		args = append(args, id)
	}

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	found := make(map[int64]struct{})
	for rows.Next() {
		var id int64
		var value string
		if err := rows.Scan(&id, &value); err != nil {
			return nil, nil, err
		}
		result[id] = value
		found[id] = struct{}{}
	}

	var missing []int64
	for _, id := range ids {
		if _, ok := found[id]; !ok {
			missing = append(missing, id)
		}
	}
	return result, missing, nil
}

func (s *PostgreSQLStore) DepotGetAllByBucket(
	space store_interface.TenancySpace,
	bucketID int32,
) (map[int64]string, error) {

	rows, err := s.db.Query(`
		SELECT id, value FROM depot
		WHERE app_id=$1 AND tenancy_id=$2 AND bucket_id=$3
	`, space.AppId, space.TenancyId, bucketID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[int64]string)
	for rows.Next() {
		var id int64
		var value string
		if err := rows.Scan(&id, &value); err != nil {
			return nil, err
		}
		result[id] = value
	}
	return result, nil
}
