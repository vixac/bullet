package sqlite_store

import (
	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/store/store_interface"
)

func (s *SQLiteStore) DepotPut(space store_interface.TenancySpace, key int64, value string) error {
	_, err := s.db.Exec(`
		INSERT INTO depot (app_id, tenancy_id, key, value)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(app_id, tenancy_id, key)
		DO UPDATE SET value=excluded.value
	`, space.AppId, space.TenancyId, key, value)
	return err
}

func (s *SQLiteStore) DepotGet(space store_interface.TenancySpace, key int64) (string, error) {
	var value string
	err := s.db.QueryRow(`
		SELECT value FROM depot
		WHERE app_id=? AND tenancy_id=? AND key=?
	`, space.AppId, space.TenancyId, key).Scan(&value)
	return value, err
}

func (s *SQLiteStore) DepotDelete(
	space store_interface.TenancySpace,
	key int64,
) error {
	_, err := s.db.Exec(`
		DELETE FROM depot
		WHERE app_id=? AND tenancy_id=? AND key=?
	`, space.AppId, space.TenancyId, key)
	return err
}

func (s *SQLiteStore) DepotPutMany(
	space store_interface.TenancySpace,
	items []model.DepotKeyValueItem,
) error {

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO depot (app_id, tenancy_id, key, value)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(app_id, tenancy_id, key)
		DO UPDATE SET value=excluded.value
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, item := range items {
		if _, err := stmt.Exec(
			space.AppId,
			space.TenancyId,
			item.Key,
			item.Value,
		); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (s *SQLiteStore) DepotGetMany(
	space store_interface.TenancySpace,
	keys []int64,
) (map[int64]string, []int64, error) {

	result := make(map[int64]string)
	if len(keys) == 0 {
		return result, nil, nil
	}

	query := `
		SELECT key, value
		FROM depot
		WHERE app_id=? AND tenancy_id=?
		  AND key IN (` + placeholders(len(keys)) + `)
	`

	args := []any{space.AppId, space.TenancyId}
	for _, k := range keys {
		args = append(args, k)
	}

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	found := make(map[int64]struct{})
	for rows.Next() {
		var k int64
		var v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, nil, err
		}
		result[k] = v
		found[k] = struct{}{}
	}

	var missing []int64
	for _, k := range keys {
		if _, ok := found[k]; !ok {
			missing = append(missing, k)
		}
	}

	return result, missing, nil
}
