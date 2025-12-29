package sqlite_store

import (
	"database/sql"

	_ "github.com/mattn/go-sqlite3"
)

type SQLiteStore struct {
	db *sql.DB
}

func placeholders(n int) string {
	s := "?"
	for i := 1; i < n; i++ {
		s += ",?"
	}
	return s
}

//VX:TODO
//PRAGMA foreign_keys = ON;
//PRAGMA temp_store = MEMORY;

func NewSQLiteStore(path string) (*SQLiteStore, error) {
	db, err := sql.Open("sqlite3", path+"?_journal_mode=WAL&_synchronous=NORMAL")
	if err != nil {
		return nil, err
	}

	store := &SQLiteStore{db: db}
	if err := store.initSchema(); err != nil {
		return nil, err
	}

	return store, nil
}

func (s *SQLiteStore) initSchema() error {
	schema := []string{
		`CREATE TABLE IF NOT EXISTS track (
			app_id INTEGER,
			tenancy_id INTEGER,
			bucket_id INTEGER,
			key TEXT,
			value INTEGER,
			tag INTEGER,
			metric REAL,
			PRIMARY KEY (app_id, tenancy_id, bucket_id, key)
		);`,

		`CREATE INDEX IF NOT EXISTS track_prefix_idx
		 ON track(app_id, tenancy_id, bucket_id, key);`,

		`CREATE TABLE IF NOT EXISTS depot (
			app_id INTEGER,
			tenancy_id INTEGER,
			key INTEGER,
			value TEXT,
			PRIMARY KEY (app_id, tenancy_id, key)
		);`,

		`CREATE TABLE IF NOT EXISTS wayfinder (
			item_id INTEGER PRIMARY KEY AUTOINCREMENT,
			app_id INTEGER,
			tenancy_id INTEGER,
			bucket_id INTEGER,
			key TEXT,
			payload TEXT,
			tag INTEGER,
			metric REAL
		);`,
	}

	for _, stmt := range schema {
		if _, err := s.db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}
