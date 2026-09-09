package sqlite_store

import (
	"database/sql"
	"errors"

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
	if path == "" {
		return nil, errors.New("your sqlite path is empty.")
	}
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

		`CREATE TABLE IF NOT EXISTS track_mutations (
			mutation_id TEXT PRIMARY KEY
		);`,

		`CREATE TABLE IF NOT EXISTS depot (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			app_id INTEGER NOT NULL,
			tenancy_id INTEGER NOT NULL,
			bucket_id INTEGER NOT NULL,
			value TEXT NOT NULL
		);`,

		`CREATE INDEX IF NOT EXISTS depot_space_bucket_idx
		 ON depot(app_id, tenancy_id, bucket_id);`,

		`CREATE TABLE IF NOT EXISTS ledger_positions (
			app_id INTEGER NOT NULL,
			tenancy_id INTEGER NOT NULL,
			next_position INTEGER NOT NULL,
			PRIMARY KEY (app_id, tenancy_id)
		);`,

		`CREATE TABLE IF NOT EXISTS ledger (
			app_id INTEGER NOT NULL,
			tenancy_id INTEGER NOT NULL,
			ledger_id TEXT NOT NULL,
			position INTEGER NOT NULL,
			append_id TEXT NOT NULL,
			created_at_ns INTEGER NOT NULL,
			payload TEXT NOT NULL,
			payload_hash BLOB NOT NULL,
			PRIMARY KEY (app_id, tenancy_id, position),
			UNIQUE (app_id, tenancy_id, ledger_id, append_id)
		);`,

		`CREATE INDEX IF NOT EXISTS ledger_space_ledger_position_idx
		 ON ledger(app_id, tenancy_id, ledger_id, position);`,

		`CREATE INDEX IF NOT EXISTS ledger_space_position_idx
		 ON ledger(app_id, tenancy_id, position);`,

		// Grove tables
		`CREATE TABLE IF NOT EXISTS grove_nodes (
			app_id INTEGER,
			tenancy_id INTEGER,
			tree_id TEXT,
			node_id TEXT,
			parent_id TEXT,
			position REAL,
			metadata TEXT,
			is_deleted BOOLEAN DEFAULT 0,
			PRIMARY KEY (app_id, tenancy_id, tree_id, node_id)
		);`,

		`CREATE INDEX IF NOT EXISTS grove_nodes_parent_idx
		 ON grove_nodes(app_id, tenancy_id, tree_id, parent_id) WHERE is_deleted = 0;`,

		// Closure table for efficient tree traversal
		`CREATE TABLE IF NOT EXISTS grove_closure (
			app_id INTEGER,
			tenancy_id INTEGER,
			tree_id TEXT,
			ancestor_id TEXT,
			descendant_id TEXT,
			depth INTEGER,
			PRIMARY KEY (app_id, tenancy_id, tree_id, ancestor_id, descendant_id)
		);`,

		`CREATE INDEX IF NOT EXISTS grove_closure_descendant_idx
		 ON grove_closure(app_id, tenancy_id, tree_id, descendant_id);`,

		// Mutation tracking for idempotency
		`CREATE TABLE IF NOT EXISTS grove_mutations (
			app_id INTEGER,
			tenancy_id INTEGER,
			tree_id TEXT,
			node_id TEXT,
			mutation_id TEXT,
			PRIMARY KEY (app_id, tenancy_id, tree_id, node_id, mutation_id)
		);`,

		// Aggregates storage
		`CREATE TABLE IF NOT EXISTS grove_aggregates (
			app_id INTEGER,
			tenancy_id INTEGER,
			tree_id TEXT,
			node_id TEXT,
			aggregate_key TEXT,
			aggregate_value INTEGER,
			PRIMARY KEY (app_id, tenancy_id, tree_id, node_id, aggregate_key)
		);`,

		`CREATE INDEX IF NOT EXISTS grove_aggregates_key_idx
		 ON grove_aggregates(app_id, tenancy_id, tree_id, aggregate_key);`,
	}

	for _, stmt := range schema {
		if _, err := s.db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}
