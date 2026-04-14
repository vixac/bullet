package postgresql

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type PostgreSQLStore struct {
	db *sql.DB
}

// placeholders generates "$start,$start+1,...,$start+n-1" for PostgreSQL parameterized queries.
func placeholders(start, n int) string {
	parts := make([]string, n)
	for i := 0; i < n; i++ {
		parts[i] = "$" + strconv.Itoa(start+i)
	}
	return strings.Join(parts, ",")
}

func NewPostgreSQLStore(dsn string) (*PostgreSQLStore, error) {
	if dsn == "" {
		return nil, errors.New("postgresql DSN is empty")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("postgresql ping failed: %w", err)
	}
	store := &PostgreSQLStore{db: db}
	if err := store.initSchema(); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *PostgreSQLStore) initSchema() error {
	schema := []string{
		`CREATE TABLE IF NOT EXISTS track (
			app_id INTEGER,
			tenancy_id BIGINT,
			bucket_id INTEGER,
			key TEXT,
			value BIGINT,
			tag BIGINT,
			metric DOUBLE PRECISION,
			PRIMARY KEY (app_id, tenancy_id, bucket_id, key)
		);`,

		`CREATE INDEX IF NOT EXISTS track_prefix_idx
		 ON track(app_id, tenancy_id, bucket_id, key);`,

		`CREATE TABLE IF NOT EXISTS depot (
			id BIGSERIAL PRIMARY KEY,
			app_id INTEGER NOT NULL,
			tenancy_id BIGINT NOT NULL,
			bucket_id INTEGER NOT NULL,
			value TEXT NOT NULL
		);`,

		`CREATE INDEX IF NOT EXISTS depot_space_bucket_idx
		 ON depot(app_id, tenancy_id, bucket_id);`,

		`CREATE TABLE IF NOT EXISTS grove_nodes (
			app_id INTEGER,
			tenancy_id BIGINT,
			tree_id TEXT,
			node_id TEXT,
			parent_id TEXT,
			position DOUBLE PRECISION,
			metadata TEXT,
			is_deleted BOOLEAN DEFAULT FALSE,
			PRIMARY KEY (app_id, tenancy_id, tree_id, node_id)
		);`,

		`CREATE INDEX IF NOT EXISTS grove_nodes_parent_idx
		 ON grove_nodes(app_id, tenancy_id, tree_id, parent_id) WHERE is_deleted = FALSE;`,

		`CREATE TABLE IF NOT EXISTS grove_closure (
			app_id INTEGER,
			tenancy_id BIGINT,
			tree_id TEXT,
			ancestor_id TEXT,
			descendant_id TEXT,
			depth INTEGER,
			PRIMARY KEY (app_id, tenancy_id, tree_id, ancestor_id, descendant_id)
		);`,

		`CREATE INDEX IF NOT EXISTS grove_closure_descendant_idx
		 ON grove_closure(app_id, tenancy_id, tree_id, descendant_id);`,

		`CREATE TABLE IF NOT EXISTS grove_mutations (
			app_id INTEGER,
			tenancy_id BIGINT,
			tree_id TEXT,
			node_id TEXT,
			mutation_id TEXT,
			PRIMARY KEY (app_id, tenancy_id, tree_id, node_id, mutation_id)
		);`,

		`CREATE TABLE IF NOT EXISTS grove_aggregates (
			app_id INTEGER,
			tenancy_id BIGINT,
			tree_id TEXT,
			node_id TEXT,
			aggregate_key TEXT,
			aggregate_value BIGINT,
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
