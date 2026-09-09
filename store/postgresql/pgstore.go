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
	fmt.Printf("VX: staring new postres store with dsn <redacted lol> \n")
	if dsn == "" {
		return nil, errors.New("postgresql DSN is empty")
	}

	fmt.Printf("VX: attampting to open \n")
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		fmt.Printf("VX: optn failed %s\n", err.Error())
		return nil, err
	}
	fmt.Printf("VX:attempting to ping\n")
	if err := db.Ping(); err != nil {
		fmt.Printf("VX: Ping failed %s\n", err.Error())
		return nil, fmt.Errorf("postgresql ping failed: %w", err)
	}
	store := &PostgreSQLStore{db: db}
	fmt.Printf("VX:attempting to init\n")
	if err := store.initSchema(); err != nil {
		fmt.Printf("VX: init schema failed %s\n", err.Error())
		return nil, err
	}
	fmt.Printf("Postgres store created.")
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

		`CREATE TABLE IF NOT EXISTS track_mutations (
			mutation_id TEXT PRIMARY KEY
		);`,

		`CREATE TABLE IF NOT EXISTS depot (
			id BIGSERIAL PRIMARY KEY,
			app_id INTEGER NOT NULL,
			tenancy_id BIGINT NOT NULL,
			bucket_id INTEGER NOT NULL,
			value TEXT NOT NULL
		);`,

		`CREATE INDEX IF NOT EXISTS depot_space_bucket_idx
		 ON depot(app_id, tenancy_id, bucket_id);`,

		`CREATE TABLE IF NOT EXISTS ledger_positions (
			app_id INTEGER NOT NULL,
			tenancy_id BIGINT NOT NULL,
			next_position BIGINT NOT NULL,
			PRIMARY KEY (app_id, tenancy_id)
		);`,

		`CREATE TABLE IF NOT EXISTS ledger (
			app_id INTEGER NOT NULL,
			tenancy_id BIGINT NOT NULL,
			ledger_id TEXT NOT NULL,
			position BIGINT NOT NULL,
			append_id TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL,
			payload TEXT NOT NULL,
			payload_hash BYTEA NOT NULL,
			PRIMARY KEY (app_id, tenancy_id, position),
			UNIQUE (app_id, tenancy_id, ledger_id, append_id)
		);`,

		`CREATE INDEX IF NOT EXISTS ledger_space_ledger_position_idx
		 ON ledger(app_id, tenancy_id, ledger_id, position);`,

		`CREATE INDEX IF NOT EXISTS ledger_space_position_idx
		 ON ledger(app_id, tenancy_id, position);`,

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
