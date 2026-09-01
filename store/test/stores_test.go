package store_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/vixac/bullet/store/boltdb"
	"github.com/vixac/bullet/store/postgresql"
	"github.com/vixac/bullet/store/ram"
	sqlite_store "github.com/vixac/bullet/store/sqlite"
	"github.com/vixac/bullet/store/store_interface"
)

var trackStores = map[string]store_interface.TrackStore{}
var depotStores = map[string]store_interface.DepotStore{}
var groveStores = map[string]store_interface.GroveStore{}
var ledgerStores = map[string]store_interface.LedgerStore{}

func init() {
	ramStore := ram.NewRamStore()
	trackStores["ram"] = ramStore
	depotStores["ram"] = ramStore
	groveStores["ram"] = ramStore
	ledgerStores["ram"] = ramStore

	sqliteStore, err := sqlite_store.NewSQLiteStore(":memory:")
	if err != nil {
		panic(err)
	}
	trackStores["sqlite"] = sqliteStore
	depotStores["sqlite"] = sqliteStore
	groveStores["sqlite"] = sqliteStore
	ledgerStores["sqlite"] = sqliteStore

	boltStore, err := boltdb.NewBoltStore("test-grove.db")
	if err != nil {
		panic(err)
	}
	trackStores["boltdb"] = boltStore
	groveStores["boltdb"] = boltStore
	// Note: boltdb is not added to depotStores (not yet implemented for boltdb)
}

// TestMain starts a PostgreSQL container, wires it into all store maps, runs the
// test suite, then tears everything down.
func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	ctx := context.Background()

	pgC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:16",
			ExposedPorts: []string{"5432/tcp"},
			Env: map[string]string{
				"POSTGRES_USER":     "test",
				"POSTGRES_PASSWORD": "test",
				"POSTGRES_DB":       "test",
			},
			WaitingFor: wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
		},
		Started: true,
	})
	if err != nil {
		fmt.Printf("Failed to start PostgreSQL container: %v\n", err)
		return 1
	}
	defer pgC.Terminate(ctx)

	host, err := pgC.Host(ctx)
	if err != nil {
		fmt.Printf("Failed to get PostgreSQL container host: %v\n", err)
		return 1
	}
	port, err := pgC.MappedPort(ctx, "5432")
	if err != nil {
		fmt.Printf("Failed to get PostgreSQL mapped port: %v\n", err)
		return 1
	}

	dsn := fmt.Sprintf("postgres://test:test@%s:%s/test?sslmode=disable", host, port.Port())
	pgStore, err := postgresql.NewPostgreSQLStore(dsn)
	if err != nil {
		fmt.Printf("Failed to create PostgreSQL store: %v\n", err)
		return 1
	}

	trackStores["postgresql"] = pgStore
	depotStores["postgresql"] = pgStore
	groveStores["postgresql"] = pgStore
	ledgerStores["postgresql"] = pgStore

	code := m.Run()

	os.Remove("test-grove.db")
	os.Remove("test-track.db")
	os.Remove("test-depot.db")

	return code
}
