# Warehouse

Warehouse stores immutable binary blobs independently of Depot. RAM, SQLite and PostgreSQL are
implemented; MongoDB and BoltDB return:
`ErrWarehouseUnsupported` (HTTP 501). RAM data disappears when the process exits.

Run a RAM server:

```sh
go run ./cmd/bullet -db-type ram -port 8080
```

Run a persistent SQLite server:

```sh
go run ./cmd/bullet -db-type sqlite -sqlite warehouse.db -port 8080
```

SQLite creates its Warehouse table automatically, including when opening an
existing Bullet database. Blob data and idempotency keys survive reopening.
Concurrent retries are arbitrated by a unique constraint scoped to the space.
Large batch reads are split internally to stay within SQLite parameter limits.

Run a persistent PostgreSQL server:

```sh
go run ./cmd/bullet -db-type postgresql -postgres 'postgres://user:password@localhost/bullet?sslmode=disable' -port 8080
```

PostgreSQL creates its Warehouse table automatically when opening the store.
It stores binary values as BYTEA and creation timestamps as integer nanoseconds.
Blob data and space-scoped idempotency keys persist across store instances;
concurrent retries are resolved by a database unique constraint. Large reads are
split internally without imposing a public batch limit.

All routes require `X-App-Id` and `X-Tenancy-Id`, using the existing tenancy model:

| Method | Route | JSON body | Successful response |
| --- | --- | --- | --- |
| POST | `/warehouse/blobs` | `{"put_id":"write-1","content_type":"text/plain","value":"aGVsbG8=","checksum":""}` | Blob, HTTP 200 |
| GET | `/warehouse/blobs/:id` | None | Blob, HTTP 200 |
| POST | `/warehouse/blobs/batch-get` | `{"ids":["blob-id"]}` | Object keyed by blob ID, HTTP 200 |

Byte values use standard JSON base64 encoding. A Blob has `id`, `put_id`,
`content_type`, `value`, `checksum`, and `created_at` (UTC timestamp).
IDs are opaque server-generated strings.

A nonempty PutID is required. PutID is scoped to the application and tenancy.
Retrying identical bytes, content type and checksum returns the original blob,
including its ID and creation time. Reusing PutID with different bytes or metadata
returns HTTP 409. Nil and empty byte slices represent the same empty content.
A new PutID creates a new blob, even for identical content. There is no update or
delete operation. Get returns HTTP 404 for a missing ID; GetMany omits missing IDs,
collapses duplicates and returns `{}` for no matches or an empty request.

Checksums are opaque metadata for now: they are neither calculated nor validated.
Payload and batch limits are deferred. RAM copies byte slices on writes and reads
so callers cannot modify stored data through returned values. Store methods take
`context.Context` and honor cancellation before accessing data.

`WarehouseStore` is embedded in the combined `Store` interface. External Store
implementations must add the three Warehouse methods (unsupported stubs suffice).
Existing Depot routes and data are unchanged.

The matching firbolg_clients change needs this unreleased Bullet code. For local
multi-repository development, create a temporary Go workspace containing both
repositories and set `GOWORK` to its absolute path. Before publishing the client,
release Bullet and update the client's Bullet dependency to that release; do not
publish a filesystem replace directive.
