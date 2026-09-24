# Bullet public contracts and client migration

This is a breaking Go API refactor. Bullet now owns the domain types, HTTP
representations, and tenant-scoped client interfaces. There are no compatibility
aliases at the old import paths. Existing HTTP routes and JSON representations
are preserved; a breaking Go release does not require a simultaneous wire-format
change.

## Package responsibilities

| Package | Contents | Used by |
| --- | --- | --- |
| `github.com/vixac/bullet/model` | Domain IDs, tenancy, values, records, options, limits, and sentinel errors | Stores, clients, applications |
| `github.com/vixac/bullet/protocol` | HTTP request/response bodies, header names, and explicit wire/domain conversions | REST adapters and HTTP handlers |
| `github.com/vixac/bullet/client` | Tenant-scoped interfaces: `Track`, `Depot`, `Grove`, `Ledger`, `Warehouse`, and their composite `Client` | Applications and client adapters |
| `github.com/vixac/bullet/store/store_interface` | Backend interfaces with explicit tenancy | Store implementations, embedded adapters, server wiring |
| `github.com/vixac/bullet/api` | Gin handlers and routing | Server wiring |

Production dependencies run from `client` and `store_interface` to `model`, and
from `protocol` to `model`. Neither domain types nor client contracts import Gin,
HTTP handlers, database drivers, or `firbolg_clients`. The existing Bullet Go
module remains the release unit; this refactor does not introduce submodules.

`model` does not specify JSON or database serialization. Do not marshal its
structs directly as HTTP bodies. `protocol` owns that representation, including
when its structs look similar to domain structs. Storage-specific codecs belong
to store implementations.

## Tenancy: one space per store call, one space per client

The client interfaces exactly mirror the backend interfaces, with the
`model.TenancySpace` argument removed. Method names, other arguments, result
shapes, and error identities match. Warehouse retains its existing context
argument; adding context to the other operations is a separate change.

For example:

```go
// Backend
TrackGet(space model.TenancySpace, bucketID int32, key string, opts model.TrackReadOptions) (model.TrackValue, error)
TrackMutate(space model.TenancySpace, req model.TrackMutation) (model.TrackMutationResult, error)

// Tenant-scoped client
TrackGet(bucketID int32, key string, opts model.TrackReadOptions) (model.TrackValue, error)
TrackMutate(req model.TrackMutation) (model.TrackMutationResult, error)
```

The following is a constructor/forwarding pattern for the future local adapter,
not an additional implementation provided by this step:

```go
type LocalTrack struct {
    store store_interface.TrackStore
    space model.TenancySpace
}

func NewLocalTrack(store store_interface.TrackStore, space model.TenancySpace) *LocalTrack {
    return &LocalTrack{store: store, space: space}
}

func (c *LocalTrack) TrackMutate(req model.TrackMutation) (model.TrackMutationResult, error) {
    return c.store.TrackMutate(c.space, req)
}
```

Implement the remaining `client.Track` methods by forwarding the same stored
space. Keep the space private and immutable; construct a new scoped client for
a different tenancy. Consumers can depend on the smallest subsystem interface
rather than the composite `client.Client`.

A REST adapter similarly receives `model.TenancySpace` in its constructor and
sets these headers for every request:

```go
request.Header.Set(protocol.AppIDHeader, strconv.FormatInt(int64(space.AppId), 10))
request.Header.Set(protocol.TenancyIDHeader, strconv.FormatInt(space.TenancyId, 10))
```

The HTTP handler extracts the headers and supplies tenancy once to the backend.
Tenancy is not repeated in body objects. In particular, `model.TrackPut` and
`model.TrackKey` have no space field: a `model.TrackMutation` belongs entirely to
the space supplied to `TrackMutate`. Mutation IDs retain their existing
store-wide deduplication scope; binding a client does not namespace mutation IDs.

Selecting a space does not authorize access to it. Authentication/authorization
and connection ownership are separate concerns. No client or data-store
interface includes `Close` or `TrackClose`; the application owns resource cleanup.

## Migrating firbolg_clients

First release Bullet with these packages, then update the Bullet dependency in
`firbolg_clients`. The latter's current v0.2.11 dependency cannot provide them.
The adapter migration can be released as a breaking `firbolg_clients` version.

Delete duplicated domain definitions and import the Bullet model types directly.
Replace client interface definitions with the interfaces in `bullet/client`,
or use those interfaces directly at constructor and consumer boundaries.
Prefer direct imports over creating a permanent second layer of aliases.

| Existing client definitions | New source / shape |
| --- | --- |
| `NodeID`, `TreeID`, `AggregateKey`, `AggregateValue`, `AggregateDeltas`, `MutationID`, `ChildPosition`, `NodeMetadata` | Same names in `model` |
| `PaginationParams`, `PaginationResult`, `NodeInfo`, `NodeWithDepth`, `DescendantOptions` | Same names in `model` |
| `BlobID`, `PutID`, `Blob`, `PutBlobRequest`, Warehouse errors | Same names in `model`; HTTP bodies use `protocol.Blob` and `protocol.PutBlobRequest` |
| Client-specific tenancy struct | `model.TenancySpace` |
| `TrackMutation`, `TrackMutationResult` | `model.TrackMutation`, `model.TrackMutationResult`; puts are `[]model.TrackPut`, deletes are `[]model.TrackKey` |
| `TrackDeleteValue` and server `TrackBucketKeyPair` domain usage | `model.TrackKey` |
| Client flat `TrackKeyValueItem` | `model.TrackKeyValueItem{Key: key, Value: model.TrackValue{Value: value, Tag: tag, Metric: metric, Payload: payload}}` |
| `TrackGetManyResponse` | Client method returns `values, missing, err`; both maps use `int32` bucket keys |
| Track request wrappers, prefix filter wrappers | Client methods take the corresponding store arguments directly; HTTP adapters use `protocol.Track*` bodies |
| Depot request/response wrappers | Client methods take the corresponding store arguments and return IDs, values, maps, and missing IDs directly; HTTP bodies remain in `protocol.Depot*` |
| Grove request/response wrappers | Client methods take tree/node IDs and options directly and return the corresponding domain values; HTTP bodies remain in `protocol.Grove*` |
| `LedgerAppendItem`, `LedgerRecord`, `LedgerSelector`, `LedgerPage` | Same names in `model`, with typed `LedgerID`, `LedgerAppendID`, and `LedgerPosition` |
| Ledger operation request/response wrappers | `client.Ledger` arguments/results; `protocol.Ledger*` types encode HTTP bodies |
| `ErrTrackMutationUnsupported` | `model.ErrTrackMutationUnsupported` |
| Backend error references under `store_interface` | Same names under `model` |

Client method names intentionally now match Bullet:

| Old client method | Canonical method |
| --- | --- |
| `TrackInsertOne` | `TrackPut` |
| `TrackGetManyByPrefix` | `GetItemsByKeyPrefix` |
| `TrackGetByManyPrefixes` | `GetItemsByKeyPrefixes` |
| `DepotGetOne` | `DepotGet` |
| `GroveCreateNode`, `GroveDeleteNode`, `GroveMoveNode` | `CreateNode`, `DeleteNode`, `MoveNode` |
| `GroveExists`, `GroveGetNodeInfo`, `GroveGetChildren`, `GroveGetAncestors`, `GroveGetDescendants` | `Exists`, `GetNodeInfo`, `GetChildren`, `GetAncestors`, `GetDescendants` |
| `GroveApplyAggregateMutation` | `ApplyAggregateMutation` |
| Other `GroveGet*` methods | Corresponding `Get*` method, retaining `Bulk` where present |

Track, Depot, Ledger, and Warehouse methods not listed above retain their names,
but wrapper-based arguments/results change to the backend's domain signatures.
The complete signatures are in `client/client.go`. Add compile-time assertions
for both adapters against `client.Client` after porting them.

### HTTP encoding remains explicit

* Track single writes and mutation puts encode `value` as a decimal string.
  Batch writes and reads use nested values with uppercase `Value`, `Tag`, and
  `Metric` fields. Nil nested metadata remains JSON `null`.
* `protocol.TrackGetManyResponse` uses string bucket keys in JSON. The scoped
  domain interface uses `int32` keys for both returned maps. Parse and validate
  those keys at the REST boundary.
* Track payloads are limited to 64 KiB of raw bytes. Point and explicit-key
  reads return them only when `TrackReadOptions.IncludePayload` is true; prefix
  queries never return payloads. Nil means no stored payload and a non-nil empty
  slice means a stored zero-byte payload. Track puts replace the complete value,
  so a nil payload removes any prior payload.
* Prefix queries return `protocol.TrackQueryResponse` with an `items` array;
  scoped clients return `[]model.TrackKeyValueItem`, not a batch-get wrapper.
* Ledger positions remain decimal strings on the wire and `model.LedgerPosition`
  in domain calls. Use `protocol.LedgerRecordResponse` instead of a private copy
  of `ledgerRecordWire`. Preserve parse errors when converting responses.
* Warehouse bytes remain base64 JSON, fields remain snake_case, and timestamps
  remain RFC3339 JSON strings. Use the Warehouse conversion helpers in `protocol`.
* Grove body field names remain snake_case. Tree/node IDs that belong in URL
  paths and options that belong in queries must be handled by the REST adapter;
  marshaling a domain request is not a substitute for building the endpoint.
* HTTP errors still use `protocol.ErrorResponse{Error: message}`. Sharing domain
  errors does not automatically reconstruct their identity from HTTP responses.

### Scope of this step

This change migrates Bullet's backend signatures, implementations, handlers, and
existing tests to the canonical packages. It provides client interfaces and
protocol models for `firbolg_clients` to consume. It does not modify that repo,
move its adapters, or implement a new local/REST adapter here.

The next step must explicitly port REST TrackMutate and resolve existing Grove
pagination/options gaps. It must also normalize empty results and repeated
bucket inputs consistently between local and REST paths. Unsupported options
must not be silently discarded. Structured HTTP error codes, cancellation for
non-Warehouse calls, service-layer validation, and STL compound transactions
remain separate behavior changes rather than being hidden inside this type move.

## Verification

`client/client_test.go` verifies each client interface has exactly the backend
methods, inputs, and outputs with one tenancy argument removed.
`protocol/protocol_test.go` checks fixed JSON examples independently of the
handlers, including large integer encoding, nested Track values, error bodies,
Warehouse conversion, and mutation bodies without per-item tenancy. Existing
handler and store suites exercise the migrated contracts end to end.
