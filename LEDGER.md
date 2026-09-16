# Ledger selection:

Ledger reads accept exactly one selector:

- `{"all": true}`: every ledger in the current tenancy space.
- `{"ledger_ids": ["orders", "payments"]}`: an explicit set of up to 100 ledger IDs.
- `{"prefix": "orders_"}`: every ledger whose ID starts with `orders_`.

Prefixes are literal and case-sensitive. For example, `orders_` matches
`orders_` and `orders_123`, but not `Orders_123` or `ordersX123`.
A prefix must follow ledger ID syntax: 1–128 ASCII characters, starting with
a letter or digit, followed by letters, digits, dots, underscores, or hyphens.
An empty prefix does not select all ledgers; use `all`.

Both POST read endpoints support the selector:

```json
{"prefix": "orders_", "limit": 100}
```

Send this body to `/ledger/read/backward` for descending position order or
`/ledger/read/forward` for ascending position order, with the usual
`X-App-Id` and `X-Tenancy-Id` headers.

Backward reads return `records` and an optional `next_cursor`. Pass that cursor
as `cursor` with the same prefix to continue the original snapshot. Forward
reads accept an exclusive `after_position` and optional inclusive
`through_position` (both decimal strings). Limits remain 1–1000; paginate to
retrieve every matching record.

Go callers use `store_interface.LedgerSelector{Prefix: "orders_"}`.
RAM, SQLite, and PostgreSQL support this selection. SQLite uses its existing
ledger ID index; PostgreSQL creates a byte-order ledger ID index on store
initialization. MongoDB and BoltDB do not currently support Ledger.
