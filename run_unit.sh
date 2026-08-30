#!/usr/bin/env bash
set -euo pipefail

# POSTGRESQL_DSN=hello BULLET_PORT=80 BULLET_DB_TYPE=sqlite BOLT_PATH=data.db SQLITE_PATH=test-sqlite.sqlite  ./run_unit.sh
 unit_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
 cd "$unit_dir"

: "${BULLET_PORT:?BULLET_PORT is required}"
: "${BULLET_DB_TYPE:?BULLET_DB_TYPE is required}"

exec "$unit_dir/bullet" \
  -port "$BULLET_PORT" \
  -db-type "$BULLET_DB_TYPE" \
  -mongo "${MONGO_PASS:-}" \
  -bolt "${BOLT_PATH:-}" \
  -sqlite "${SQLITE_PATH:-}" \
  -postgres "${POSTGRESQL_DSN:-}"