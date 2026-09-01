#!/usr/bin/env bash
set -euo pipefail

#FIRBOLG_OUTPUT_DIR=./release ./package_unit.sh 


: "${FIRBOLG_OUTPUT_DIR:?FIRBOLG_OUTPUT_DIR is required}"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
output_dir="$FIRBOLG_OUTPUT_DIR"

# Firbolg creates and validates this directory before invoking the script.
if [[ ! -d "$output_dir" ]]; then
  echo "Bullet: Output directory does not exist: $output_dir" >&2
  exit 1
fi

cd "$repo_root"

go build \
  -buildvcs=false \
  -trimpath \
  -o "$output_dir/bullet" \
  ./cmd/bullet

install -m 0755 \
  "$repo_root/run_unit.sh" \
  "$output_dir/run_unit.sh"


