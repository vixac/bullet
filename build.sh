#!/bin/bash
cd "$(dirname "${BASH_SOURCE[0]}")"
cd bullet
go build -o bullet_bin ./cmd/bullet
