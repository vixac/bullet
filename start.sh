#!/bin/bash
cd "$(dirname "${BASH_SOURCE[0]}")"
cd bullet
go run ./cmd/bullet -port $BULLET_PORT -db-type $BULLET_DB_TYPE -mongo $MONGO_PASS -bolt $BOLT_PATH
