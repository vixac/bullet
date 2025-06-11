#!/bin/bash
#dev:  BULLET_PORT=80 BULLET_DB_TYPE=mongodb BOLT_PATH=data.db ./start.sh


cd "$(dirname "${BASH_SOURCE[0]}")"
cd bullet
go run ./cmd/bullet -port $BULLET_PORT -db-type $BULLET_DB_TYPE -mongo $MONGO_PASS -bolt $BOLT_PATH
