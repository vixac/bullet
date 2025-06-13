#!/bin/bash
cd "$(dirname "${BASH_SOURCE[0]}")"
./bullet/bullet_bin  -port $BULLET_PORT -db-type $BULLET_DB_TYPE -mongo $MONGO_PASS -bolt $BOLT_PATH
