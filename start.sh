#!/bin/bash
# ./build.sh .bullet_bin && BULLET_PORT=80 BULLET_DB_TYPE=mongodb BOLT_PATH=data.db ./start.sh .bullet_bin

#cd bullet then...
# go run ./cmd/bullet -bolt boltdb -mongo $MONGO_PASS -db-type mongodb -port 10 
if [ -z "$1" ]
  then
        echo "You must provide a binary name"
        exit 1
fi
echo "Bullet starting on $1 and we are in $(eval pwd)"

./$1 -port $BULLET_PORT -db-type $BULLET_DB_TYPE -mongo $MONGO_PASS -bolt $BOLT_PATH
