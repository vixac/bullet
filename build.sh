#!/bin/bash
cd "$(dirname "${BASH_SOURCE[0]}")"

if [ -z "$1" ]
  then
        echo "You must provide a binary name"
        exit 1
fi

cd bullet
#yea the binary needs to be build on the top level or whatever.
go build -o ../$1 ./cmd/bullet
