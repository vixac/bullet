#!/bin/bash
cd "$(dirname "${BASH_SOURCE[0]}")"

if [ -z "$1" ]
  then
        echo "You must provide a binary name"
        exit 1
fi

echo "Building bullet binary: '$1'"
#yea the binary needs to be build on the top level or whatever.
go build -buildvcs=false -o $1 ./cmd/bullet

