#!/bin/bash

PORT=$1

# Insert One
curl -X POST http://localhost:$PORT/depot/insert-one \
  -H "Content-Type: application/json" \
    -H "X-App-Id: 1" \
  -d '{
    "key": 123456,
    "value": "hello world"
  }'

echo -e "\n"

# Insert Many
curl -X POST http://localhost:$PORT/depot/insert-many \
  -H "Content-Type: application/json" \
    -H "X-App-Id: 1" \
  -d '{
    "items": [
      {"key": 100, "value": "alpha"},
      {"key": 200, "value": "beta"},
      {"key": 300, "value": "gamma"}
    ]
  }'

echo -e "\n"

# Get One
curl -X POST http://localhost:$PORT/depot/get-one \
  -H "Content-Type: application/json" \
    -H "X-App-Id: 1" \
  -d '{
    "key": 123456
  }'

echo -e "\n"

# Get Many
curl -X POST http://localhost:$PORT/depot/get-many \
  -H "Content-Type: application/json" \
    -H "X-App-Id: 1" \
  -d '{
    "keys": [100, 200, 400]
  }'

echo -e "\n"

# Delete One
curl -X POST http://localhost:$PORT/depot/delete-one \
  -H "Content-Type: application/json" \
    -H "X-App-Id: 1" \
  -d '{
    "key": 123456
  }'

echo -e "\n"
