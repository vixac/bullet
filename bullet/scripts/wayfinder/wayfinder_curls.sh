#!/bin/bash

PORT=$1
BASE_URL=http://localhost:$PORT/wayfinder
API_KEY="yGKZICaw6ebzrb83nilyoOUVYNWyNC20PDBy4ctDi0Phbc9LhRBK9bw3WsJcW7fksx6"

# Insert One
echo "Insert One"
curl -X POST $BASE_URL/insert-one \
  -H "Content-Type: application/json" \
  -H "X-Api-Key: $API_KEY" \
  -d '{
    "bucketId": 42,
    "key": "foo:123",
    "payload": "this-is-a-payload"
  }'
echo -e "\n"

# Get One (should succeed)
echo "Get One"
curl -X POST $BASE_URL/get-one \
  -H "Content-Type: application/json" \
  -H "X-Api-Key: $API_KEY" \
  -d '{
    "bucketId": 42,
    "key": "foo:123"
  }'
echo -e "\n"

# Query by Prefix (should match)
echo "Query by Prefix (match)"
curl -X POST $BASE_URL/query-by-prefix \
  -H "Content-Type: application/json" \
  -H "X-Api-Key: $API_KEY" \
  -d '{
    "bucketId": 42,
    "prefix": "foo:",
    "metricIsGt": true
  }'
echo -e "\n"

# Query by Prefix (no match)
echo "Query by Prefix (no match)"
curl -X POST $BASE_URL/query-by-prefix \
  -H "Content-Type: application/json" \
  -H "X-Api-Key: $API_KEY" \
  -d '{
    "bucketId": 42,
    "prefix": "bar:",
    "metricIsGt": true
  }'
echo -e "\n"

# Negative Get One (nonexistent key)
echo "Get One (missing)"
curl -X POST $BASE_URL/get-one \
  -H "Content-Type: application/json" \
  -H "X-Api-Key: $API_KEY" \
  -d '{
    "bucketId": 42,
    "key": "nonexistent-key"
  }'
echo -e "\n"
