#!/bin/bash

PORT=$1
BASE_URL=http://localhost:$PORT/wayfinder
API_KEY=$2
echo "The port you passed in to hit bullet is $PORT. You can also hit firbolg_gateway locally if you do 80/bullet for the PORT"
echo "These wayfinder calls require an api key. You passed in '$API_KEY'"

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
