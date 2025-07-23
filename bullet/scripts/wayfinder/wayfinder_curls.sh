#!/bin/bash

#unlike the other scripts in this repo, this one is hitting firbolg_gateway with jwt token. Thats probably not exactly what we want for this repo
PORT=$1
BASE_URL=http://localhost:$PORT/wayfinder
JWT_TOKEN=$2
echo "The port you passed in to hit bullet is $PORT. You can also hit firbolg_gateway locally if you do 80/bullet for the PORT"
echo "These wayfinder calls require a jwtoken. You passed in '$JWT_TOKEN'"

# Insert One
echo "Insert One"
curl -X POST $BASE_URL/insert-one \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $JWT_TOKEN" \
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
  -H "Authorization: Bearer $JWT_TOKEN" \
  -d '{
    "bucketId": 42,
    "key": "foo:123"
  }'
echo -e "\n"

# Query by Prefix (should match)
echo "Query by Prefix (match)"
curl -X POST $BASE_URL/query-by-prefix \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $JWT_TOKEN" \
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
  -H "Authorization: Bearer $JWT_TOKEN" \
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
  -H "Authorization: Bearer $JWT_TOKEN" \
  -d '{
    "bucketId": 42,
    "key": "nonexistent-key"
  }'
echo -e "\n"
