curl -X POST http://localhost:$1/get-many \
  -H "Content-Type: application/json" \
  -d '{
    "appId": 1,
    "buckets": [
      {
        "bucketId": 42,
        "keys": ["foo", "bar", "missingKey"]
      },
      {
        "bucketId": 43,
        "keys": ["baz"]
      }
    ]
  }'
