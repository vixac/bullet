curl -X POST http://localhost:$1/bucket/get-many \
  -H "Content-Type: application/json" \
  -d '{
    "appId": 1,
    "buckets": [
      {
        "bucketId": 42,
        "keys": ["foo:1:a", "bar:1", "missingKey"]
      },
      {
        "bucketId": 43,
        "keys": ["baz"]
      }
    ]
  }'
