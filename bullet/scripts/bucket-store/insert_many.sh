curl -X POST http://localhost:$1/bucket/insert-many \
  -H "Content-Type: application/json" \
  -d '{
    "appId": 1,
    "buckets": [
      {
        "bucketId": 42,
        "items": [
          {"key":"bar:1","value":1},
          {"key":"bar:1:a","value":10},
          {"key":"bar:2","value":2},
          {"key":"bar:2:a","value":20},
          {"key":"bar:3:a","value":20},
          {"key":"foo:1","value":1},
          {"key":"foo:2","value":2}
        ]
      },
      {
        "bucketId": 43,
        "items": [
          {"key":"baz","value":789}
        ]
      }
    ]
  }'
