curl -X POST http://localhost:$1/bucket/insert-many \
  -H "Content-Type: application/json" \
  -H "X-App-ID: 1" \
  -d '{
    "buckets": [
      {
        "bucketId": 42,
        "items": [
          {"key":"bar:1","value":1},
          {"key":"bar:1:a","value":10, "tag": 1},
          {"key":"bar:2","value":2, "metric": 123.45},
          {"key":"bar:2:a","value":20, "tag": 2, "metric": 456.78},
          {"key":"bar:3:a","value":20},
          {"key":"foo:1","value":1, "tag": 3},
          {"key":"foo:2","value":2}
        ]
      },
      {
        "bucketId": 43,
        "items": [
          {"key":"baz","value":789, "metric": 999.99}
        ]
      }
    ]
  }'