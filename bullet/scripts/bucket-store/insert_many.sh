curl -X POST http://localhost:$1/bucket-store/insert-many \
  -H "Content-Type: application/json" \
  -d '{
    "appId": 1,
    "buckets": [
      {
        "bucketId": 42,
        "items": [
          {"key":"foo","value":123},
          {"key":"bar","value":456}
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
