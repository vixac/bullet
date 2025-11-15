echo "port is $1"
curl -X POST http://localhost:$1/track/insert-one \
  -H "Content-Type: application/json" \
    -H "X-App-Id: 1" \
  -d '{"bucketId":42,"key":"mykey","value":123456, "tag": 2, "metric": 456.78}'
