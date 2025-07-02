echo "port is $1"
curl -X POST http://localhost:$1/track/get-one \
  -H "Content-Type: application/json" \
    -H "X-App-ID: 1" \
  -d '{"bucketId":42,"key":"mykey"}'

