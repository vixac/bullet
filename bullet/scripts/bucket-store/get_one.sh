echo "port is $1"
curl -X POST http://localhost:$1/bucket/get-one \
  -H "Content-Type: application/json" \
  -d '{"appId":1,"bucketId":42,"key":"mykey"}'

