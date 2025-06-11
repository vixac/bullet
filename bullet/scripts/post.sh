curl -X POST http://localhost:8080/put \
  -H "Content-Type: application/json" \
  -d '{"appId":1,"bucketId":42,"key":"mykey","value":123456}'
