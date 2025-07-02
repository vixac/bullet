#!/bin/bash

# Usage: ./get-items.sh 8080

if [ -z "$1" ]; then
  echo "Usage: $0 <port>"
  exit 1
fi

PORT=$1

# Sample JSON payload
JSON_PAYLOAD=$(cat <<EOF
{
  "bucketId": 42,
  "prefix": "bar:1"
}
EOF
)

# Execute the POST request
curl -X POST "http://localhost:$PORT/bullet/track/get-query" \
  -H "Content-Type: application/json" \
    -H "X-App-ID: 1" \
  -d "$JSON_PAYLOAD" \
  -w "\nHTTP status: %{http_code}\n" \
  --silent --show-error
