FROM golang:1.24.1-alpine as builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . ./

RUN go build -o bullet ./cmd/bullet

# lightweight runtime container
FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/bullet /app/bullet

EXPOSE 9002

ENTRYPOINT ["/app/bullet"]
CMD ["-port", "9002", "-db-type", "mongodb", "-mongo", "mongodb://localhost:27017", "-bolt", "/data/bullet.db"]
