# core-proxy

SOCKS5 proxy engine dengan SSH worker pool, load balancing, rule engine, dan hot reload.

## Fitur
- SOCKS5 (NO AUTH & User/Pass) + CONNECT untuk IPv4/IPv6/FQDN
- SSH Worker state machine + reconnect exponential backoff + full jitter
- Load balancing: round-robin, least-conn, adaptive + hysteresis
- Rule engine: domain exact/suffix/keyword, CIDR, fallback match
- Domain list eksternal dengan hot reload (atomic swap)
- HTTP payload injector multi-segment (split, crlf, host, port)
- Control Plane REST API: /healthz, /api/v1/reload, /api/v1/workers
- Graceful shutdown

## Token payload
- [cr]        : CR
- [lf]        : LF
- [crlf]      : CRLF
- [host]      : host target
- [port]      : port target
- [host_port] : host:port
- [protocol]  : protokol (default HTTP/1.1)
- [split]     : pisah TCP segment
- [raw]       : CONNECT host:port HTTP/1.1 CRLF CRLF

## Build
    go mod tidy
    go vet ./...
    go build -o bin/core-proxy ./cmd/proxy

## Cross-compile arm64
    CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -ldflags="-s -w" -o bin/core-proxy-arm64 ./cmd/proxy

## Run
    ./bin/core-proxy -config config.yaml

## API
- GET  /healthz         - health check
- POST /api/v1/reload   - hot reload config
- GET  /api/v1/workers  - daftar worker

