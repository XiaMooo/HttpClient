# Local HTTPS Benchmark - 2026-05-20

## Environment

- Host: local machine
- Server: `tools/go_compare/bench_https_server.go`
- URL: `https://127.0.0.1:8443/ping`
- TLS: self-signed local certificate, clients run with `--insecure`
- Proxy: disabled with `--no-proxy` where supported
- C++ compiler: detected by CMake as GNU 16.1.1
- Dependencies observed during configure:
  - libcurl 8.20.0
  - OpenSSL 3.6.2
  - libnghttp2 1.69.0

## Main Matrix

Command:

```bash
REQUESTS=5000 CONCURRENCY=32 ./scripts/run_bench_matrix.sh
```

| Runtime | C++ preset | Optimization | Protocol | Requests | Concurrency | OK | Fail | Wall ms | Notes |
|---|---:|---|---|---:|---:|---:|---:|---:|---|
| C++ libcurl multi | o2 | `-O2 -DNDEBUG` | HTTP/2 | 5000 | 32 | 5000 | 0 | 156 | `avg_reported_ms=0.513614` |
| C++ Asio/Beast | o2 | `-O2 -DNDEBUG` | HTTP/1.1 | 5000 | 32 | 5000 | 0 | 127 | keep-alive prototype |
| C++ Asio/nghttp2 | o2 | `-O2 -DNDEBUG` | HTTP/2 | 5000 | 32 | 5000 | 0 | 70 | H2 session serialized by strand |
| C++ libcurl multi | o3 | `-O3 -DNDEBUG` | HTTP/2 | 5000 | 32 | 5000 | 0 | 153 | `avg_reported_ms=0.502061` |
| C++ Asio/Beast | o3 | `-O3 -DNDEBUG` | HTTP/1.1 | 5000 | 32 | 5000 | 0 | 135 | keep-alive prototype |
| C++ Asio/nghttp2 | o3 | `-O3 -DNDEBUG` | HTTP/2 | 5000 | 32 | 5000 | 0 | 73 | H2 session serialized by strand |
| C++ libcurl multi | o0 | `-O0 -g` | HTTP/2 | 5000 | 32 | 5000 | 0 | 191 | `avg_reported_ms=0.555970` |
| C++ Asio/Beast | o0 | `-O0 -g` | HTTP/1.1 | 5000 | 32 | 5000 | 0 | 388 | keep-alive prototype |
| C++ Asio/nghttp2 | o0 | `-O0 -g` | HTTP/2 | 5000 | 32 | 5000 | 0 | 223 | H2 session serialized by strand |
| C++ libcurl multi | relwithdebinfo | `-O2 -g -DNDEBUG` | HTTP/2 | 5000 | 32 | 5000 | 0 | 160 | `avg_reported_ms=0.521148` |
| C++ Asio/Beast | relwithdebinfo | `-O2 -g -DNDEBUG` | HTTP/1.1 | 5000 | 32 | 5000 | 0 | 132 | keep-alive prototype |
| C++ Asio/nghttp2 | relwithdebinfo | `-O2 -g -DNDEBUG` | HTTP/2 | 5000 | 32 | 5000 | 0 | 69 | H2 session serialized by strand |
| Go net/http | n/a | Go default | HTTP/2 | 5000 | 32 | 5000 | 0 | 81 | `h1=0 h2=5000` |
| Go net/http | n/a | Go default | HTTP/1.1 | 5000 | 32 | 5000 | 0 | 49 | forced with `--http1`, `h1=5000 h2=0` |

## High Concurrency H2 Smoke

| Runtime | Build | Protocol | Requests | Concurrency | OK | Fail | Wall ms |
|---|---|---|---:|---:|---:|---:|---:|
| C++ Asio/nghttp2 | o2 | HTTP/2 | 20000 | 128 | 20000 | 0 | 308 |
| Go net/http | default | HTTP/2 | 20000 | 128 | 20000 | 0 | 326 |

## TLS Handshake And Connection Reuse

Command:

```bash
./build-o2/httpclient_tls_test https://127.0.0.1:8443/ping 8 --insecure --no-proxy
```

`http_version=3` is libcurl's enum value for HTTP/2.

| Round | Status | Total ms | DNS ms | TCP connect ms | TLS handshake ms | Pretransfer ms | TTFB ms | New connections | HTTP version |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0 | 200 | 3.487 | 0.057 | 0.200 | 3.254 | 3.318 | 3.456 | 1 | 3 |
| 1 | 200 | 0.175 | 0.000 | 0.000 | 0.000 | 0.040 | 0.152 | 0 | 3 |
| 2 | 200 | 0.397 | 0.000 | 0.000 | 0.000 | 0.023 | 0.383 | 0 | 3 |
| 3 | 200 | 0.171 | 0.000 | 0.000 | 0.000 | 0.025 | 0.158 | 0 | 3 |
| 4 | 200 | 0.101 | 0.000 | 0.000 | 0.000 | 0.017 | 0.081 | 0 | 3 |
| 5 | 200 | 0.096 | 0.000 | 0.000 | 0.000 | 0.019 | 0.087 | 0 | 3 |
| 6 | 200 | 0.110 | 0.000 | 0.000 | 0.000 | 0.015 | 0.090 | 0 | 3 |
| 7 | 200 | 0.077 | 0.000 | 0.000 | 0.000 | 0.017 | 0.065 | 0 | 3 |

Interpretation:

- Round 0 opens a new TCP/TLS connection.
- Rounds 1-7 reuse the existing connection: `num_connects=0`, `connect_ms=0`, `appconnect_ms=0`.
- ALPN negotiated HTTP/2.

## Notes

- These are localhost numbers against a trivial Go HTTPS server. They are useful for regression and runtime overhead comparisons, not for final internet-facing latency claims.
- `o2` is the baseline for future C++ comparisons.
- The current Asio/nghttp2 client uses one H2 session and serializes session access through a strand. Multi-origin sharding and per-core `io_context` are still future runtime work.
