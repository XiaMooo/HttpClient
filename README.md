# HttpClient Runtime

这是一个 C++ HTTP client runtime 实验仓库，并附带 Go 对照压测程序。

当前主线是自研 `httpclient::HttpClient`：

- `httpclient_core`：`Request` / `Response` / `RequestBuilder` / form/multipart helper。
- `Boost.Asio + Beast`：HTTP/1.1 keep-alive、TLS pool、origin shard。
- `Boost.Asio + nghttp2`：HTTP/2 session、stream、frame、HPACK、flow control。
- `HttpClient`：自动 H1/H2 路由、ALPN/protocol cache、连接复用、调度。

`libcurl multi` 已从主线剥离为可选 baseline。默认构建不会查找或链接 libcurl；需要对照 benchmark 时显式打开 `HTTPCLIENT_ENABLE_CURL_BASELINE=ON`。

## 目标

- 先做一个稳定可跑的高性能 HTTP client 基线
- 支持异步请求、连接复用、HTTP/2
- 预留后续切换到新框架的空间

## 构建 C++

```bash
cmake -S . -B build
cmake --build build -j
```

也可以使用固定优化等级 preset：

```bash
cmake --preset o2 && cmake --build --preset o2 -j
cmake --preset o3 && cmake --build --preset o3 -j
cmake --preset o0 && cmake --build --preset o0 -j
cmake --preset relwithdebinfo && cmake --build --preset relwithdebinfo -j
```

其中 `o2` 是 C++ benchmark 的 baseline；`relwithdebinfo` 明确使用 `-O2 -g -DNDEBUG`。

## 运行 smoke test

短回归测试会启动本地 HTTPS H2/H1 server，并检查 `HttpClient` 的
H1-only、H2-only、mixed、reset-after-warmup 路径：

```bash
cmake --preset o2
cmake --build --preset o2 -j
ctest --test-dir build-o2 --output-on-failure
```

也可以直接运行脚本：

```bash
scripts/smoke_httpclient_local.sh build-o2
```

这个测试只验证路径正确性和基本统计，不作为性能结论。

## 运行 C++ benchmark

```bash
./build/httpclient_bench --url https://example.com --requests 32
```

HTTP/1.1 Asio/Beast：

```bash
./build/asio_httpclient_bench --url https://127.0.0.1:8443/ping --requests 1000 --concurrency 16 --insecure --no-proxy
```

HTTP/2 Asio/nghttp2：

```bash
./build/h2_httpclient_bench --url https://127.0.0.1:8443/ping --requests 1000 --concurrency 16 --insecure
```

初始化参数见 [docs/configuration.md](docs/configuration.md)，其中包含 TTL、LRU 容量、H1 shard、H2 session 和探测策略的默认值与 benchmark 映射。

libcurl baseline：

```bash
cmake --preset o2 -DHTTPCLIENT_ENABLE_CURL_BASELINE=ON
cmake --build --preset o2 --target curl_httpclient_bench -j
./build-o2/curl_httpclient_bench --url https://example.com --requests 32
```

## 构造请求

HTTP runtime 只负责传输，不负责 JSON/Protobuf/XML 等内容的序列化和解析。
`RequestBuilder::json()` 只设置 `Content-Type: application/json` 并携带调用方给定的字符串。

```cpp
auto json_req = httpclient::RequestBuilder::post(url)
    .json(R"({"name":"alice"})")
    .accept("application/json")
    .build();

auto form_req = httpclient::RequestBuilder::post(url)
    .form_urlencoded({{"name", "alice"}, {"city", "hello world"}})
    .build();

auto proto_req = httpclient::RequestBuilder::post(url)
    .bytes(std::move(serialized_proto), "application/x-protobuf")
    .build();
```

multipart/form-data 接收调用方已经准备好的字段和文件内容 bytes，不读取文件系统：

```cpp
std::vector<httpclient::MultipartPart> parts{
    {.name = "name", .value = "alice"},
    {.name = "file", .value = bytes, .filename = "a.bin",
     .content_type = "application/octet-stream"},
};

auto req = httpclient::RequestBuilder::post(url).multipart(parts).build();
```

## 运行优化等级矩阵

先启动本地 HTTPS server：

```bash
cd tools/go_compare
go run bench_https_server.go
```

再在仓库根目录运行：

```bash
REQUESTS=1000 CONCURRENCY=16 ./scripts/run_bench_matrix.sh
```

默认会按 `o2 o3 o0 relwithdebinfo` 构建并测试 C++，然后运行 Go 默认 HTTP/2 和 Go 强制 HTTP/1.1。可以用环境变量调节：

```bash
URL=https://127.0.0.1:8443/ping \
REQUESTS=5000 \
CONCURRENCY=32 \
PRESETS="o2 o3" \
RUN_ASIO_H1=0 \
./scripts/run_bench_matrix.sh
```

## 运行 mixed protocol benchmark

用于对比 `HttpClient` 与 Go `net/http` 在混合 H1/H2、TLS 连接复用、
strict ALPN detect 下的表现：

```bash
REQUESTS=10000 \
CONCURRENCY=128 \
DELAY_MS=5 \
RESPONSE_BYTES=1024 \
BODY_CASES='0 1024' \
WARMUP_PER_URL=128 \
CONCURRENT_WARMUP=1 \
STRICT_DETECT=1 \
H2_SESSIONS=2 \
scripts/run_mixed_protocol_bench.sh
```

## 运行 HTTPS/TLS 复用测试

```bash
cmake --preset o2 -DHTTPCLIENT_ENABLE_CURL_BASELINE=ON
cmake --build --preset o2 --target curl_httpclient_tls_test -j
./build-o2/curl_httpclient_tls_test https://example.com 4
```

这是 libcurl baseline 的 TLS 复用测试，会按顺序打 4 次同一个 HTTPS 地址，输出 `appconnect_ms`、`num_connects`、`http_version` 等字段。

## 运行 Go 对照 server

```bash
cd tools/go_compare
go run bench_server.go
```

## 运行 Go 对照 HTTPS server

```bash
cd tools/go_compare
go run bench_https_server.go
```

## 运行 Go 对照 client

```bash
cd tools/go_compare
go run bench_client.go --url http://127.0.0.1:8080/ping --requests 32 --concurrency 8
```

## 本地 TLS 复用测试建议

1. 启动 `go run bench_https_server.go`
2. 运行 `./build-o2/curl_httpclient_tls_test https://127.0.0.1:8443/ping 4 --insecure`
3. 观察首个请求的 `appconnect_ms` 与后续请求的 `appconnect_ms` / `num_connects`

## 后续路线

1. `O2` 作为 C++ baseline，持续和 Go 默认 HTTP/2、Go HTTP/1.1 对照。
2. `libcurl multi` 只作为可选 benchmark baseline 保留，不进入主线 API。
3. 继续完善自研 runtime：origin 分片、per-core `io_context`、连接池、TLS 连接复用、请求排队、超时取消、对象池。
4. HTTP/2 session 内部必须通过 strand 串行访问，避免显式锁保护 nghttp2 session、stream map 和 TLS 写队列。
