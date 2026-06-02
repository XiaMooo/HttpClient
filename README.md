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

常用 `httpx` 风格能力：

```cpp
httpclient::HttpClient::Options options;
options.base_url = "https://api.example.com/v1/";
options.default_headers.push_back("User-Agent: httpclient");
options.default_query_params.push_back({"locale", "zh-CN"});
options.follow_redirects = true;
options.max_redirects = 20;
options.max_retries = 2;
options.retry_backoff = std::chrono::milliseconds(50);
options.enable_cookie_jar = true;
options.max_cookie_domains = 1024;
options.max_cookies_per_domain = 64;
options.auto_decompress = true;  // gzip / deflate
options.timeout.total_ms = 5000;
options.h2.max_session_groups = 4096;
options.h2.session_group_idle_ttl = std::chrono::seconds(300);

options.request_hooks.push_back([](httpclient::Request& req) {
  req.set_header("X-Trace", "trace-id");
});
options.response_hooks.push_back([](httpclient::Response& resp) {
  // record metrics/logging here
});

httpclient::HttpClient client(options);

auto req = httpclient::RequestBuilder::get("users")
    .query_param("page", "1")
    .bearer_auth(token)
    .follow_redirects()
    .timeout({.total_ms = 2000, .connect_ms = 500, .read_ms = 1500})
    .build();

auto resp = client.request(std::move(req));
resp.raise_for_status();
```

支持的上层策略包括：

- `base_url`
- default headers / default query params
- Basic auth / Bearer auth
- redirect following，含 `max_redirects`
- retry / backoff / retry status
- cookie jar，支持 domain/path/max-age 的基础匹配
- request / response hooks
- opt-in gzip / deflate 自动解压
- total/connect/read/write/pool timeout 配置面；H1 已按 pool/connect/write/read 阶段应用，H2 已按 stream slot pool/write idle/read idle/total deadline 应用
- `Response::header()` / `headers_named()` / `text()` / `bytes()` / `content_type()` / `is_success()` / `is_redirect()` / `raise_for_status()`

## Proxy

代理默认是 client 级配置，request 级只作为覆盖或禁用：

```cpp
httpclient::HttpClient::Options options;
options.proxy = httpclient::ProxyConfig{"http://127.0.0.1:8899"};
options.no_proxy = {"localhost", "127.0.0.1", ".internal"};
options.trust_env_proxy = true;  // HTTP_PROXY / HTTPS_PROXY / ALL_PROXY

httpclient::HttpClient client(options);

auto proxied = client.get("http://example.com");

auto bypass = httpclient::RequestBuilder::get("http://example.com")
    .no_proxy()
    .build();

auto override = httpclient::RequestBuilder::get("http://example.com")
    .proxy("http://127.0.0.1:8888")
    .build();
```

当前支持：

- direct H1/H2。
- HTTP target 通过 HTTP proxy 的 absolute-form request。
- HTTPS target 通过 HTTP proxy 的 `CONNECT` 隧道，然后在隧道内做 TLS。
- H2 target 通过 HTTP proxy 的 `CONNECT` 隧道。
- HTTP target 通过 HTTPS proxy 的 TLS 代理连接和 absolute-form request。
- HTTPS/H2 target 通过 HTTPS proxy 的 TLS-in-TLS `CONNECT` 隧道。
- SOCKS5 proxy 支持 H1 HTTP、H1 HTTPS 和 H2。
- `Options::proxy` 作为默认代理，`RequestBuilder::proxy()` 覆盖默认代理，`RequestBuilder::no_proxy()` 禁用本次请求代理。
- `Options::no_proxy` 支持 host、后缀域名和 `*`。
- `Options::trust_env_proxy` 支持 `HTTP_PROXY` / `HTTPS_PROXY` / `ALL_PROXY` 及小写变量；同时识别 `NO_PROXY` / `no_proxy`。
- HTTP proxy URL 支持 `http://user:pass@host:port`，会生成 Basic `Proxy-Authorization`；用户名和密码支持百分号解码。
- H2 session pool 会按 origin + proxy transport key 隔离，避免 direct/proxy/不同认证复用同一连接。

request 级 `.proxy()` 是低频覆盖能力；`.no_proxy()` 会清掉 request 级 proxy 并禁用 client/env proxy，direct 请求仍可走 Auto/H2。

## 快速使用

最简单的用法是让 `HttpClient` 自己持有并启动后台 `io_context`。构造后事件循环会长期运行，
调用方可以在普通函数里直接同步调用：

```cpp
#include "httpclient/http_client.hpp"

#include <iostream>

int main() {
  httpclient::HttpClient client;

  auto resp = client.get("https://example.com");
  if (!resp.error.empty()) {
    std::cerr << resp.error << "\n";
    return 1;
  }

  std::cout << resp.status << "\n";
  std::cout << resp.body << "\n";
}
```

需要并发提交但不想写协程时，用 `request_async()` 或 `async_request_callback()`：

```cpp
httpclient::HttpClient client;

auto req = httpclient::RequestBuilder::post("https://example.com/api")
    .json(R"({"name":"alice"})")
    .build();

auto future = client.request_async(std::move(req));
auto resp = future.get();
```

如果你的程序已经有自己的 Asio loop，可以继续使用嵌入式构造，不会额外启动后台线程：

```cpp
boost::asio::io_context io;
httpclient::HttpClient client(io);
```

协程内使用仍然走底层 awaitable API：

```cpp
boost::asio::awaitable<void> run(httpclient::HttpClient& client) {
  auto resp = co_await client.async_get("https://example.com");
  co_return;
}
```

## asyncx 协程组合

`HttpClient` 的协程 API 返回 `boost::asio::awaitable<T>`。通用协程组合工具不放在
`httpclient` 命名空间，而是放在独立的 `asyncx` 命名空间里。它可以组合任何同一
Asio executor 上的 awaitable，不限于 HTTP 请求。

```cpp
#include "asyncx/asyncx.hpp"

boost::asio::awaitable<void> run(httpclient::HttpClient& client) {
  auto [a, b] = co_await asyncx::gather(
      client.async_get("https://example.com/a"),
      client.async_get("https://example.com/b"));

  auto resp = co_await asyncx::wait_for(
      client.async_get("https://example.com/slow"),
      std::chrono::milliseconds(500));

  auto [maybe_resp, timeout] = co_await asyncx::race(
      client.async_get("https://example.com/slow"),
      asyncx::sleep(500));
  if (timeout.has_value()) {
    // timeout branch won; race cancels the HTTP task
  }

  auto task = co_await asyncx::create_task(
      client.async_get("https://example.com/later"));
  auto [pending, fast] = co_await asyncx::one_of(
      task,
      asyncx::sleep(10));
  if (!pending.has_value()) {
    auto late = co_await task.await();
  }

  auto outcomes = co_await asyncx::gather(
      asyncx::return_exceptions,
      client.async_get("https://example.com/maybe-ok"),
      client.async_get("https://example.com/maybe-fail"));

  auto batch = co_await asyncx::gather_limited(
      urls.size(), 128, [&](std::size_t i) {
        return client.async_get(urls[i]);
      });

  asyncx::Semaphore sem(32);
  co_await sem.acquire();
  sem.release();

  asyncx::Queue<std::string> queue(1024);
  co_await queue.put("job");
  auto job = co_await queue.get();

  asyncx::ThreadPool cpu_pool(4);
  auto parsed = co_await asyncx::run_in_pool(cpu_pool, [] {
    return parse_or_compute_heavy_value();
  });

  struct ParseJob {
    std::string input;
    Parsed run() { return parse(input); }
  };
  auto parsed2 = co_await asyncx::run_job_in_pool<ParseJob>(
      cpu_pool, std::move(input));
}
```

语义对齐 Python `asyncio`：

- `gather(...)` 会并发启动传入的 cold awaitable，返回 `std::tuple<T...>`；默认等所有任务完成后传播第一个异常，不因为某个子任务失败而取消其它任务。
- `gather(asyncx::return_exceptions, ...)` 不抛出子任务异常，而是返回 `Outcome<T>`；调用者可检查 `has_value()` / `exception()`。
- `gather(std::vector<Task<T>>)` 和 `gather(std::vector<boost::asio::awaitable<T>>)` 已支持；vector 版本元素类型必须一致。
- `gather_limited(total, concurrency, fn)` 是带并发窗口的 async map，适合大量同类请求或压测；完成一个立即补一个。
- `one_of(...)` 返回第一个完成时的 `std::tuple<std::optional<T>...>` 快照，不取消其它任务。
- `race(...)` 返回第一个完成时的 `std::tuple<std::optional<T>...>` 快照，并取消其它任务。
- `wait_for(awaitable, timeout)` 超时后取消被等待任务，并抛 `asyncx::TimeoutError`。
- `wait(vector<Task<T>>, ReturnWhen)` 返回 done/pending 任务集合；`as_completed(vector<Task<T>>)` 返回完成顺序。
- `shield(awaitable/task)` 保护内层任务不被外层 `wait_for` / `race` 取消。
- `Queue<T>` / `Semaphore` / `Lock` / `Event` 提供 asyncio 风格的基础同步原语。
- `create_task(awaitable)` 会立即启动 awaitable，返回可复制的 hot `Task<T>` 句柄。
- `Task<T>` 提供 `id()` / `name()` / `set_name()` / `status()` / `exception()` / `executor()`；由于 Boost.Asio 的 `await_transform` 限制，等待任务仍使用 `co_await task.await()`，不能直接 `co_await task`。
- `TaskGroup` 提供基础结构化并发：统一创建任务、`join()` 等待、析构时取消未 join 的任务。
- `set_debug(true)` 和 `set_exception_handler(fn)` 可打开简单的调试异常回调。
- `one_of` / `gather` / `race` / `wait_for` 可以混合接收 `Task<T>` 和裸 awaitable；裸 awaitable 会被组合函数接管并启动，`Task<T>` 返回后仍可继续 `co_await task.await()`。
- `run_in_pool(thread_pool, fn)` 把 CPU/阻塞任务放到 `boost::asio::thread_pool`，完成后回到当前 coroutine；支持返回值、`void` 和异常传播。
- `asyncx::ThreadPool` 是 RAII 包装，析构时自动 `join()`；也可以通过 `.native()` 访问底层 `boost::asio::thread_pool`。
- `run_job_in_pool<Job>(thread_pool, args...)` 用编译期确定的 Job 类型包装重任务，动态参数进入 Job 构造，调用 `job.run()` 或 `job()`，不需要虚函数或 `std::function`。
- 取消会在 `asyncx` 边界表达为 `asyncx::CancelledError`；底层 Asio 的 `operation_aborted` 会被归一化到这个异常。

约束：裸 awaitable 会在当前 coroutine 的 executor 上启动；参与同一个组合器的裸
awaitable 应该运行在同一个 Asio executor。`Task<T>` 可以跨 executor 等待，必要时可用
`same_executor(a, b)` / `assert_same_executor(a, b)` 做显式防护。需要和业务协程组合时，
推荐用 `HttpClient(io_context&)`，让 HTTP 请求和业务协程共享同一个 `io_context`。

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

响应体和响应头默认会存入 `Response`。热路径或压测不需要保留响应内容时，可以显式关闭存储；
如果只需要流式消费 body，用 `stream_response()` 或 `on_body_chunk()`：

```cpp
auto discard = httpclient::RequestBuilder::get(url)
    .store_response(false, false)
    .build();

auto streamed = httpclient::RequestBuilder::get(url)
    .stream_response([](std::string_view chunk) {
      consume(chunk);
    }, false)
    .store_response(false, false)
    .build();
```

H1/H2 都遵守这两个开关：关闭 body/header 存储后，runtime 仍完整读取网络响应以便复用连接，
但不会把 body 追加到 `Response::body`，也不会构造 header 字符串列表。

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
PROFILE=auto \
scripts/run_mixed_protocol_bench.sh
```

输出包含 `wall_ms`、`p50_us`、`p95_us`、`p99_us`、进程 CPU、RSS/Go runtime 内存和连接复用统计。
`PROFILE=auto` 是默认值：低并发保持轻量，高并发自动扩大 H1 active shard
窗口和 H2 active session 数。可用 `PROFILE=throughput` 固定高吞吐资源，或用
`PROFILE=balanced` 做保守资源对照。
趋势表格可以直接运行：

```bash
REQUEST_CASES='1000 5000 10000' \
CONCURRENCY_CASES='128 256 512' \
scripts/run_resource_trend_bench.sh
```

从个位数到大规模请求的趋势测试可以用：

```bash
REQUEST_CASES='8 64 1000 10000 100000' \
CONCURRENCY_CASES='8 64 512' \
scripts/run_scale_bench.sh
```

百万级默认不跑，避免本地误触发长时间压测；需要时显式打开：

```bash
INCLUDE_1E6=1 \
RUN_GO_GATHER=0 \
scripts/run_scale_bench.sh
```

H1 pool 固定采用 FIFO-fair idle reservation：连接归还后优先交给已等待的请求，
不允许新请求插队抢走 idle connection。这是 runtime 稳定尾延迟语义，不提供配置开关。

完整外部验证脚本会构建 O2、运行 ctest、可选 ASAN、可选 mixed bench：

```bash
RUN_ASAN=1 RUN_MIXED_BENCH=1 scripts/verify_external_bench.sh
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
