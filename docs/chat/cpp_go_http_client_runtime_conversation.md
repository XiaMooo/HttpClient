# 高性能 C++/Go HTTP Client Runtime 对话导出

> 导出范围：当前对话中关于现代 C++ 高性能网络客户端、Go `net/http`、协程/纤程、跨平台 poller 和资源管理的讨论。

---

## Message 1

**role: user**

现代CPP的高性能网络库（只做请求不需要host http server)如果可以，最好能支持http2，websockete，多路复用，tls缓存等等比较现代的功能，最好也能支持异步，性能比肩httpx甚至是go

---

## Message 2

**role: assistant**

结论先说：**只做高性能 HTTP Client，我最推荐 `libcurl multi/socket API`，外面自己包一层 C++20 coroutine/future 接口**。它不“现代”，但能力最完整、生产稳定性最好，而且 HTTP/2/HTTP/3 多路复用、连接复用、TLS session cache、异步事件驱动都已经有。`libcurl` 的 multi interface 本身支持同线程多并发、事件驱动 socket 接口、可扩展到上千并发连接；HTTP/2/HTTP/3 multiplex 默认支持；TLS session-ID cache 默认开启；WebSocket 也有 `ws://`/`wss://` API。([curl.se](https://curl.se/libcurl/c/libcurl-multi.html?utm_source=chatgpt.com))

我会这样选：

| 需求 | 推荐 |
|---|---|
| 生产稳定、高性能 HTTP/1.1 + HTTP/2 请求 | **libcurl multi** |
| 想要更像现代 C++/协程 | **自己封装 libcurl multi**，或找薄封装，不建议重封太厚 |
| 想要更“C++ 框架式”的 async client | **libhv** |
| 只要 HTTP/1.1 + WebSocket，且你熟 Asio | **Boost.Beast** |
| 要极致 HTTP/2/HTTP/3 协议控制 | **Proxygen** 或 **nghttp2**，但工程成本高 |
| 已经在 Qt 生态 | **QNetworkAccessManager + Qt WebSockets** |

### 最推荐：libcurl multi/socket

它最符合你说的这些点：只发请求、不 host server、异步、高性能、HTTP/2 multiplex、TLS 连接复用/会话复用、代理、DNS cache、连接池、超时控制、证书校验、HTTP/3 也能做。`libcurl` 的连接复用机制会保留连接池，后续同 host 请求可复用连接，避免重复 TCP/TLS 握手；HTTP/2 和 HTTP/3 下还可以在同一连接上做多个并发 transfer。([everything.curl.dev](https://everything.curl.dev/transfers/conn/reuse.html?utm_source=chatgpt.com))

它的劣势是：API 是 C 风格，不像 `httpx.AsyncClient()` 或 Go `http.Client` 那么舒服。所以正确用法不是直接裸写一堆 `curl_easy_setopt`，而是封装成：

```cpp
// pseudo-code
auto client = HttpClient({
    .max_connections = 1024,
    .max_host_connections = 16,
    .enable_http2 = true,
    .enable_tls_session_cache = true,
});

auto r1 = co_await client.get("https://api.xxx.com/a");
auto r2 = co_await client.post_json("https://api.xxx.com/b", body);
```

底层用 `curl_multi_socket_action()` 接入你自己的 `epoll` / `asio` / `libuv` 事件循环。`libcurl` 官方也把 multi/socket 作为事件驱动模型描述，应用层监听 socket 事件后调用 `curl_multi_socket_action()` 推进传输。([curl.se](https://curl.se/libcurl/c/curl_multi_socket_action.html?utm_source=chatgpt.com))

### 但是：WebSocket 要单独注意

`libcurl` 现在支持 WebSocket，能用 `ws://`/`wss://`，并提供 `curl_ws_send()` / `curl_ws_recv()`。但它的 WebSocket 是典型 HTTP Upgrade 模型，也就是从普通 HTTP(S) GET upgrade 到 WebSocket；如果你想要“多个 WebSocket 会话跑在同一 HTTP/2 连接的不同 stream 上”，这类支持在 C++ 客户端生态里并不常见，不能简单认为“支持 HTTP/2 + 支持 WebSocket = WebSocket over HTTP/2 多路复用”。([curl.se](https://curl.se/docs/websocket.html?utm_source=chatgpt.com))

如果你的 WebSocket 只是连接交易所、推送服务、实时消息服务，`wss://` 独立连接通常够用。真正想要一个 TCP/TLS 连接里承载大量双向流，更建议考虑 **gRPC streaming / HTTP/2 stream / WebTransport**，而不是传统 WebSocket。

### 第二选择：libhv

`libhv` 更像“现代一点、易用一点的 C/C++ 网络库”，自带 EventLoop、HTTP client/server、WebSocket client/server、SSL/TLS，并声称支持 HTTPS、HTTP/1.x、HTTP/2、gRPC；它的 HTTP client 有同步、异步、连接池以及类似 Python requests / JS axios 的高层接口。([github.com](https://github.com/ithewei/libhv?utm_source=chatgpt.com))

它适合你想快速落地、API 不想太底层的场景。缺点是生态和长期稳定性不如 libcurl，复杂协议细节、边界行为、代理/证书/HTTP2 参数可控性也不一定比 libcurl 强。

### 不太推荐作为主 HTTP/2 Client：Boost.Beast

`Boost.Beast` 很适合 HTTP/1.1 和 WebSocket，基于 Boost.Asio，异步模型清晰。但官方定位是 HTTP/1 和 WebSocket，不是 HTTP/2 客户端库。([boost.org](https://www.boost.org/library/latest/beast/?utm_source=chatgpt.com))

所以如果你明确要求 HTTP/2、多路复用，它不适合作为主方案。除非你的需求退化为：HTTP/1.1 REST + WSS，且你本来就重度使用 Asio。

### 高阶方案：Proxygen / nghttp2

`Proxygen` 是 Meta/Facebook 的 C++ HTTP 库，目标是高性能现代 HTTP，支持 HTTP/1.1、HTTP/2、HTTP/3，内部也用于 server、proxy、client 场景。([ports.macports.org](https://ports.macports.org/port/proxygen/details/?utm_source=chatgpt.com))

`nghttp2` 是底层 HTTP/2 实现，提供 HTTP/2 framing、HPACK、client/server/proxy 工具；但它偏底层，C++ 高层 Asio 包曾因维护问题从主仓库迁出，直接用它做业务 client 工程成本较高。([github.com](https://github.com/nghttp2/nghttp2?utm_source=chatgpt.com))

这两个适合“我要自己控制 HTTP/2 stream、priority、flow control、hpack、长连接调度”的场景，不适合单纯替代 httpx。

### 我的建议

你的需求我会分成两档：

**工程落地优先：**

```text
libcurl multi + C++20 coroutine wrapper
```

这是最稳的。性能上完全有机会超过 Python httpx，接近甚至超过 Go 的请求客户端，前提是你别搞成 thread-per-request，而是统一 event loop + 连接池 + HTTP/2 multiplex。

**想要开发舒服一点：**

```text
libhv
```

更像开箱即用的现代 C++ 网络库，但我会先压测你真实场景：同 host 并发、跨 host 并发、TLS 握手复用、HTTP/2 多路复用、WebSocket 长连接、超时取消、DNS 行为、代理行为。

最终一句话：**追求“httpx/go 级别能力 + C++ 性能”，首选 libcurl multi；追求“现代 C++ API 易用”，选 libhv；不要用 Beast 去硬做 HTTP/2。**

---

## Message 3

**role: user**

想要超越呢？

---

## Message 4

**role: assistant**

想要**明显超越 httpx，甚至在特定场景超过 Go `net/http`**，就不要把目标放在“找一个现成 C++ requests 库”上，而是要做成**专用高性能 HTTP Client Runtime**。

我的建议分三档：

## 1. 最现实的“超越 httpx”方案：libcurl multi + 自己封装调度层

这条路最稳。`libcurl multi` 本身支持同线程多并发、事件驱动、可扩展到上千并发连接；它还有 HTTP/2 / HTTP/3 concurrent streams 控制，默认最大并发 stream 是 100；TLS session-ID cache 默认启用。也就是说，底层能力已经够了，瓶颈通常在你怎么调度、怎么复用对象、怎么处理回调和内存。([curl.se](https://curl.se/libcurl/c/libcurl-multi.html))

核心结构应该是：

```text
Application API
    ↓
C++20 coroutine / Future 封装
    ↓
Request Scheduler
    ↓
Origin Pool: scheme + host + port + proxy + cert config
    ↓
HTTP/2 Stream Pool / HTTP/1.1 Connection Pool
    ↓
libcurl multi_socket + epoll/io_uring/asio
```

你要超越 httpx，重点不是“C++ 比 Python 快”，而是：

```text
少建连接
少握手
少分配内存
少跨线程
少系统调用
少复制 body
少动态 header 构造
```

这套做好后，**超越 httpx 是比较现实的**。httpx 的强项是工程体验，不是极限吞吐。

## 2. 想挑战 Go：nghttp2 + 自研连接池/调度器

如果目标是“在固定 API 服务、固定域名、高并发小请求场景里超过 Go”，那可以考虑：

```text
nghttp2 + OpenSSL/BoringSSL + epoll/io_uring + C++20 coroutine + 自研 pool
```

`nghttp2` 是专门的 HTTP/2 C 实现，提供 client tutorial、HPACK API、stream priority、flow-control、session API 等低层能力，适合你自己控制 HTTP/2 stream、窗口、队列和调度策略。([nghttp2.org](https://nghttp2.org/documentation/))

这一档比 libcurl 难很多，但更容易做出“专用场景超越 Go”的结果。原因是 Go `net/http` 是通用客户端，它要照顾大量场景；你可以针对自己的业务做偏置优化，例如：

```text
固定几个 host
固定 header 模板
固定 JSON body 大小
固定超时策略
固定证书配置
固定 HTTP/2 stream 并发
固定重试/熔断策略
```

只要场景足够固定，你可以比通用库少很多分支和分配。

推荐结构：

```text
每个 CPU core 一个 EventLoop
每个 EventLoop 一个 ClientShard
每个 Origin 一个 H2SessionPool
每个 H2Session 维护 N 个 active streams
请求按 origin hash 到固定 shard
响应回调尽量不跨线程
```

不要做成“全局连接池 + 多线程抢锁”。那样很容易被 Go 打爆。

## 3. 极限路线：Proxygen / Fizz / mvfst 生态

如果你想要非常现代的 HTTP/2 / HTTP/3 能力，可以看 Proxygen。它是 Facebook/Meta 的 C++ HTTP 库，README 里写明其核心 HTTP 抽象在 Facebook 内部用于构建 server、proxy 和 client，并支持 HTTP/1.1、HTTP/2、HTTP/3，目标是 performant and modern C++ HTTP library。([github.com](https://github.com/facebook/proxygen))

但我不建议你一上来用它，原因是工程复杂度高，依赖重，构建和调试成本都高。它更像基础设施团队用的库，不像普通业务侧 HTTP client。

## 真正想“超越”的关键不是库，而是策略

你应该把优化目标拆成这几个：

### 小请求高 QPS

重点是：

```text
HTTP/2 multiplex
TLS session resumption
连接预热
header 模板缓存
request/response 对象池
body buffer 复用
单 event loop 内完成回调
```

这类场景下，C++ 专用客户端很有机会超过 Go。

### 大响应下载

重点是：

```text
zero-copy / 少 copy
streaming parse
不要把 response 全量攒进 string
直接写文件、pipe、ring buffer 或 parser
```

这种场景下，语言差距没那么大，瓶颈更多是网络和内存复制。

### WebSocket 长连接

不要和普通 HTTP 请求混在同一个调度器里。建议分成：

```text
HTTP Request Client
WebSocket Session Manager
```

WebSocket 更关注长连接保活、断线重连、ping/pong、消息队列背压，而不是短请求 QPS。

### 多域名请求

多域名场景不一定 HTTP/2 multiplex 有优势，因为 multiplex 主要发生在同一个 origin 上。你需要：

```text
DNS cache
Happy Eyeballs
连接预热
每 origin 独立限流
失败隔离
```

## 我会怎么选

如果你现在是要做一个自己的高性能客户端库，我建议路线是：

```text
第一阶段：libcurl multi_socket + C++20 coroutine 封装
第二阶段：做 benchmark，确认瓶颈
第三阶段：只有当 libcurl 成为瓶颈时，才切 nghttp2 自研 H2 client
```

不要一开始就自研 HTTP/2。HTTP/2 的坑很多：flow control、stream reset、GOAWAY、SETTINGS、HPACK、连接迁移策略、服务端最大 stream 限制、代理、证书、超时取消、半关闭状态，全都要处理。

一个比较合理的目标是：

```text
比 httpx 快：libcurl multi 就够
接近 Go：libcurl multi + 很好的调度层
特定场景超过 Go：nghttp2 + 自研事件循环/连接池
通用场景超过 Go：很难，不建议作为目标
```

如果只允许我给一个最终技术选型：

```text
生产优先：libcurl multi_socket + C++20 coroutine wrapper
性能极限：nghttp2 + BoringSSL/OpenSSL + epoll/io_uring + per-core sharding
不建议首选：Boost.Beast 做 HTTP/2、普通 cpp-httplib、纯 header-only 小库
```

一句话：**想“超越”，不要找 requests 风格的 C++ 库，而是做一个 per-core sharding、连接预热、HTTP/2 stream 池化、对象池化、零拷贝流式处理的专用客户端。libcurl 是最稳起点，nghttp2 是冲极限的底座。**

---

## Message 5

**role: user**

go能编译.so，并暴露给Cpp，实现高性能的调度么

---

## Message 6

**role: assistant**

能，但**不建议把 Go 当成“给 C++ 调用的细粒度高性能调度器”**。更准确的结论是：

```text
可以：C++ 调 Go .so，让 Go 内部维护 HTTP client、连接池、goroutine 调度、HTTP/2 多路复用。
不适合：每个小操作都 C++ → Go → C++ callback 高频来回跨边界。
```

Go 官方支持 `-buildmode=c-shared`，可以把一个 `main` package 编译成 C shared library，只有用 cgo `//export` 导出的函数能被 C/C++ 调用。([pkg.go.dev](https://pkg.go.dev/cmd/go/?utm_source=chatgpt.com))

---

## 推荐架构

比较合理的是让 **Go 完整拥有网络层**：

```text
C++ 主程序
  |
  | C ABI: client_submit / client_cancel / client_poll
  v
Go .so
  |
  | goroutine scheduler
  | net/http / x/net/http2 / websocket
  | TLS session reuse
  | connection pool
  v
远端服务
```

也就是说，C++ 不要参与每个 socket 的调度。C++ 只提交请求，Go 负责：

```text
连接池
HTTP/2 stream multiplex
TLS 复用
DNS cache
超时
重试
限流
WebSocket 长连接管理
```

然后结果再回到 C++。

这样可以利用 Go 的优势：goroutine 调度、`net/http` 成熟连接池、HTTP/2 支持、生态好。

---

## 不推荐的架构

不要这样：

```text
C++ 发一个请求
  -> 调 Go
     -> Go 启 goroutine
        -> 完成后立刻 callback C++
           -> C++ 再调用 Go
              -> Go 再 callback C++
```

这种边界来回跳会吃掉不少性能优势。尤其是小请求、高 QPS、低延迟场景，FFI/cgo 边界、内存转换、callback、线程调度都会变成噪声。

---

## C++ / Go 边界设计要点

### 1. 用 C ABI，不要暴露 C++ 对象

Go 导出的 `.so` 最好只暴露 C 风格 API：

```cpp
extern "C" {
    int client_init(const ClientConfig* cfg);
    uint64_t client_submit(const Request* req);
    int client_cancel(uint64_t request_id);
    int client_poll(Response* out, int max_count);
    void client_shutdown();
}
```

不要让 Go 直接理解 C++ class、`std::string`、`std::function`、模板对象。

---

### 2. 结果返回最好用 poll，而不是直接 callback

更推荐：

```text
C++ submit 请求
Go 内部完成请求
Go 把结果写入 completion queue
C++ 主循环 poll 结果
```

而不是 Go 完成后直接调用 C++ callback。

原因是 callback 会引入跨语言调用、线程归属、锁、生命周期管理等复杂问题。`poll` 模型更像高性能网络库，也更容易接入 C++ 自己的事件循环。

---

### 3. 内存最好由 C++ 分配，Go 只读/写

Go 和 C 之间传指针有严格限制。Go 官方 cgo 文档明确说明，Go 是 GC 语言，C 代码保存 Go 指针有约束；C 只能在 Go 内存被 pin 住时持有 Go 指针，而且不能随便长期保存。([pkg.go.dev](https://pkg.go.dev/cmd/cgo?utm_source=chatgpt.com))

所以边界上建议：

```text
请求 body/header：C++ 分配，传 pointer + length 给 Go
响应结果：Go 写入 C++ 提供的 buffer，或者 Go 返回 handle，C++ 再 copy 出来
长期对象：用 uint64_t handle，不传 Go 指针
```

不要把 Go slice、Go string、Go map、Go function pointer 直接交给 C++ 长期保存。

---

## 一个更稳的 API 形态

### C++ 侧

```cpp
struct Request {
    const char* method;
    const char* url;
    const char* body;
    size_t body_len;
    uint64_t user_data;
};

struct Response {
    uint64_t request_id;
    uint64_t user_data;
    int status;
    const char* body;
    size_t body_len;
    int error_code;
};

uint64_t client_submit(const Request* req);
int client_poll(Response* out, int max_count);
void response_free(Response* resp);
```

### Go 侧负责

```text
client_submit:
  - 复制必要字段
  - 分配 request_id
  - 投递到 Go 内部 channel
  - 立即返回

worker goroutine:
  - 使用共享 http.Client
  - 复用连接池
  - HTTP/2 multiplex
  - 完成后放入 completion queue

client_poll:
  - C++ 主动拉取完成结果
```

这个模型比 callback 更容易做高性能。

---

## 性能判断

如果你的目标是：

```text
让 C++ 获得 Go net/http 的工程能力
```

可以。

如果目标是：

```text
比纯 Go 更快
```

不一定。因为 Go runtime + cgo 边界不会免费。

如果目标是：

```text
比 C++ 自研 nghttp2/libcurl multi 更快
```

一般不现实。

更准确地说：

| 方案 | 性能潜力 | 工程复杂度 | 推荐程度 |
|---|---:|---:|---:|
| C++ 调 Go .so，Go 管网络 | 中高 | 中 | 可以做 |
| C++ + libcurl multi | 高 | 中 | 更稳 |
| C++ + nghttp2 自研调度 | 很高 | 很高 | 冲极限 |
| Go 单独做 sidecar 服务 | 中高 | 低中 | 很实用 |
| C++/Go 高频 callback 混合调度 | 不稳定 | 高 | 不推荐 |

---

## 我会怎么建议你选

如果你的 C++ 主程序只是业务核心，网络请求层希望快速获得 Go 那套成熟能力，那么：

```text
Go .so 可以做。
```

但要让 Go 成为一个完整的网络 runtime，不要让它只当“函数库”。

如果你是为了**极限性能**，尤其是小请求、高并发、低 p99，那么我还是更建议：

```text
C++ 主体 + libcurl multi_socket
或者
C++ 主体 + nghttp2 + 自研 per-core event loop
```

一句话：**Go 编译成 `.so` 暴露给 C++ 是可行的，但它适合“把 Go 网络客户端作为一个黑盒 runtime 嵌入 C++”，不适合用来做跨语言细粒度调度。想要极限性能，C++ 内部直接做 libcurl/nghttp2 调度更合适。**

---

## Message 7

**role: user**

Go本身是如何实现那么快那么高性能的net http请求的，这个import应该是开源的吧，是语言本身的架构(天生多线程异步高并发)还是这个模块的程序设计优良

---

## Message 8

**role: assistant**

是两者叠加，但核心答案是：

**Go 的 `net/http` 快，不是因为 `net/http` 这个包单独多神，而是因为它站在 Go runtime 的 goroutine scheduler + netpoll 网络轮询器之上，同时 `Transport` 又做了比较成熟的连接池、复用、HTTP/2、超时和并发安全设计。**

而且它确实是开源的，`net/http/transport.go` 就在 Go 标准库源码里，BSD-style license；源码里也明确写了这是 HTTP client 的底层 `Transport` 实现。([go.dev](https://go.dev/src/net/http/transport.go?s=20222%3A20264))

## 1. Go 的 API 看起来是阻塞，底层其实是异步调度

Go 代码通常这样写：

```go
resp, err := client.Do(req)
```

看起来像同步阻塞。但在网络 I/O 上，它不是“一个请求卡死一个 OS 线程”的传统模型。

Go 官方文档说明，goroutine 会被复用到多个 OS 线程上；如果一个 goroutine 因 I/O 等待而阻塞，其他 goroutine 仍然可以继续运行。([go.dev](https://go.dev/doc/effective_go?lang=en&version=1))

也就是说，Go 给程序员的是：

```text
同步代码写法
+
异步 runtime 调度
```

这点非常关键。它避免了 C++ 里常见的 callback hell / future 链 / 手写 epoll 状态机，同时又不需要每个请求一个真实线程。

## 2. runtime 层有 integrated network poller

Go runtime 有集成的网络轮询器。源码注释明确写了 `Integrated network poller`，并且不同平台会有 epoll、kqueue、Windows 等实现。([go.dev](https://go.dev/src/runtime/netpoll.go))

在 Linux 下，Go runtime 的 netpoll 使用 epoll，源码里可以看到 `EpollCreate1`、`Eventfd`、`EpollCtl`，并且注册的是 `EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET` 这一类事件。([go.dev](https://go.dev/src/runtime/netpoll_epoll.go))

所以大致模型是：

```text
goroutine 发起网络读写
        ↓
socket 暂时不可读/不可写
        ↓
goroutine 被 park
        ↓
OS thread 去跑别的 goroutine
        ↓
epoll/kqueue 通知 fd ready
        ↓
runtime 唤醒对应 goroutine
```

这就是为什么 Go 能用“看起来阻塞”的代码写出高并发网络程序。

## 3. goroutine scheduler 本身就是为这种场景设计的

Go runtime 源码里把 scheduler 的任务描述为：把 ready-to-run goroutines 分发到 worker threads 上；核心概念是 G、M、P：

```text
G = goroutine
M = worker thread / machine
P = processor，执行 Go 代码所需的调度资源
```

源码注释还提到，调度器状态是分布式的，尤其是 per-P work queues，避免把所有调度状态集中到一个全局中心，否则会影响可扩展性。([go.dev](https://go.dev/src/runtime/proc.go?h=findrunnable))

所以 Go 快的第一层原因是：**它把“海量并发任务”抽象成 goroutine，而不是 OS thread。**

C++ 当然也可以做到，但你需要自己选：

```text
asio
libuv
epoll
io_uring
协程调度器
任务队列
线程池
取消机制
超时机制
```

Go 把这些内建进了 runtime 和标准库的使用习惯里。

## 4. `net/http` 的 `Transport` 设计也很关键

Go HTTP client 真正核心不是 `http.Client`，而是 `http.Transport`。

官方文档明确说，`Client` 和 `Transport` 可以被多个 goroutine 并发使用，并且为了效率应该只创建一次、重复使用。([pkg.go.dev](https://pkg.go.dev/net/http))

这句话非常重要。Go 高性能 HTTP 的基本用法不是：

```go
for {
    client := &http.Client{}
}
```

而是：

```go
var client = &http.Client{
    Transport: transport,
}
```

长期复用一个 `Transport`。

`DefaultTransport` 会按需建立网络连接，并缓存连接供后续请求复用；默认还带有 `ForceAttemptHTTP2: true`、`MaxIdleConns: 100`、`IdleConnTimeout: 90s`、`TLSHandshakeTimeout: 10s` 等配置。([go.dev](https://go.dev/src/net/http/transport.go?s=20222%3A20264))

这意味着它快的第二层原因是：

```text
连接复用
TLS 连接复用
HTTP keep-alive
HTTP/2 自动尝试
空闲连接池
并发安全
超时控制
代理支持
```

## 5. HTTP/2 是标准库集成的，不是用户手写的

Go 的 `net/http` 对 HTTPS 会根据服务端支持和配置选择 HTTP/1.1 或 HTTP/2；官方文档说明 `DefaultTransport` 支持 HTTP/2。([pkg.go.dev](https://pkg.go.dev/net/http))

Go 标准库里还直接 bundle 了 `golang.org/x/net/http2` 的实现，源码注释说大多数用户会通过 `net/http` 自动间接使用 HTTP/2。([go.dev](https://go.dev/src/net/http/h2_bundle.go?utm_source=chatgpt.com))

所以对用户来说：

```go
client.Do(req)
```

底层可能已经是：

```text
TLS + ALPN
        ↓
协商 h2
        ↓
HTTP/2 ClientConn
        ↓
一个连接多个 stream
        ↓
多请求复用同一个 TCP/TLS 连接
```

这就是 Go 在访问支持 HTTP/2 的 API 服务时经常表现很好的原因。

## 6. 它不是“天生多线程异步高并发”这么简单

更准确应该说：

```text
Go 语言/runtime 给了高并发网络 I/O 一个非常低摩擦的执行模型；
net/http 这个模块在这个模型上做了成熟的工程实现。
```

二者缺一不可。

如果只有 goroutine，没有优秀的 `Transport`，那 HTTP client 仍然会慢，比如频繁建连接、频繁 TLS 握手、没有连接池。

如果只有优秀 HTTP 库，但语言层每个请求都是真线程，或者需要你手动管理状态机，开发复杂度和调度成本也会很高。

## 7. Go 为什么经常比 Python httpx 快？

主要不是 HTTP 协议算法本身差距，而是这几个点：

```text
Go 是编译型语言
goroutine 轻量
runtime netpoll 统一管理 I/O 等待
标准库连接池成熟
Transport 并发安全
HTTP/2 自动集成
减少 Python 解释器、asyncio、对象分配等开销
```

`httpx` 的优势是开发体验、生态和灵活性，不是极限吞吐。Go 的 `net/http` 是标准库级别长期打磨的生产组件。

## 8. Go 为什么有时候不如专门 C++ 实现？

因为 Go `net/http` 是通用库，不是极限场景专用库。它需要兼顾：

```text
HTTP/1.1
HTTP/2
代理
重定向
Cookie
压缩
TLS
Context cancellation
错误语义
兼容性
标准库稳定性
```

如果你用 C++ 专门针对一个固定场景写：

```text
固定 host
固定 header
固定 body 格式
固定 HTTP/2
固定连接数
固定超时策略
固定内存池
固定 parser
```

理论上 C++ 可以更快。

但如果 C++ 写成：

```text
每个请求一个线程
每个请求新建连接
没有连接池
没有 HTTP/2 multiplex
频繁 malloc/free
response 全量 copy 多次
```

那很容易被 Go `net/http` 打爆。

## 9. 对你前面“Go 编 .so 给 C++ 调”的启发

Go 的优势不是某一个 `import net/http` 函数，而是这一整套 runtime + Transport 的配合。

所以如果你想把 Go 嵌入 C++，正确姿势是：

```text
C++ 只提交请求
Go 内部长期持有 http.Client / Transport
Go 内部管理 goroutine、连接池、HTTP/2、超时、复用
C++ 通过 poll 或批量取结果
```

错误姿势是：

```text
C++ 每次调用 Go
Go 每次新建 client
请求完成再 callback C++
频繁跨语言边界
```

那样会把 Go 的优势吃掉。

## 10. 总结一句

**Go 的高性能 HTTP 不是单纯靠 `net/http` 这个模块，也不是单纯靠语言“天生异步”。它是 runtime scheduler、integrated netpoll、轻量 goroutine、标准库 `Transport` 连接池/复用/HTTP2 设计共同作用的结果。**

如果你要在 C++ 里复刻 Go 的效果，重点不是“找个 HTTP 库”，而是复刻这套结构：

```text
轻量任务模型
+
事件驱动 I/O
+
连接池
+
HTTP/2 stream 池
+
TLS/连接复用
+
统一超时/取消/背压
```

所以我前面才说：**C++ 想超越 Go，不是裸调一个库，而是要做一个专用 HTTP Client Runtime。**

---

## Message 9

**role: user**

也就是，超越可以，本质上是复刻go本身的goroutine调度，并学习net http的工程逻辑，实现go最基底的运行时调度机制+模仿net/http的行为

---

## Message 10

**role: assistant**

对，但要稍微修正一下表述：

**不是完整复刻 Go runtime，而是复刻 Go 在网络请求场景里的几个关键效果。**

也就是：

```text
goroutine 调度的效果
+
netpoll 异步 I/O 的效果
+
net/http Transport 的连接池与协议复用行为
```

而不是把 Go 的 runtime 全部重写一遍。

## Go 快的本质可以拆成两块

第一块是运行时调度：

```text
大量轻量任务
阻塞式写法
底层异步 I/O
少量 OS thread 承载大量并发
I/O ready 后唤醒任务
```

C++ 里对应的是：

```text
C++20 coroutine / fiber
+
epoll / io_uring / kqueue
+
per-core event loop
+
work stealing / task queue
+
timer wheel / timeout manager
```

第二块是 `net/http.Transport` 的工程逻辑：

```text
连接池
每 origin 连接复用
HTTP/2 stream 多路复用
TLS session resumption
DNS cache
超时与取消
请求排队
最大连接数限制
最大空闲连接数
失败连接剔除
GOAWAY / RST_STREAM / retry 处理
```

所以你说的方向基本对：

```text
不是简单找一个 HTTP 库
而是做一个 HTTP Client Runtime
```

## 但不要真的“照抄 goroutine”

Go 的 goroutine 强在通用性：

```text
自动栈增长
M:N 调度
抢占
GC 协作
channel/select
syscall 阻塞处理
netpoll 集成
```

C++ 要完全复刻这个，工程量非常大，而且未必值得。

你真正需要的是面向 HTTP client 的轻量调度器：

```text
RequestTask
  ↓
EventLoop
  ↓
Socket readiness
  ↓
Protocol state machine
  ↓
Completion queue
```

也就是说，C++ 里更合理的是：

```text
用 coroutine 表达异步逻辑
用 event loop 处理 socket
用 per-core sharding 减少锁
用连接池/stream 池降低建连成本
用对象池降低 malloc/free
```

不是实现一个完整 Go runtime。

## 一个高性能 C++ HTTP Client Runtime 应该长这样

```text
Application
   ↓
HttpClient API
   ↓
Request Scheduler
   ↓
Shard Router
   ↓
Per-Core EventLoop
   ↓
Origin Pool
   ↓
HTTP/2 Session Pool / HTTP/1.1 Connection Pool
   ↓
TLS / TCP / DNS
```

更细一点：

```text
HttpClient
 ├── DNSCache
 ├── TLSContextPool
 ├── OriginPool
 │    ├── example.com:443
 │    │     ├── H2Session #1
 │    │     ├── H2Session #2
 │    │     └── idle H1 connections
 │    └── api.xxx.com:443
 ├── TimerManager
 ├── RetryPolicy
 ├── BackpressureController
 └── CompletionQueue
```

每个请求进来后，不应该立刻新建连接，而是：

```text
hash(origin) → 进入固定 shard
查看是否已有 HTTP/2 session
如果有空闲 stream slot → 直接发 stream
如果没有 → 排队或新建连接
响应完成 → 释放 stream slot
连接保持 idle
```

这就是模仿 Go `Transport` 的核心行为。

## 真正能超过 Go 的地方

Go `net/http` 是通用实现，它要照顾很多情况。你如果场景固定，可以做得更激进。

例如你的业务如果是：

```text
固定几个 API host
固定 HTTP/2
固定 header 模板
固定 JSON 格式
固定超时策略
不需要 cookie
不需要 redirect
不需要复杂 proxy
```

那 C++ 可以砍掉大量通用逻辑。

可以优化成：

```text
Header 预编码
HPACK 动态表策略固定
body buffer 复用
response streaming parse
固定 origin 的连接预热
每 core 独立连接池
请求对象池化
减少跨线程调度
减少锁
减少拷贝
```

这时超过 Go 是有机会的。

## 但如果做成这样，就很难超过 Go

```text
C++ 每个请求一个 std::thread
每个请求新建 TCP/TLS
没有 HTTP/2 multiplex
全局连接池大锁
response 全部复制到 std::string
频繁 malloc/free
callback 跨线程乱跳
```

这种 C++ 很容易比 Go 慢。

## 所以你的判断可以改成这句话

更准确地说：

```text
想超越 Go 的 HTTP client，不是要完整复刻 Go runtime，
而是要在 C++ 里实现等价的网络调度模型：
轻量任务 + 事件驱动 I/O + per-core 调度 + 连接池 + HTTP/2 stream 池 + TLS/DNS 复用。
```

然后再学习 `net/http.Transport` 的工程行为：

```text
长生命周期 client
连接复用
origin 级别限流
HTTP/2 自动多路复用
空闲连接管理
超时/取消/重试
故障连接清理
背压控制
```

## 最推荐的实现路线

我不建议一上来完全自研。

比较稳的路线是：

```text
阶段 1：libcurl multi_socket + 自己的调度层
阶段 2：封装成 C++20 coroutine API
阶段 3：实现 per-core sharding、连接预热、对象池
阶段 4：压测确认瓶颈
阶段 5：只有 libcurl 成为瓶颈时，再切 nghttp2 自研 HTTP/2 层
```

最终目标不是“写一个 C++ requests”，而是：

```text
写一个专用 HTTP Client Runtime
```

一句话总结：**你的理解是对的，但重点不是复刻 Go 的完整运行时，而是复刻它在网络 I/O 上的效果，再模仿 `net/http.Transport` 的连接复用、HTTP/2 多路复用和工程调度逻辑。这样在固定场景下才有机会真正超过 Go。**

---

## Message 11

**role: user**

是否需要纤程？还是自己写协程调度就足够优秀？

---

## Message 12

**role: assistant**

**大多数情况下不需要纤程。**

如果你的目标是高性能 HTTP Client Runtime，**C++20 stackless coroutine + 自己的事件循环/调度器就足够优秀**，甚至更适合极限性能。

可以这样理解：

```text
Go goroutine ≈ 语言/runtime 级 stackful 轻量线程
C++20 coroutine ≈ 编译器支持的 stackless 状态机
Fiber ≈ 用户态 stackful 线程
```

## 结论

你的场景优先级应该是：

```text
首选：C++20 coroutine + epoll/io_uring + per-core event loop
次选：Boost.Asio coroutine / libcurl multi_socket 封装
特殊情况才用：fiber
```

也就是说，不是“越像 goroutine 越好”。Go 的 goroutine 很强，是因为 Go runtime 帮你解决了栈增长、抢占、netpoll、调度、syscall 处理、GC 协作等一整套问题。C++ 里强行用 fiber 复刻 goroutine，通常会把复杂度带回来。

## 为什么协程就够了？

HTTP Client 本质是大量 I/O 等待：

```text
DNS 等待
TCP connect 等待
TLS handshake 等待
socket 可读/可写等待
HTTP/2 stream window 等待
response body 等待
timeout 等待
```

这些等待点天然适合 `co_await`。

例如：

```cpp
Response r = co_await client.get("https://api.example.com/data");
```

底层可以变成：

```text
发起请求
socket 暂时不可写 → suspend coroutine
epoll/io_uring 通知 ready
resume coroutine
继续协议状态机
读取响应
完成任务
```

这里并不需要 fiber 的“独立栈”。C++20 coroutine 会把局部状态保存到 coroutine frame 里，本质上就是一个高效状态机。

## Fiber 的优势是什么？

Fiber 是 stackful 的，所以它可以让普通阻塞式代码看起来不用改：

```cpp
auto r = client.get(url);   // 看起来阻塞
```

但底层其实切走 fiber，让同一个线程跑别的 fiber。

它的优势主要是：

```text
更接近 Go goroutine 写法
可以保留深层调用栈
改造旧同步代码更容易
不需要所有函数都变成 async/co_await
```

比如你有大量旧代码：

```cpp
A() -> B() -> C() -> http_get()
```

如果不用 fiber，可能要一路改成：

```cpp
co_await A()
co_await B()
co_await C()
co_await http_get()
```

这时候 fiber 有价值。

## 但 Fiber 的代价也明显

Fiber 会有自己的栈，哪怕是小栈，也会带来额外成本：

```text
每个 fiber 需要栈内存
要处理栈大小问题
context switch 比 stackless coroutine 重
调试更复杂
和 sanitizer / profiler / exception 交互更麻烦
和 C++ TLS / thread_local 语义可能有坑
抢占调度不好做
跨线程迁移也麻烦
```

尤其你要做的是高性能 HTTP Client，不是通用语言 runtime。那 fiber 很可能是“为了像 Go 而像 Go”，不一定带来性能收益。

## C++20 coroutine 更适合你的目标

对于 HTTP/2 多路复用客户端，理想模型是：

```text
每个 CPU core 一个 EventLoop
每个 EventLoop 管自己的 socket
每个 origin 有自己的连接池 / H2 session 池
每个 request 是一个 coroutine task
I/O 不 ready 就 suspend
ready 后原地 resume
结果写入 completion queue
```

结构大概是：

```text
HttpClient
 ├── Shard 0 / EventLoop 0
 │    ├── epoll/io_uring
 │    ├── TimerQueue
 │    ├── OriginPool
 │    └── Coroutine Scheduler
 ├── Shard 1 / EventLoop 1
 ├── Shard 2 / EventLoop 2
 └── Shard N / EventLoop N
```

请求进来：

```text
hash(origin) → 固定 shard
             → 找 H2Session
             → 分配 stream
             → coroutine suspend/resume
             → response complete
```

这个模型不需要 fiber。

## “自己写协程调度”要写到什么程度？

不需要写成 Go runtime 那么复杂。你只需要写一个**面向网络 I/O 的协作式调度器**。

核心组件：

```text
Task<T>
Awaiter
EventLoop
TimerManager
Socket readiness registry
Completion queue
Cancellation token
Backpressure queue
```

协程调度逻辑可以很简单：

```text
coroutine 运行
遇到 I/O await
注册 fd + event
suspend
事件 ready
把 coroutine handle 放入 ready queue
event loop resume
```

伪结构：

```cpp
struct EventLoop {
    void run();

    void schedule(std::coroutine_handle<> h);
    void wait_readable(int fd, std::coroutine_handle<> h);
    void wait_writable(int fd, std::coroutine_handle<> h);
    void add_timer(TimePoint t, std::coroutine_handle<> h);

private:
    std::deque<std::coroutine_handle<>> ready_;
    EpollPoller poller_;
    TimerQueue timers_;
};
```

重点不是调度器写多复杂，而是：

```text
不要跨线程乱 resume
不要全局大锁
不要每个请求 malloc 很多小对象
不要 response 复制多次
不要每次请求新建连接
```

## 什么时候应该用 Fiber？

只有几种情况我会建议用 fiber：

### 1. 你有大量旧同步 C++ 代码

比如业务逻辑都是：

```cpp
auto a = fetch_a();
auto b = fetch_b(a);
auto c = fetch_c(b);
```

你不想改成 coroutine 传播，这时候 fiber 可以降低改造成本。

### 2. 你要兼容阻塞式第三方库

比如某些库没有 async API，但你又想在用户态调度它。不过注意：如果它真的阻塞了 OS thread，fiber 也救不了，除非你能 hook I/O 或者把它丢到专门线程池。

### 3. 你想做类似 Go 的通用任务运行时

比如不仅是 HTTP，还有文件 I/O、RPC、数据库、复杂业务调用链，都想用同步写法表达。这时候 fiber 可以考虑，但工程量会上升很多。

## 什么时候不该用 Fiber？

你的场景大概率属于这个：

```text
新写一个高性能 HTTP Client
请求流程可控
协议状态机可控
愿意用 co_await
重点是低延迟、高并发、少内存、少锁
```

那就不该优先 fiber。

## 和 Go 的差异

Go 的 goroutine 看起来像 fiber，但 Go runtime 有几个 C++ fiber 通常没有的优势：

```text
动态增长栈
runtime 级 netpoll 集成
抢占调度
syscall 感知
GC 感知
标准库全套配合
```

你在 C++ 里用 fiber，只是拿到了“stackful 切换”，没有自动得到 Go runtime 的全部能力。

所以更现实的 C++ 路线是：

```text
不要复刻 goroutine
而是复刻 Go net/http 在网络 I/O 上的效果
```

即：

```text
轻量任务
事件驱动 I/O
连接池
HTTP/2 stream 池
TLS/DNS 复用
per-core sharding
超时取消
背压控制
```

这些用 C++20 coroutine 完全能做。

## 最推荐路线

如果你自己做 runtime，我建议：

```text
第一版：
C++20 coroutine + epoll + timer queue + HTTP/1.1/2 连接池

第二版：
per-core event loop + origin sharding + 对象池 + 零拷贝/少拷贝 body

第三版：
HTTP/2 stream 调度优化 + TLS session cache + DNS cache + 连接预热

最后才考虑：
fiber 兼容层
```

也可以暴露两套 API：

```cpp
// 高性能原生 async API
Task<Response> get_async(std::string_view url);

// 用户友好的同步包装
Response get(std::string_view url) {
    return runtime.block_on(get_async(url));
}
```

这样既保留性能，也不用一上来引入 fiber。

## 一句话

**不需要纤程。对于新写的高性能网络客户端，C++20 stackless coroutine + per-core event loop 通常比 fiber 更轻、更可控、更适合极限性能。Fiber 主要适合兼容旧同步代码或追求 Go 式同步写法，不是高性能 HTTP Client 的必要条件。**

---

## Message 13

**role: user**

考虑跨平台通用性，如何实现不同平台编译期间调度自己的系统poll实现

---

## Message 14

**role: assistant**

可以，建议用**“上层统一协程调度，底层按平台编译选择 Poller/Proactor 后端”**的结构。

关键点是：**不要把 Linux `epoll`、macOS/BSD `kqueue`、Windows `IOCP` 强行抽象成完全一样的 poll API**。它们模型不同：

```text
Linux epoll     = readiness model，可读/可写就绪通知
macOS kqueue    = readiness model，可读/可写/用户事件/定时器
Windows IOCP    = completion model，异步操作完成通知
```

所以更合理的抽象层不是单纯叫 `Poller`，而是叫：

```text
EventLoop Backend
```

上层只关心：

```text
co_await read
co_await write
co_await connect
co_await timeout
co_await cancel
```

底层各平台自己实现。

---

## 一、推荐总体结构

目录可以这样设计：

```text
net/
  include/
    net/event_loop.hpp
    net/task.hpp
    net/socket.hpp
    net/backend.hpp
  src/
    event_loop.cpp
    socket_common.cpp
    platform/
      linux/
        epoll_backend.cpp
        socket_linux.cpp
      bsd/
        kqueue_backend.cpp
        socket_bsd.cpp
      windows/
        iocp_backend.cpp
        socket_windows.cpp
```

编译时根据平台选源文件：

```cmake
if(WIN32)
    target_sources(mynet PRIVATE
        src/platform/windows/iocp_backend.cpp
        src/platform/windows/socket_windows.cpp
    )
    target_compile_definitions(mynet PRIVATE MYNET_BACKEND_IOCP=1)
    target_link_libraries(mynet PRIVATE ws2_32 mswsock)
elseif(APPLE OR CMAKE_SYSTEM_NAME MATCHES "BSD")
    target_sources(mynet PRIVATE
        src/platform/bsd/kqueue_backend.cpp
        src/platform/bsd/socket_bsd.cpp
    )
    target_compile_definitions(mynet PRIVATE MYNET_BACKEND_KQUEUE=1)
elseif(LINUX)
    target_sources(mynet PRIVATE
        src/platform/linux/epoll_backend.cpp
        src/platform/linux/socket_linux.cpp
    )
    target_compile_definitions(mynet PRIVATE MYNET_BACKEND_EPOLL=1)
else()
    message(FATAL_ERROR "Unsupported platform")
endif()
```

这比在一堆业务代码里到处写 `#ifdef _WIN32` 干净很多。

---

## 二、不要把 `#ifdef` 扩散到上层

上层只包含统一接口：

```cpp
class EventLoop {
public:
    void run();
    void stop();

    void schedule(std::coroutine_handle<> h);

    Task<void> wait_readable(SocketHandle fd);
    Task<void> wait_writable(SocketHandle fd);
    Task<void> sleep_for(std::chrono::milliseconds dur);

    Task<std::size_t> async_read(SocketHandle fd, void* buf, std::size_t len);
    Task<std::size_t> async_write(SocketHandle fd, const void* buf, std::size_t len);
};
```

底层根据平台实现。

业务层只写：

```cpp
auto n = co_await loop.async_read(sock, buf, len);
co_await loop.async_write(sock, data, size);
```

而不是：

```cpp
#ifdef __linux__
    epoll_ctl(...)
#elif _WIN32
    GetQueuedCompletionStatus(...)
#endif
```

---

## 三、Linux / macOS 可以抽象成 Reactor

Linux `epoll` 和 macOS/BSD `kqueue` 都适合做 readiness-based reactor。

统一事件可以设计成：

```cpp
enum class IoEvent : uint32_t {
    Read  = 1 << 0,
    Write = 1 << 1,
    Error = 1 << 2,
    Close = 1 << 3,
};

struct PollEvent {
    SocketHandle fd;
    uint32_t events;
    void* user_data;
};
```

Linux 后端大概是：

```cpp
class EpollBackend {
public:
    void register_fd(SocketHandle fd, uint32_t events, void* user_data);
    void modify_fd(SocketHandle fd, uint32_t events, void* user_data);
    void unregister_fd(SocketHandle fd);

    int poll(PollEvent* out, int max_events, int timeout_ms);
    void wakeup();
};
```

macOS/BSD 后端大概是：

```cpp
class KqueueBackend {
public:
    void register_fd(SocketHandle fd, uint32_t events, void* user_data);
    void modify_fd(SocketHandle fd, uint32_t events, void* user_data);
    void unregister_fd(SocketHandle fd);

    int poll(PollEvent* out, int max_events, int timeout_ms);
    void wakeup();
};
```

Linux 用：

```text
epoll_create1
epoll_ctl
epoll_wait
eventfd 或 pipe 唤醒
```

macOS/BSD 用：

```text
kqueue
kevent
EVFILT_READ
EVFILT_WRITE
EVFILT_USER 唤醒
```

---

## 四、Windows 不建议用 `select` / `WSAPoll`

如果要高性能，Windows 后端最好直接用 **IOCP**。

不要为了接口统一而退化成：

```text
select
poll
WSAPoll
```

这些能跑，但不适合你要的高性能 HTTP Client Runtime。

Windows 正确路线是：

```text
IOCP
+
Overlapped I/O
+
WSARecv / WSASend
+
ConnectEx
+
GetQueuedCompletionStatusEx
```

也就是说，Windows 不是等“fd 可读”，而是：

```text
提交一个异步读操作
        ↓
操作完成后 IOCP 返回 completion
        ↓
恢复对应 coroutine
```

所以 Windows 后端更像：

```cpp
class IocpBackend {
public:
    void submit_read(SocketHandle fd, IoOperation* op);
    void submit_write(SocketHandle fd, IoOperation* op);
    void submit_connect(SocketHandle fd, IoOperation* op);

    int poll(CompletionEvent* out, int max_events, int timeout_ms);
    void wakeup();
};
```

这和 epoll/kqueue 的 `wait_readable(fd)` 模型不完全一样。

---

## 五、统一抽象应该放在 “Awaiter 层”，不是 Poller 层

为了跨平台优雅，建议这样分层：

```text
HttpClient / Socket API
        ↓
Coroutine Awaiter
        ↓
EventLoop
        ↓
Backend
        ├── epoll
        ├── kqueue
        └── IOCP
```

也就是说，统一的是：

```cpp
co_await async_read(...)
co_await async_write(...)
co_await async_connect(...)
```

而不是强行统一：

```cpp
poll(fd, READABLE)
```

因为在 Windows IOCP 下，`async_read()` 本身就会提交 `WSARecv`，然后等待完成；在 Linux/macOS 下，`async_read()` 可能先尝试 `recv()`，如果返回 `EAGAIN`，再等待 readable。

伪代码：

```cpp
Task<std::size_t> Socket::async_read(void* buf, std::size_t len) {
#if MYNET_BACKEND_IOCP
    co_return co_await loop_.iocp_read(fd_, buf, len);
#else
    while (true) {
        auto n = ::recv(fd_, buf, len, 0);
        if (n >= 0) {
            co_return static_cast<std::size_t>(n);
        }

        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            co_await loop_.wait_readable(fd_);
            continue;
        }

        throw SocketError(errno);
    }
#endif
}
```

但这个 `#if` 最好也藏在平台实现文件里，不要暴露到业务层。

---

## 六、编译期选择有两种方式

### 方式 1：CMake 选择不同 `.cpp`

这是最推荐的。

公共头文件：

```cpp
// backend.hpp
namespace net {

class NativeBackend;

using Backend = NativeBackend;

}
```

平台实现分别编译：

```text
epoll_backend.cpp   定义 NativeBackend = EpollBackend
kqueue_backend.cpp  定义 NativeBackend = KqueueBackend
iocp_backend.cpp    定义 NativeBackend = IocpBackend
```

优点：

```text
上层干净
编译产物只包含当前平台代码
不会在头文件里污染一堆平台宏
```

### 方式 2：头文件里 `using DefaultBackend = ...`

例如：

```cpp
#if defined(_WIN32)
#include "net/platform/windows/iocp_backend.hpp"
namespace net {
using DefaultBackend = IocpBackend;
}
#elif defined(__APPLE__) || defined(__FreeBSD__)
#include "net/platform/bsd/kqueue_backend.hpp"
namespace net {
using DefaultBackend = KqueueBackend;
}
#elif defined(__linux__)
#include "net/platform/linux/epoll_backend.hpp"
namespace net {
using DefaultBackend = EpollBackend;
}
#endif
```

然后：

```cpp
template<class Backend = DefaultBackend>
class EventLoop {
    Backend backend_;
};
```

这种方式性能好，基本无虚函数开销，但头文件会更复杂。

---

## 七、性能敏感时，少用虚函数热路径

如果你只在一个平台上编译运行，那么不需要运行时多态。

不要这样：

```cpp
class IPoller {
public:
    virtual int poll(...) = 0;
    virtual void register_fd(...) = 0;
};
```

这不是不能用，但在高性能库里，热路径最好静态分发。

更推荐：

```cpp
template<class Backend>
class BasicEventLoop {
public:
    void run() {
        backend_.poll(...);
    }

private:
    Backend backend_;
};

using EventLoop = BasicEventLoop<DefaultBackend>;
```

这样编译器可以内联更多代码。

不过也要现实一点：真正的瓶颈通常是系统调用、网络、TLS、HTTP parser、内存分配。虚函数不是第一瓶颈。第一版可以先用接口，后面再优化。

---

## 八、定时器不要一开始依赖平台 timerfd

为了跨平台，建议上层自己维护 timer heap：

```text
TimerQueue = min-heap
```

每次 event loop poll 前计算最近定时器：

```cpp
int timeout_ms = timer_queue.next_timeout_ms();
backend.poll(events, max_events, timeout_ms);
```

醒来后处理到期定时器。

这样三端都通用：

```text
Linux   epoll_wait(timeout)
macOS   kevent(timeout)
Windows GetQueuedCompletionStatusEx(timeout)
```

后面如果极限优化，Linux 可以加 `timerfd`，macOS 可以加 `EVFILT_TIMER`，但第一版不必。

---

## 九、唤醒机制也要平台化

当其他线程投递任务到某个 EventLoop 时，需要唤醒它。

对应实现：

```text
Linux:
  eventfd，或者 pipe

macOS/BSD:
  EVFILT_USER，或者 pipe

Windows:
  PostQueuedCompletionStatus
```

统一接口：

```cpp
class Backend {
public:
    void wakeup();
};
```

上层：

```cpp
void EventLoop::schedule(std::coroutine_handle<> h) {
    {
        std::lock_guard lock(mutex_);
        ready_queue_.push_back(h);
    }
    backend_.wakeup();
}
```

更高性能的 per-core 模型里，跨线程投递应该尽量少，避免频繁 wakeup。

---

## 十、SocketHandle 也要统一

Unix 是 `int`，Windows 是 `SOCKET`。

```cpp
#ifdef _WIN32
using SocketHandle = SOCKET;
constexpr SocketHandle invalid_socket = INVALID_SOCKET;
#else
using SocketHandle = int;
constexpr SocketHandle invalid_socket = -1;
#endif
```

关闭 socket 也不同：

```cpp
#ifdef _WIN32
::closesocket(fd);
#else
::close(fd);
#endif
```

非阻塞设置也不同：

```cpp
#ifdef _WIN32
u_long mode = 1;
ioctlsocket(fd, FIONBIO, &mode);
#else
int flags = fcntl(fd, F_GETFL, 0);
fcntl(fd, F_SETFL, flags | O_NONBLOCK);
#endif
```

这些都应该放在 `socket_platform.cpp`，不要散落在 HTTP 层。

---

## 十一、HTTP Client Runtime 的跨平台推荐架构

可以这样设计：

```text
HttpClient
  ↓
RequestScheduler
  ↓
ShardRouter
  ↓
EventLoop
  ↓
PlatformBackend
      ├── Linux: epoll
      ├── macOS/BSD: kqueue
      └── Windows: IOCP
```

每个 EventLoop 持有：

```text
ready queue
timer queue
completion queue
origin pool
socket registry
platform backend
```

每个平台只实现最底层：

```text
socket 创建/关闭/设置
connect/read/write awaiter
poll/completion 等待
wakeup
timer timeout
```

HTTP/1.1、HTTP/2、TLS、连接池这些尽量写成平台无关。

---

## 十二、Linux 是否要用 io_uring？

可以作为后期后端，不建议第一版就上。

建议优先级：

```text
第一版：
Linux epoll
macOS kqueue
Windows IOCP

第二版：
Linux io_uring 可选后端

第三版：
根据压测决定是否默认启用 io_uring
```

原因是 `io_uring` 的收益依赖场景。对纯网络 socket，它不一定稳定碾压 epoll；而且工程复杂度、内核版本差异、fallback 都要处理。

可以这样做：

```cmake
option(MYNET_ENABLE_IO_URING "Enable io_uring backend on Linux" OFF)

if(LINUX AND MYNET_ENABLE_IO_URING)
    target_sources(mynet PRIVATE src/platform/linux/io_uring_backend.cpp)
    target_compile_definitions(mynet PRIVATE MYNET_BACKEND_IO_URING=1)
    target_link_libraries(mynet PRIVATE uring)
else()
    target_sources(mynet PRIVATE src/platform/linux/epoll_backend.cpp)
    target_compile_definitions(mynet PRIVATE MYNET_BACKEND_EPOLL=1)
endif()
```

---

## 十三、最终建议

你的跨平台抽象不要设计成：

```cpp
poller.wait_readable(fd);
poller.wait_writable(fd);
```

然后强行让 Windows 适配它。

更推荐设计成：

```cpp
co_await socket.async_connect(addr);
co_await socket.async_read(buf);
co_await socket.async_write(buf);
co_await timer.sleep_for(10ms);
```

底层：

```text
Linux/macOS:
  非阻塞 socket + readiness wait + recv/send

Windows:
  Overlapped socket + IOCP completion
```

也就是：

```text
统一协程 API
不统一底层 I/O 模型
```

一句话总结：**跨平台高性能网络调度的正确做法是：编译期选择平台 Backend，Linux 用 epoll，macOS/BSD 用 kqueue，Windows 用 IOCP；上层统一在 coroutine awaiter 和 EventLoop 层，而不是强行把所有平台抽象成同一个 poll。**

---

## Message 15

**role: user**

管理好这些资源的创建和释放是复杂的事情么，以及纯C实现上述这些是否会很复杂

---

## Message 16

**role: assistant**

是的，**资源创建和释放会是整个项目里最容易出 bug 的部分之一**。而且如果你用**纯 C 实现**，复杂度会明显上升，不是因为 C 性能不行，而是因为 C 没有 RAII、没有析构、没有类型安全的所有权表达，需要你自己把生命周期规则写得非常严谨。

结论可以先说：

```text
C++ 实现：复杂，但可控，RAII + move-only handle + coroutine frame 能显著降低错误率
纯 C 实现：可以做到很高性能，但生命周期管理、取消、错误回滚、并发释放会很复杂
```

## 一、复杂点主要不在“创建”，而在“谁负责释放”

高性能 HTTP Client Runtime 里面会有很多资源：

```text
EventLoop
Poller / IOCP handle
socket fd / SOCKET
TLS context
TLS session
DNS cache entry
HTTP/2 session
HTTP/2 stream
request object
response object
header buffer
body buffer
timer
coroutine/task handle
completion object
callback/user_data
跨线程队列节点
```

单独看每个都不复杂，复杂的是它们之间有依赖关系。

例如一个 HTTP/2 请求的生命周期可能是：

```text
Request
  ↓
绑定到 OriginPool
  ↓
绑定到 H2Session
  ↓
分配 H2Stream
  ↓
注册超时 Timer
  ↓
写入 TLS buffer
  ↓
写入 socket
  ↓
等待响应 frame
  ↓
完成 / 取消 / 超时 / 连接断开
  ↓
释放 Stream
  ↓
归还 buffer
  ↓
取消 timer
  ↓
通知上层
```

中间任何一步失败，都要正确回滚。

比如：

```text
TLS 握手失败
HTTP/2 stream 被 RST
socket 被远端关闭
请求超时
用户主动 cancel
EventLoop 正在 shutdown
completion queue 已满
上层提前释放 client
```

这些状态叠加后，资源释放就会很容易出错。

## 二、C++ 的优势：RAII 能把生命周期绑定到对象

C++ 可以这样管理 fd：

```cpp
class Socket {
public:
    explicit Socket(int fd = -1) : fd_(fd) {}

    ~Socket() {
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }

    Socket(const Socket&) = delete;
    Socket& operator=(const Socket&) = delete;

    Socket(Socket&& other) noexcept : fd_(other.fd_) {
        other.fd_ = -1;
    }

    Socket& operator=(Socket&& other) noexcept {
        if (this != &other) {
            reset();
            fd_ = other.fd_;
            other.fd_ = -1;
        }
        return *this;
    }

    void reset(int new_fd = -1) {
        if (fd_ >= 0) {
            ::close(fd_);
        }
        fd_ = new_fd;
    }

    int get() const { return fd_; }

private:
    int fd_;
};
```

这种写法的好处是：

```text
对象离开作用域自动释放
异常/早返回也不会漏 fd
禁止复制，避免 double close
支持 move，明确所有权转移
```

TLS、timer、buffer、stream handle 都可以类似封装。

这就是 C++ 比 C 更适合写这类 runtime 的核心原因之一。

## 三、纯 C 不是不能写，但要自己实现“手动 RAII”

C 里通常要写成这样：

```c
int client_request(...) {
    socket_t sock = INVALID_SOCKET;
    tls_t* tls = NULL;
    buffer_t* buf = NULL;
    int rc = -1;

    sock = socket_create();
    if (sock == INVALID_SOCKET) goto cleanup;

    tls = tls_create();
    if (!tls) goto cleanup;

    buf = buffer_acquire();
    if (!buf) goto cleanup;

    rc = do_request(sock, tls, buf);

cleanup:
    if (buf) buffer_release(buf);
    if (tls) tls_destroy(tls);
    if (sock != INVALID_SOCKET) socket_close(sock);

    return rc;
}
```

这种模式在 C 里很常见：

```text
初始化为无效值
每一步失败 goto cleanup
cleanup 里按逆序释放
每个 destroy 函数必须允许 NULL / invalid handle
```

这可以做得很稳，但大型项目里会非常啰嗦。问题是，当资源关系变成异步后，`goto cleanup` 也不够用了。

## 四、异步场景下，纯 C 的复杂度会陡增

同步 C 代码的资源管理还比较好写。

难的是异步：

```text
请求提交后函数已经返回
资源不能在当前栈上
socket 事件未来才回来
timer 未来才触发
用户可能提前 cancel
连接可能被 pool 复用
response 可能还没被上层消费
```

这时候你需要堆上的 operation 对象：

```c
typedef struct http_op {
    atomic_int refcnt;
    int state;

    socket_t sock;
    tls_t* tls;
    h2_stream_t* stream;
    timer_handle_t timer;

    buffer_t* req_buf;
    buffer_t* resp_buf;

    void (*callback)(struct http_op* op, void* user);
    void* user;

    bool cancelled;
    bool completed;
} http_op_t;
```

然后你要非常小心：

```text
socket ready 回调可能访问 op
timer 回调可能访问 op
cancel 也可能访问 op
completion queue 也持有 op
上层用户也可能持有 op handle
```

所以你必须做引用计数：

```c
http_op_retain(op);
http_op_release(op);
```

否则很容易出现：

```text
use-after-free
double-free
timer 回调访问已释放对象
socket 回调访问已关闭 fd
cancel 和 complete 同时触发
```

这才是纯 C 异步 runtime 真正复杂的地方。

## 五、最难的是 cancel / timeout / shutdown

高性能请求库里，最容易写炸的是这几类逻辑。

### 1. timeout 和正常完成同时发生

例如：

```text
线程 A：socket 收到完整响应，准备 complete
线程 B：timer 到期，准备 timeout
```

你必须保证只有一个路径赢：

```c
if (atomic_compare_exchange_strong(&op->state, RUNNING, COMPLETING)) {
    complete_success(op);
} else {
    // 已经被 cancel/timeout/complete，不能重复释放
}
```

### 2. 用户 cancel 和网络事件同时发生

```text
用户 cancel request
但 epoll 里已经有这个 fd 的 readable event
```

你要确保 readable handler 看见 request 已取消，不再恢复已释放任务。

### 3. EventLoop shutdown

shutdown 时要处理：

```text
未完成请求
已排队但未发送请求
正在 TLS 握手的连接
正在复用的 HTTP/2 stream
空闲连接池
timer queue
completion queue
```

这比单纯 close 所有 fd 复杂。

### 4. HTTP/2 连接关闭

一个 HTTP/2 connection 上可能有很多 stream：

```text
H2Session
  ├── stream 1
  ├── stream 3
  ├── stream 5
  └── stream 7
```

如果 socket 断开，你需要把所有 active stream 全部 fail 掉，并且不能漏掉它们的 timer、buffer、completion。

## 六、C++ 可以用所有权模型把复杂度压下去

C++ 里可以分层：

```text
unique_ptr      表示独占所有权
shared_ptr      表示跨异步回调共享生命周期
weak_ptr        防止回调里悬挂访问
move-only type  表示资源不能复制
RAII destructor 自动释放底层句柄
scope guard     处理中途失败回滚
```

比如：

```cpp
struct RequestOp : std::enable_shared_from_this<RequestOp> {
    Socket socket;
    TimerHandle timer;
    Buffer request_buf;
    Buffer response_buf;

    std::atomic<State> state;

    void complete(Result r) {
        State expected = State::Running;
        if (!state.compare_exchange_strong(expected, State::Completed)) {
            return;
        }

        timer.cancel();
        // 归还 stream/buffer，投递 completion
    }
};
```

异步回调里持有：

```cpp
auto self = shared_from_this();
loop.wait_readable(fd, [self] {
    self->on_readable();
});
```

当然，`shared_ptr` 有开销。极限性能下可以换成 intrusive refcount 或对象池。但第一版用 `shared_ptr` 通常更安全。

## 七、纯 C 实现需要自己造这些东西

如果你坚持纯 C，大概率需要自己实现：

```text
引用计数 intrusive refcount
对象池 slab/pool allocator
handle table
状态机
defer/scope cleanup 宏
平台 socket 封装
timer handle 生命周期管理
completion queue
cancel token
错误码体系
日志/trace 体系
```

比如 C 里常见的安全资源释放风格：

```c
#define SAFE_CLOSE_SOCKET(s) \
    do { \
        if ((s) != INVALID_SOCKET) { \
            socket_close(s); \
            (s) = INVALID_SOCKET; \
        } \
    } while (0)

#define SAFE_FREE(p) \
    do { \
        if ((p) != NULL) { \
            free(p); \
            (p) = NULL; \
        } \
    } while (0)
```

但是这只能解决简单释放，解决不了异步所有权。

异步所有权通常需要：

```c
typedef struct ref_counted {
    atomic_uint refs;
    void (*destroy)(void*);
} ref_counted_t;

void retain(ref_counted_t* r) {
    atomic_fetch_add_explicit(&r->refs, 1, memory_order_relaxed);
}

void release(ref_counted_t* r) {
    if (atomic_fetch_sub_explicit(&r->refs, 1, memory_order_acq_rel) == 1) {
        r->destroy(r);
    }
}
```

这基本就是在 C 里手写一部分 C++ 的对象生命周期系统。

## 八、纯 C 的优势在哪里？

纯 C 也不是没有优势。

它的优势是：

```text
ABI 稳定
容易暴露给其他语言
编译器要求低
可控性极强
没有异常
没有模板膨胀
没有复杂 C++ ABI 问题
```

如果你想做一个底层 core library，然后给 C++、Python、Rust、Go 都绑定，C ABI 很有吸引力。

但内部实现不一定要纯 C。很常见的做法是：

```text
内部 C++ 实现
外部暴露 C ABI
```

例如：

```cpp
extern "C" {

mynet_client_t* mynet_client_create(const mynet_config_t* cfg);
void mynet_client_destroy(mynet_client_t* client);

uint64_t mynet_submit(
    mynet_client_t* client,
    const mynet_request_t* req
);

int mynet_poll(
    mynet_client_t* client,
    mynet_response_t* out,
    int max_count
);

}
```

外部看起来是 C，内部用 C++ RAII 管理复杂生命周期。这是我更推荐的。

## 九、如果你要写，高层建议这样分配复杂度

不要让每一层都直接管理全部资源。要分层所有权。

推荐：

```text
Client
  拥有所有 Shard/EventLoop

Shard/EventLoop
  拥有 Poller、TimerQueue、OriginPool

OriginPool
  拥有 H1 ConnectionPool / H2 SessionPool

Connection / H2Session
  拥有 socket、TLS、protocol state

RequestOp
  拥有 request buffer、response buffer、timer、completion 状态

Stream
  由 H2Session 创建和回收，RequestOp 只持有 handle
```

所有权规则要写死：

```text
Client destroy 前必须 stop 所有 EventLoop
EventLoop stop 时必须 cancel 所有 RequestOp
H2Session close 时必须 fail 所有 active Stream
RequestOp complete/cancel/timeout 只能发生一次
Timer cancel 后不能再访问已释放 op
Socket 关闭前必须从 poller 注销
Completion 被用户取走后才释放 response buffer
```

这类规则比代码本身更重要。

## 十、具体建议

如果你的目标是高性能并且工程可控，我建议：

```text
内部：C++20
资源：RAII + move-only handle + intrusive refcount/object pool
异步：coroutine + EventLoop
外部：C ABI
```

也就是：

```text
不要纯 C 写完整 runtime
也不要把 C++ ABI 暴露出去
```

这是比较平衡的方案。

## 十一、纯 C 可以怎么做才不至于失控？

如果必须纯 C，我建议强制采用这些规则：

```text
所有资源 struct 都有 init/destroy
destroy 必须允许重复安全调用或明确不可重复
所有 handle 初始化为 invalid
所有函数返回错误码，不隐式释放入参
所有异步对象必须引用计数
所有状态转换必须 CAS 或在 event loop 单线程内完成
所有 socket 事件回调先检查 state
所有 timer 回调先 retain op，再判断 state
所有 completion 由用户显式 release
```

并且尽量采用：

```text
每个 EventLoop 单线程拥有自己的资源
跨线程只通过 MPSC 队列投递 command
避免多个线程同时操作同一个 socket/session/request
```

这样可以大幅减少锁和竞态。

## 十二、一句话总结

**管理这些资源本身非常复杂，复杂度主要来自异步生命周期、取消、超时、连接复用和跨线程事件竞争。C++ 可以用 RAII、move-only handle、智能指针或 intrusive refcount 把复杂度压住；纯 C 当然能写，而且性能可以很高，但你需要手写引用计数、状态机、cleanup 规则和对象池，工程复杂度会明显高很多。**

最推荐的工程形态是：

```text
C++ 实现核心 runtime
C ABI 暴露接口
内部不用异常或谨慎用异常
资源全部 RAII
热路径用对象池 / intrusive refcount
```

这样既有 C 的 ABI 稳定性，又不会把生命周期管理完全变成手工劳动。

---

## Message 17

**role: user**

将我们的对话导出为一个.md，包含role，还有对话内容
