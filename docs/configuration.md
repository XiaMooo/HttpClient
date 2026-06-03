# Runtime Configuration

本文记录当前 `HttpClient` runtime 的初始化参数、默认值和 benchmark 映射。

## HttpClient

`HttpClient::Options` 是推荐入口，内部包含 H1/H2 子配置。

| 参数 | 默认值 | 说明 |
|---|---:|---|
| `max_cached_origins` | `4096` | `HttpClient` origin/protocol cache 的 LRU 容量。超过后只驱逐非探测中的稳定条目。`0` 表示不按容量驱逐。 |
| `origin_cache_ttl` | `300s` | `HttpClient` origin/protocol cache 的空闲 TTL。命中会刷新时间。`0s` 表示不按 TTL 过期。 |
| `h2_failure_ttl` | `30s` | H2 尝试失败后临时降级为 H1 的重试间隔。 |
| `origin_waiter_limit` | `32` | 同 origin ALPN/protocol 探测时的等待队列上限。 |
| `detection_overflow_policy` | `FallbackH1` | 探测等待队列满时的策略：低延迟优先走 H1，严格路由则等待探测结果。 |
| `runtime_profile` | `Auto` | Runtime profile。`Auto` 低并发保持轻量，高并发按压力扩大 H1/H2 并行度；`Balanced` 保守固定资源；`Throughput` 固定高吞吐资源。 |

`DetectionOverflowPolicy::WaitForDetection` 适合 benchmark 和协议正确性优先的场景；
`FallbackH1` 适合线上低延迟兜底，但 H2-capable origin 在探测期间可能临时走 H1。

## H1 Pool

| 参数 | 默认值 | 说明 |
|---|---:|---|
| `h1.shard_count` | `0` | H1 shard 数。`0` 表示自动选择，当前最多 4 个 shard。 |
| `h1.auto_shards` | `false` | 由 `RuntimeProfile::Auto` 设置。开启后同 origin 初始只使用少量 shard，并按 in-flight 压力逐步扩大活跃 shard 窗口。 |
| `h1.max_connections_per_origin` | `128` | 每个 H1 origin 的最大连接数。Auto profile 会把默认有效上限收窄到 32，降低 TLS connection 和内存压力；Throughput profile 会放大该值以追求极限吞吐。没有 idle 且达到上限时，请求等待连接归还；idle 连接归还时固定优先唤醒已等待请求，避免新请求插队造成 p99 饥饿。 |
| `h1.max_origins_per_shard` | `4096` | 每个 H1 shard 的 origin pool LRU 容量。`0` 表示不按容量驱逐。 |
| `h1.origin_idle_ttl` | `300s` | H1 origin pool 空闲 TTL。只在新 origin 创建时触发懒驱逐。`0s` 表示不按 TTL 过期。 |
| `h1.maintenance_interval` | `10s` | H1 shard 级后台维护周期。清理过期 idle origin，并在 Auto 低负载持续一段时间后收窄 active shard 窗口。`0s` 表示关闭。 |
| `h1.auto_scale_down_idle_ttl` | `60s` | Auto H1 持续低负载多久后允许逐级缩档。 |
| `h1.auto_scale_up_interval` | `25ms` | Auto H1 升档最小间隔，避免突发流量一次性打开所有 shard。 |
| `h1.enable_ssl_verify` | `true` | H1 TLS peer/host 校验总开关。 |
| `h1.stripe_origins_across_shards` | `false` | 实验参数。开启后同 origin 请求会打散到多个 shard，会提升并行度但弱化同 origin 池复用。 |
| `h1.use_lightweight_h1` | `true` | 使用轻量 H1 parser/write path。 |
| `h1.use_h1_connection_actor` | `false` | 实验参数。按 actor 串行驱动 H1 TLS connection。 |
| `h1.h1_actor_connections_per_origin` | `8` | actor 模式下每 origin 的 actor connection 数。 |

H1 pool 的连接归还固定采用 FIFO-fair idle reservation：如果已有请求在等待连接，
刚归还的 idle connection 会预留给被唤醒的 waiter，新请求不能插队抢走它。
这个行为不提供配置开关，因为它是防止少量请求被饿到整轮末尾的尾延迟稳定性语义。

高并发单 H1 origin 下，`max_connections_per_origin` 不应盲目调大。当前本地 TLS bench
显示：Auto 有效上限 32 在 100k/1M mixed 场景下比 64 更低 RSS、更少 TLS connection，
且吞吐和 p99 没有退化。256/512 这类大连接数只适合作为 Throughput 压测选项。调参建议
使用 `scripts/run_h1_pool_scan.sh` 同时观察 `h1_conn_created`：热路径里该值应接近 0。

## H2 Session

| 参数 | 默认值 | 说明 |
|---|---:|---|
| `h2.sessions_per_origin` | `1` | H2 session 数。当前 H2Client 是按 session 轮询，不是完整 per-origin shard pool。 |
| `h2.shard_count` | `0` | H2 owned io_context 数。`0` 表示使用调用方 io_context；Auto/Throughput 可设置为多 shard。 |
| `h2.auto_shards` | `false` | 由 `RuntimeProfile::Auto` 设置。开启后每 origin 初始使用 1 个 H2 session，按 in-flight 压力逐步扩大到更多 session/shard。 |
| `h2.max_concurrent_streams` | `128` | 本地 H2 stream admission 上限，最终会与 peer `SETTINGS_MAX_CONCURRENT_STREAMS` 取较小值。 |
| `h2.session_group_idle_ttl` | `300s` | H2 session group 空闲 TTL。 |
| `h2.maintenance_interval` | `10s` | H2 后台维护周期。只在 H2 owned shard 存在时启用，不占用调用方外部 `io_context`。 |
| `h2.auto_scale_down_idle_ttl` | `60s` | Auto H2 持续低负载多久后允许逐级减少 active session。 |
| `h2.auto_scale_up_interval` | `25ms` | Auto H2 中低压力升档最小间隔，避免短突发一次性建立多个 H2/TLS connection；高压力阈值下会直接升到目标 active session 数，减少 measured 阶段的 stream slot 等待。 |
| `h2.verify_tls` | `true` | H2 TLS 校验开关。 |

## Recommended Presets

默认 Auto profile：

```cpp
httpclient::HttpClient client;  // runtime_profile = Auto
```

Auto profile 会保留低并发轻量路径，并在高并发时逐步扩大 H1 active shard
窗口和 H2 active session 数。后台 maintenance timer 会清理 idle origin/session，并在持续
低负载后逐级缩小 active shard/session 窗口。它不会在流量抖动中频繁销毁
`io_context` 线程；线程在 client 生命周期内保持，连接和 session 才会按 TTL/LRU 回收。

Benchmark 严格路由：

```cpp
httpclient::HttpClient::Options options;
options.detection_overflow_policy =
    httpclient::HttpClient::DetectionOverflowPolicy::WaitForDetection;
options.runtime_profile = httpclient::HttpClient::RuntimeProfile::Throughput;
options.origin_waiter_limit = 128;
options.h1.max_connections_per_origin = 128;
options.h2.sessions_per_origin = 2;
options.h2.max_concurrent_streams = 128;
```

线上默认建议：

```cpp
httpclient::HttpClient::Options options;
options.max_cached_origins = 4096;
options.runtime_profile = httpclient::HttpClient::RuntimeProfile::Auto;
options.origin_cache_ttl = std::chrono::seconds(300);
options.h2_failure_ttl = std::chrono::seconds(30);
options.origin_waiter_limit = 64;
options.detection_overflow_policy =
    httpclient::HttpClient::DetectionOverflowPolicy::FallbackH1;
options.h1.max_connections_per_origin = 32;
options.h2.sessions_per_origin = 1;
```

高 origin 数场景：

```cpp
httpclient::HttpClient::Options options;
options.max_cached_origins = 16384;
options.origin_cache_ttl = std::chrono::seconds(120);
options.h1.max_origins_per_shard = 8192;
options.h1.origin_idle_ttl = std::chrono::seconds(120);
```

## Benchmark Mapping

`src/httpclient_bench.cpp` 支持以下配置参数：

| CLI 参数 | Options 字段 |
|---|---|
| `--origin-waiters N` | `origin_waiter_limit` |
| `--strict-detect` | `detection_overflow_policy = WaitForDetection` |
| `--max-cached-origins N` | `max_cached_origins` |
| `--origin-cache-ttl-sec N` | `origin_cache_ttl` |
| `--h2-failure-ttl-sec N` | `h2_failure_ttl` |
| `--auto-profile` | `runtime_profile = Auto` |
| `--balanced` | `runtime_profile = Balanced` |
| `--throughput` | `runtime_profile = Throughput` |
| `--h1-shards N` | `h1.shard_count` |
| `--h1-max-connections-per-origin N` | `h1.max_connections_per_origin` |
| `--h1-max-origins-per-shard N` | `h1.max_origins_per_shard` |
| `--h1-origin-idle-ttl-sec N` | `h1.origin_idle_ttl` |
| `--stripe-h1-origin-shards` | `h1.stripe_origins_across_shards = true` |
| `--disable-lightweight-h1` | `h1.use_lightweight_h1 = false` |
| `--h1-actor` | `h1.use_h1_connection_actor = true` |
| `--h1-actor-connections N` | `h1.h1_actor_connections_per_origin` |
| `--h2-sessions N` | `h2.sessions_per_origin` |
| `--h2-shards N` | `h2.shard_count` |
| `--h2-max-streams N` | `h2.max_concurrent_streams` |

`scripts/run_mixed_protocol_bench.sh` 也暴露了对应环境变量：

```bash
MAX_CACHED_ORIGINS=8192 \
ORIGIN_CACHE_TTL_SEC=600 \
H2_FAILURE_TTL_SEC=10 \
PROFILE=auto \
H1_SHARDS=0 \
STRIPE_H1_ORIGIN_SHARDS=0 \
H1_MAX_CONNECTIONS_PER_ORIGIN=0 \
H1_MAX_ORIGINS_PER_SHARD=8192 \
H1_ORIGIN_IDLE_TTL_SEC=600 \
H2_SESSIONS=2 \
H2_SHARDS=0 \
H2_MAX_STREAMS=128 \
STRICT_DETECT=1 \
scripts/run_mixed_protocol_bench.sh
```

benchmark 脚本默认 `PROFILE=auto`。需要固定高吞吐资源时使用
`PROFILE=throughput`；需要保守资源对照时使用 `PROFILE=balanced`。
