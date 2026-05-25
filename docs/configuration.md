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

`DetectionOverflowPolicy::WaitForDetection` 适合 benchmark 和协议正确性优先的场景；
`FallbackH1` 适合线上低延迟兜底，但 H2-capable origin 在探测期间可能临时走 H1。

## H1 Pool

| 参数 | 默认值 | 说明 |
|---|---:|---|
| `h1.shard_count` | `0` | H1 shard 数。`0` 表示自动选择，当前最多 4 个 shard。 |
| `h1.max_connections_per_origin` | `64` | 每个 H1 origin 的最大连接数。没有 idle 且达到上限时，请求等待连接归还。 |
| `h1.max_origins_per_shard` | `4096` | 每个 H1 shard 的 origin pool LRU 容量。`0` 表示不按容量驱逐。 |
| `h1.origin_idle_ttl` | `300s` | H1 origin pool 空闲 TTL。只在新 origin 创建时触发懒驱逐。`0s` 表示不按 TTL 过期。 |
| `h1.enable_ssl_verify` | `true` | H1 TLS peer/host 校验总开关。 |
| `h1.stripe_origins_across_shards` | `false` | 实验参数。开启后同 origin 请求会打散到多个 shard，会提升并行度但弱化同 origin 池复用。 |
| `h1.use_lightweight_h1` | `true` | 使用轻量 H1 parser/write path。 |
| `h1.use_h1_connection_actor` | `false` | 实验参数。按 actor 串行驱动 H1 TLS connection。 |
| `h1.h1_actor_connections_per_origin` | `8` | actor 模式下每 origin 的 actor connection 数。 |

## H2 Session

| 参数 | 默认值 | 说明 |
|---|---:|---|
| `h2.sessions_per_origin` | `1` | H2 session 数。当前 H2Client 是按 session 轮询，不是完整 per-origin shard pool。 |
| `h2.max_concurrent_streams` | `128` | 本地 H2 stream admission 上限，最终会与 peer `SETTINGS_MAX_CONCURRENT_STREAMS` 取较小值。 |
| `h2.verify_tls` | `true` | H2 TLS 校验开关。 |

## Recommended Presets

Benchmark 严格路由：

```cpp
httpclient::HttpClient::Options options;
options.detection_overflow_policy =
    httpclient::HttpClient::DetectionOverflowPolicy::WaitForDetection;
options.origin_waiter_limit = 128;
options.h1.max_connections_per_origin = 128;
options.h2.sessions_per_origin = 2;
options.h2.max_concurrent_streams = 128;
```

线上默认建议：

```cpp
httpclient::HttpClient::Options options;
options.max_cached_origins = 4096;
options.origin_cache_ttl = std::chrono::seconds(300);
options.h2_failure_ttl = std::chrono::seconds(30);
options.origin_waiter_limit = 64;
options.detection_overflow_policy =
    httpclient::HttpClient::DetectionOverflowPolicy::FallbackH1;
options.h1.max_connections_per_origin = 64;
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
| `--h1-shards N` | `h1.shard_count` |
| `--h1-max-connections-per-origin N` | `h1.max_connections_per_origin` |
| `--h1-max-origins-per-shard N` | `h1.max_origins_per_shard` |
| `--h1-origin-idle-ttl-sec N` | `h1.origin_idle_ttl` |
| `--stripe-h1-origin-shards` | `h1.stripe_origins_across_shards = true` |
| `--disable-lightweight-h1` | `h1.use_lightweight_h1 = false` |
| `--h1-actor` | `h1.use_h1_connection_actor = true` |
| `--h1-actor-connections N` | `h1.h1_actor_connections_per_origin` |
| `--h2-sessions N` | `h2.sessions_per_origin` |
| `--h2-max-streams N` | `h2.max_concurrent_streams` |

`scripts/run_mixed_protocol_bench.sh` 也暴露了对应环境变量：

```bash
MAX_CACHED_ORIGINS=8192 \
ORIGIN_CACHE_TTL_SEC=600 \
H2_FAILURE_TTL_SEC=10 \
H1_SHARDS=4 \
H1_MAX_CONNECTIONS_PER_ORIGIN=128 \
H1_MAX_ORIGINS_PER_SHARD=8192 \
H1_ORIGIN_IDLE_TTL_SEC=600 \
H2_SESSIONS=2 \
H2_MAX_STREAMS=128 \
STRICT_DETECT=1 \
scripts/run_mixed_protocol_bench.sh
```
