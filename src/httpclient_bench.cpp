#include "asyncx/asyncx.hpp"
#include "httpclient/http_client.hpp"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#if defined(__linux__) || defined(__APPLE__)
#include <sys/resource.h>
#endif

namespace asio = boost::asio;

namespace {

struct ProcessMemoryStats {
  std::uint64_t rss_kb = 0;
  std::uint64_t peak_rss_kb = 0;
};

struct ProcessCpuStats {
  std::uint64_t user_us = 0;
  std::uint64_t system_us = 0;
};

struct LatencyStats {
  std::uint64_t p50_us = 0;
  std::uint64_t p95_us = 0;
  std::uint64_t p99_us = 0;
  std::size_t samples = 0;
};

struct LatencyRecorder {
  static constexpr std::size_t kMaxSamples = 200000;

  explicit LatencyRecorder(int requests) {
    auto total = static_cast<std::size_t>(std::max(0, requests));
    stride = total <= kMaxSamples
                 ? std::size_t{1}
                 : (total + kMaxSamples - 1) / kMaxSamples;
    auto sample_count = stride == 0 ? std::size_t{0}
                                    : (total + stride - 1) / stride;
    samples.resize(sample_count);
  }

  void record(int id, std::uint64_t latency_us) {
    if (id < 0 || stride == 0) {
      return;
    }
    auto index = static_cast<std::size_t>(id);
    if (index % stride != 0) {
      return;
    }
    auto sample_index = index / stride;
    if (sample_index < samples.size()) {
      samples[sample_index] = latency_us;
    }
  }

  std::vector<std::uint64_t> samples;
  std::size_t stride = 1;
};

ProcessCpuStats read_process_cpu_stats() {
  ProcessCpuStats stats;
#if defined(__linux__) || defined(__APPLE__)
  rusage usage{};
  if (getrusage(RUSAGE_SELF, &usage) == 0) {
    stats.user_us =
        static_cast<std::uint64_t>(usage.ru_utime.tv_sec) * 1000000ULL +
        static_cast<std::uint64_t>(usage.ru_utime.tv_usec);
    stats.system_us =
        static_cast<std::uint64_t>(usage.ru_stime.tv_sec) * 1000000ULL +
        static_cast<std::uint64_t>(usage.ru_stime.tv_usec);
  }
#endif
  return stats;
}

LatencyStats summarize_latencies(std::vector<std::uint64_t>& latencies_us) {
  LatencyStats stats;
  if (latencies_us.empty()) {
    return stats;
  }
  latencies_us.erase(std::remove(latencies_us.begin(), latencies_us.end(), 0),
                     latencies_us.end());
  if (latencies_us.empty()) {
    return stats;
  }
  std::sort(latencies_us.begin(), latencies_us.end());
  stats.samples = latencies_us.size();
  auto percentile = [&](double p) {
    auto index = static_cast<std::size_t>(
        std::ceil(p * static_cast<double>(latencies_us.size())) - 1.0);
    if (index >= latencies_us.size()) {
      index = latencies_us.size() - 1;
    }
    return latencies_us[index];
  };
  stats.p50_us = percentile(0.50);
  stats.p95_us = percentile(0.95);
  stats.p99_us = percentile(0.99);
  return stats;
}

ProcessMemoryStats read_process_memory_stats() {
  ProcessMemoryStats stats;
#if defined(__linux__)
  FILE* file = std::fopen("/proc/self/status", "r");
  if (!file) {
    return stats;
  }
  char line[256];
  while (std::fgets(line, sizeof(line), file)) {
    std::string_view text(line);
    auto colon = text.find(':');
    if (colon == std::string_view::npos) {
      continue;
    }
    auto key = text.substr(0, colon);
    std::uint64_t value = 0;
    std::istringstream input(std::string(text.substr(colon + 1)));
    input >> value;
    if (key == "VmRSS") {
      stats.rss_kb = value;
    } else if (key == "VmHWM") {
      stats.peak_rss_kb = value;
    }
  }
  std::fclose(file);
#endif
  return stats;
}

struct Args {
  std::string url = "https://127.0.0.1:8443/ping";
  std::string url_alt;
  int requests = 16;
  int concurrency = 4;
  int body_bytes = 0;
  int h2_sessions = 0;
  int h2_shards = 0;
  int h2_max_streams = 128;
  int origin_waiters = 32;
  int max_cached_origins = 4096;
  int origin_cache_ttl_sec = 300;
  int h2_failure_ttl_sec = 30;
  int h1_shards = 0;
  int h1_max_connections_per_origin = 0;
  int h1_max_connecting_per_origin = 0;
  int h1_max_origins_per_shard = 4096;
  int h1_origin_idle_ttl_sec = 300;
  int h1_actor_connections_per_origin = 8;
  int warmup_per_url = 0;
  int preconnect_per_url = 0;
  int max_retries = 0;
  int timeout_ms = 5000;
  int smooth_start_ms = 0;
  bool reset_connections_after_warmup = false;
  bool concurrent_warmup = false;
  bool sequential_warmup = false;
  bool store_response = false;
  bool insecure = false;
  bool no_proxy = false;
  bool strict_detect = false;
  bool stripe_h1_origin_shards = false;
  bool disable_lightweight_h1 = false;
  bool h1_actor = false;
  bool idempotency_key = false;
  httpclient::ProtocolPolicy policy = httpclient::ProtocolPolicy::Auto;
  bool mixed = false;
  bool mixed_shuffle = false;
  bool awaitable_mode = false;
  bool gather_mode = false;
  httpclient::HttpClient::RuntimeProfile runtime_profile =
      httpclient::HttpClient::RuntimeProfile::Auto;
};

Args parse_args(int argc, char** argv) {
  Args args;
  for (int i = 1; i < argc; ++i) {
    std::string_view s(argv[i]);
    auto next = [&](std::string_view key) { return i + 1 < argc && s == key; };
    if (next("--url")) {
      args.url = argv[++i];
    } else if (next("--url-alt")) {
      args.url_alt = argv[++i];
    } else if (next("--requests")) {
      args.requests = std::atoi(argv[++i]);
    } else if (next("--concurrency")) {
      args.concurrency = std::atoi(argv[++i]);
    } else if (next("--body-bytes")) {
      args.body_bytes = std::atoi(argv[++i]);
    } else if (next("--h2-sessions")) {
      args.h2_sessions = std::atoi(argv[++i]);
    } else if (next("--h2-shards")) {
      args.h2_shards = std::atoi(argv[++i]);
    } else if (next("--h2-max-streams")) {
      args.h2_max_streams = std::atoi(argv[++i]);
    } else if (next("--origin-waiters")) {
      args.origin_waiters = std::atoi(argv[++i]);
    } else if (next("--max-cached-origins")) {
      args.max_cached_origins = std::atoi(argv[++i]);
    } else if (next("--origin-cache-ttl-sec")) {
      args.origin_cache_ttl_sec = std::atoi(argv[++i]);
    } else if (next("--h2-failure-ttl-sec")) {
      args.h2_failure_ttl_sec = std::atoi(argv[++i]);
    } else if (next("--h1-shards")) {
      args.h1_shards = std::atoi(argv[++i]);
    } else if (next("--h1-max-connections-per-origin")) {
      args.h1_max_connections_per_origin = std::atoi(argv[++i]);
    } else if (next("--h1-max-connecting-per-origin")) {
      args.h1_max_connecting_per_origin = std::atoi(argv[++i]);
    } else if (next("--h1-max-origins-per-shard")) {
      args.h1_max_origins_per_shard = std::atoi(argv[++i]);
    } else if (next("--h1-origin-idle-ttl-sec")) {
      args.h1_origin_idle_ttl_sec = std::atoi(argv[++i]);
    } else if (next("--h1-actor-connections")) {
      args.h1_actor_connections_per_origin = std::atoi(argv[++i]);
    } else if (next("--warmup-per-url")) {
      args.warmup_per_url = std::atoi(argv[++i]);
    } else if (next("--preconnect-per-url")) {
      args.preconnect_per_url = std::atoi(argv[++i]);
    } else if (next("--max-retries")) {
      args.max_retries = std::atoi(argv[++i]);
    } else if (next("--timeout-ms")) {
      args.timeout_ms = std::atoi(argv[++i]);
    } else if (next("--smooth-start-ms")) {
      args.smooth_start_ms = std::atoi(argv[++i]);
    } else if (s == "--reset-connections-after-warmup") {
      args.reset_connections_after_warmup = true;
    } else if (s == "--concurrent-warmup") {
      args.concurrent_warmup = true;
    } else if (s == "--sequential-warmup") {
      args.sequential_warmup = true;
    } else if (s == "--store-response") {
      args.store_response = true;
    } else if (s == "--insecure") {
      args.insecure = true;
    } else if (s == "--no-proxy") {
      args.no_proxy = true;
    } else if (s == "--strict-detect") {
      args.strict_detect = true;
    } else if (s == "--stripe-h1-origin-shards") {
      args.stripe_h1_origin_shards = true;
    } else if (s == "--disable-lightweight-h1") {
      args.disable_lightweight_h1 = true;
    } else if (s == "--h1-actor") {
      args.h1_actor = true;
    } else if (s == "--idempotency-key") {
      args.idempotency_key = true;
    } else if (s == "--force-h1") {
      args.policy = httpclient::ProtocolPolicy::ForceH1;
    } else if (s == "--force-h2") {
      args.policy = httpclient::ProtocolPolicy::ForceH2;
    } else if (s == "--prefer-h1") {
      args.policy = httpclient::ProtocolPolicy::PreferH1;
    } else if (s == "--prefer-h2") {
      args.policy = httpclient::ProtocolPolicy::PreferH2;
    } else if (s == "--mixed") {
      args.mixed = true;
    } else if (s == "--mixed-shuffle") {
      args.mixed = true;
      args.mixed_shuffle = true;
    } else if (s == "--awaitable") {
      args.awaitable_mode = true;
    } else if (s == "--gather") {
      args.gather_mode = true;
      args.awaitable_mode = true;
    } else if (s == "--balanced") {
      args.runtime_profile = httpclient::HttpClient::RuntimeProfile::Balanced;
    } else if (s == "--throughput") {
      args.runtime_profile = httpclient::HttpClient::RuntimeProfile::Throughput;
    } else if (s == "--auto-profile") {
      args.runtime_profile = httpclient::HttpClient::RuntimeProfile::Auto;
    }
  }
  return args;
}

bool mixed_uses_alt(int id, bool shuffle) {
  if (!shuffle) {
    return id % 2 == 1;
  }
  auto x = static_cast<std::uint64_t>(id) + 0x9e3779b97f4a7c15ULL;
  x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
  x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
  x = x ^ (x >> 31);
  return (x & 1U) != 0;
}

httpclient::AsioHttpClient::Stats::TimingStats diff_timing(
    const httpclient::AsioHttpClient::Stats::TimingStats& after,
    const httpclient::AsioHttpClient::Stats::TimingStats& before) {
  return httpclient::AsioHttpClient::Stats::TimingStats{
      after.count - before.count,
      after.total_us - before.total_us,
      after.max_us,
  };
}

httpclient::AsioHttpClient::Stats diff_stats(const httpclient::AsioHttpClient::Stats& after,
                                             const httpclient::AsioHttpClient::Stats& before) {
  return httpclient::AsioHttpClient::Stats{
      after.h1_conn_created - before.h1_conn_created,
      after.h1_idle_hit - before.h1_idle_hit,
      after.h1_idle_miss - before.h1_idle_miss,
      after.h1_conn_reused - before.h1_conn_reused,
      after.h1_return_to_idle - before.h1_return_to_idle,
      after.h1_close_after_response - before.h1_close_after_response,
      after.h1_reuse_failed - before.h1_reuse_failed,
      after.h1_reconnect_after_idle - before.h1_reconnect_after_idle,
      after.h1_cancelled - before.h1_cancelled,
      after.h1_pool_wait_cancelled - before.h1_pool_wait_cancelled,
      after.h1_close_on_cancel - before.h1_close_on_cancel,
      diff_timing(after.h1_pool_wait, before.h1_pool_wait),
      diff_timing(after.h1_connect, before.h1_connect),
      diff_timing(after.h1_acquire, before.h1_acquire),
      diff_timing(after.h1_write, before.h1_write),
      diff_timing(after.h1_read_headers, before.h1_read_headers),
      diff_timing(after.h1_read_body, before.h1_read_body),
      diff_timing(after.h1_exchange, before.h1_exchange),
  };
}

std::uint64_t avg_us(const httpclient::AsioHttpClient::Stats::TimingStats& stats) {
  return stats.count == 0 ? 0 : stats.total_us / stats.count;
}

httpclient::HttpClient::Stats diff_stats(
    const httpclient::HttpClient::Stats& after,
    const httpclient::HttpClient::Stats& before) {
  httpclient::HttpClient::Stats out;
  out.probe_h1_adopted = after.probe_h1_adopted - before.probe_h1_adopted;
  out.probe_h2_marked = after.probe_h2_marked - before.probe_h2_marked;
  out.probe_reconnect = after.probe_reconnect - before.probe_reconnect;
  out.overflow_fallback = after.overflow_fallback - before.overflow_fallback;
  out.overflow_fallback_on_h2_origin =
      after.overflow_fallback_on_h2_origin - before.overflow_fallback_on_h2_origin;
  out.url_route_cache_hits =
      after.url_route_cache_hits - before.url_route_cache_hits;
  out.url_route_cache_misses =
      after.url_route_cache_misses - before.url_route_cache_misses;
  out.h1_cached_routes = after.h1_cached_routes - before.h1_cached_routes;
  out.h2_cached_routes = after.h2_cached_routes - before.h2_cached_routes;
  out.detect_waiters = after.detect_waiters - before.detect_waiters;
  out.detect_queue_overflow = after.detect_queue_overflow - before.detect_queue_overflow;
  out.detect_overflow_to_h1 = after.detect_overflow_to_h1 - before.detect_overflow_to_h1;
  out.detect_overflow_to_h1_later_h2 =
      after.detect_overflow_to_h1_later_h2 - before.detect_overflow_to_h1_later_h2;
  out.retry_status = after.retry_status - before.retry_status;
  out.retry_transport = after.retry_transport - before.retry_transport;
  out.h1_pool = diff_stats(after.h1_pool, before.h1_pool);
  out.h2_pool.streams_submitted =
      after.h2_pool.streams_submitted - before.h2_pool.streams_submitted;
  out.h2_pool.streams_completed =
      after.h2_pool.streams_completed - before.h2_pool.streams_completed;
  out.h2_pool.streams_timed_out =
      after.h2_pool.streams_timed_out - before.h2_pool.streams_timed_out;
  out.h2_pool.streams_cancelled =
      after.h2_pool.streams_cancelled - before.h2_pool.streams_cancelled;
  out.h2_pool.stream_slot_waits =
      after.h2_pool.stream_slot_waits - before.h2_pool.stream_slot_waits;
  out.h2_pool.stream_slot_wait_cancelled =
      after.h2_pool.stream_slot_wait_cancelled -
      before.h2_pool.stream_slot_wait_cancelled;
  out.h2_pool.connect_waits =
      after.h2_pool.connect_waits - before.h2_pool.connect_waits;
  out.h2_pool.connect_wait_cancelled =
      after.h2_pool.connect_wait_cancelled - before.h2_pool.connect_wait_cancelled;
  out.h2_pool.preconnect_attempts =
      after.h2_pool.preconnect_attempts - before.h2_pool.preconnect_attempts;
  out.h2_pool.preconnect_success =
      after.h2_pool.preconnect_success - before.h2_pool.preconnect_success;
  out.h2_pool.preconnect_failed =
      after.h2_pool.preconnect_failed - before.h2_pool.preconnect_failed;
  out.h2_pool.max_active_streams = after.h2_pool.max_active_streams;
  out.h2_pool.max_pending_stream_waiters =
      after.h2_pool.max_pending_stream_waiters;
  out.h2_pool.peer_max_concurrent_streams =
      after.h2_pool.peer_max_concurrent_streams;
  out.h2_pool.configured_max_concurrent_streams =
      after.h2_pool.configured_max_concurrent_streams;
  out.h2_pool.session_groups = after.h2_pool.session_groups;
  out.h2_pool.session_groups_evicted =
      after.h2_pool.session_groups_evicted - before.h2_pool.session_groups_evicted;
  out.h2_pool.session_group_cache_hits =
      after.h2_pool.session_group_cache_hits -
      before.h2_pool.session_group_cache_hits;
  out.h2_pool.session_group_cache_misses =
      after.h2_pool.session_group_cache_misses -
      before.h2_pool.session_group_cache_misses;
  return out;
}

asio::awaitable<void> run(Args args, httpclient::HttpClient& client) {
  struct State {
    std::atomic<int> issued = 0;
    std::atomic<int> ok = 0;
    std::atomic<int> fail = 0;
    std::atomic<int> h1 = 0;
    std::atomic<int> h2 = 0;
    std::atomic<int> completed = 0;
  };

  auto state = std::make_shared<State>();
  auto latencies = std::make_shared<LatencyRecorder>(args.requests);
  auto payload = std::make_shared<std::string>(
      static_cast<std::size_t>(std::max(0, args.body_bytes)), 'x');
  auto ex = co_await asio::this_coro::executor;
  auto done_timer = std::make_shared<asio::steady_timer>(ex);
  done_timer->expires_at(asio::steady_timer::time_point::max());

  auto urls = std::vector<std::string>{args.url};
  if (args.mixed && !args.url_alt.empty()) {
    urls.push_back(args.url_alt);
  }

  auto make_request = [&](const std::string& url) {
    httpclient::Request req;
    req.url = url;
    req.method = payload->empty() ? "GET" : "POST";
    req.body = *payload;
    req.timeout_ms = args.timeout_ms;
    req.verify_peer = !args.insecure;
    req.verify_host = !args.insecure;
    req.disable_proxy = args.no_proxy;
    req.measure_total_time = false;
    req.protocol_policy = args.policy;
    req.store_response_body = args.store_response;
    req.store_response_headers = args.store_response;
    req.max_retries = args.max_retries;
    if (args.idempotency_key) {
      req.set_header("Idempotency-Key", "httpclient-bench");
    }
    return req;
  };

  const bool use_concurrent_warmup =
      args.warmup_per_url > 0 &&
      (args.concurrent_warmup || !args.sequential_warmup);

  if (use_concurrent_warmup) {
    auto total = static_cast<std::size_t>(
        args.warmup_per_url * static_cast<int>(urls.size()));
    auto warmup_concurrency =
        static_cast<std::size_t>(std::max(1, std::min(args.concurrency,
                                                      static_cast<int>(total))));
    std::atomic<int> printed = 0;
    co_await asyncx::for_each_limited(
        total, warmup_concurrency, [&](std::size_t id) -> asio::awaitable<void> {
          auto url = urls[id % urls.size()];
          auto resp = co_await client.async_request(make_request(url));
          if (!resp.error.empty() && printed.fetch_add(1) < 3) {
            std::cerr << "warmup_error=" << resp.error << "\n";
          }
          co_return;
        },
        [](std::size_t, std::monostate) {});
  } else if (args.warmup_per_url > 0 && args.sequential_warmup) {
    for (int round = 0; round < args.warmup_per_url; ++round) {
      for (const auto& url : urls) {
        auto resp = co_await client.async_request(make_request(url));
        if (!resp.error.empty()) {
          std::cerr << "warmup_error=" << resp.error << "\n";
        }
      }
    }
  }

  if (args.reset_connections_after_warmup) {
    co_await client.reset_connections();
  }

  const auto preconnect_count =
      args.preconnect_per_url < 0
          ? static_cast<std::size_t>(
                std::max(1, std::min(args.concurrency * 2, args.requests)))
          : static_cast<std::size_t>(std::max(0, args.preconnect_per_url));
  if (preconnect_count > 0) {
    for (const auto& url : urls) {
      auto req = make_request(url);
      co_await client.preconnect(std::move(req), preconnect_count);
    }
  }

  auto setup_stats = client.stats();
  std::cout << "setup_preconnect_per_url=" << preconnect_count
            << " setup_probe_h1_adopted=" << setup_stats.probe_h1_adopted
            << " setup_probe_h2_marked=" << setup_stats.probe_h2_marked
            << " setup_probe_reconnect=" << setup_stats.probe_reconnect
            << " setup_url_route_cache_hits=" << setup_stats.url_route_cache_hits
            << " setup_url_route_cache_misses=" << setup_stats.url_route_cache_misses
            << " setup_h1_cached_routes=" << setup_stats.h1_cached_routes
            << " setup_h2_cached_routes=" << setup_stats.h2_cached_routes
            << " setup_h1_conn_created="
            << setup_stats.h1_pool.h1_conn_created
            << " setup_h1_conn_reused="
            << setup_stats.h1_pool.h1_conn_reused
            << " setup_h1_return_to_idle="
            << setup_stats.h1_pool.h1_return_to_idle
            << " setup_h2_preconnect_attempts="
            << setup_stats.h2_pool.preconnect_attempts
            << " setup_h2_preconnect_success="
            << setup_stats.h2_pool.preconnect_success
            << " setup_h2_preconnect_failed="
            << setup_stats.h2_pool.preconnect_failed
            << " setup_h2_session_groups="
            << setup_stats.h2_pool.session_groups
            << "\n";

  client.reset_stats();
  auto stats_before = client.stats();
  auto cpu_before = read_process_cpu_stats();
  auto start = std::chrono::steady_clock::now();

  auto handle_response = [&, state, done_timer, ex](int id,
                                                    httpclient::Response resp,
                                                    std::chrono::steady_clock::time_point started) {
    if (id >= 0 && id < args.requests) {
      latencies->record(
          id, static_cast<std::uint64_t>(
                  std::chrono::duration_cast<std::chrono::microseconds>(
                      std::chrono::steady_clock::now() - started)
                      .count()));
    }
    if (resp.http_version == 1) {
      state->h1.fetch_add(1);
    } else if (resp.http_version == 3) {
      state->h2.fetch_add(1);
    }
    if (resp.error.empty() && resp.status >= 200 && resp.status < 500) {
      state->ok.fetch_add(1);
    } else {
      if (state->fail.load() < 3 && !resp.error.empty()) {
        std::cerr << "error=" << resp.error << "\n";
      }
      state->fail.fetch_add(1);
    }
    if (state->completed.fetch_add(1) + 1 == args.requests) {
      asio::post(ex, [done_timer] { done_timer->cancel(); });
    }
  };

  auto make_indexed_request = [&](int id) {
    httpclient::Request req = make_request(
        (args.mixed && !args.url_alt.empty())
            ? (mixed_uses_alt(id, args.mixed_shuffle) ? args.url_alt : args.url)
            : args.url);
    return req;
  };

  auto smooth_start_delay = [&](int slot, int slots) -> asio::awaitable<void> {
    if (args.smooth_start_ms <= 0 || slots <= 1) {
      co_return;
    }
    auto delay_ms = (slot * args.smooth_start_ms) / slots;
    if (delay_ms <= 0) {
      co_return;
    }
    asio::steady_timer timer(co_await asio::this_coro::executor);
    timer.expires_after(std::chrono::milliseconds(delay_ms));
    boost::system::error_code ec;
    co_await timer.async_wait(asio::redirect_error(asio::use_awaitable, ec));
  };

  auto handle_response_no_timer = [&, state](int id, httpclient::Response resp,
                                             std::chrono::steady_clock::time_point started) {
    if (id >= 0 && id < args.requests) {
      latencies->record(
          id, static_cast<std::uint64_t>(
                  std::chrono::duration_cast<std::chrono::microseconds>(
                      std::chrono::steady_clock::now() - started)
                      .count()));
    }
    if (resp.http_version == 1) {
      state->h1.fetch_add(1);
    } else if (resp.http_version == 3) {
      state->h2.fetch_add(1);
    }
    if (resp.error.empty() && resp.status >= 200 && resp.status < 500) {
      state->ok.fetch_add(1);
    } else {
      if (state->fail.load() < 3 && !resp.error.empty()) {
        std::cerr << "error=" << resp.error << "\n";
      }
      state->fail.fetch_add(1);
    }
    state->completed.fetch_add(1);
  };

  auto handle_response_no_latency = [state](httpclient::Response resp) {
    if (resp.http_version == 1) {
      state->h1.fetch_add(1);
    } else if (resp.http_version == 3) {
      state->h2.fetch_add(1);
    }
    if (resp.error.empty() && resp.status >= 200 && resp.status < 500) {
      state->ok.fetch_add(1);
    } else {
      if (state->fail.load() < 3 && !resp.error.empty()) {
        std::cerr << "error=" << resp.error << "\n";
      }
      state->fail.fetch_add(1);
    }
    state->completed.fetch_add(1);
  };

  struct GatherResult {
    httpclient::Response response;
    std::uint64_t latency_us = 0;
  };

  std::shared_ptr<std::function<void()>> callback_issue_one;
  if (args.gather_mode) {
    co_await asyncx::for_each_limited(
        static_cast<std::size_t>(args.requests),
        static_cast<std::size_t>(args.concurrency),
        [&](std::size_t id) -> asio::awaitable<GatherResult> {
          if (args.smooth_start_ms > 0 && args.concurrency > 0) {
            auto delay_ms =
                (static_cast<int>(id % static_cast<std::size_t>(args.concurrency)) *
                 args.smooth_start_ms) /
                std::max(1, args.concurrency);
            if (delay_ms > 0) {
              asio::steady_timer timer(co_await asio::this_coro::executor);
              timer.expires_after(std::chrono::milliseconds(delay_ms));
              boost::system::error_code ec;
              co_await timer.async_wait(
                  asio::redirect_error(asio::use_awaitable, ec));
            }
          }
          auto request_start = std::chrono::steady_clock::now();
          auto resp =
              co_await client.async_request(make_indexed_request(static_cast<int>(id)));
          auto latency_us = static_cast<std::uint64_t>(
              std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::steady_clock::now() - request_start)
                  .count());
          co_return GatherResult{std::move(resp), latency_us};
        },
        [&](std::size_t id, GatherResult result) {
          latencies->record(static_cast<int>(id), result.latency_us);
          handle_response_no_latency(std::move(result.response));
        });
  } else if (!args.awaitable_mode) {
    callback_issue_one = std::make_shared<std::function<void()>>();
    std::weak_ptr<std::function<void()>> weak_issue_one = callback_issue_one;
    *callback_issue_one = [&, state, payload, weak_issue_one, handle_response]() {
      auto id = state->issued.fetch_add(1);
      if (id >= args.requests) {
        return;
      }
      auto req = make_indexed_request(id);
      auto request_start = std::chrono::steady_clock::now();
      client.async_request_callback(
          std::move(req),
          [id, request_start, weak_issue_one,
           handle_response](httpclient::Response resp) mutable {
            handle_response(id, std::move(resp), request_start);
            if (auto issue_one = weak_issue_one.lock()) {
              (*issue_one)();
            }
          });
    };

    auto starters = std::min(args.concurrency, args.requests);
    for (int i = 0; i < starters; ++i) {
      asio::co_spawn(
          ex,
          [smooth_start_delay, callback_issue_one, i, starters]()
              -> asio::awaitable<void> {
            co_await smooth_start_delay(i, starters);
            (*callback_issue_one)();
            co_return;
          },
          asio::detached);
    }
  } else {
    auto issue_loop = [&, state, payload, done_timer,
                       smooth_start_delay](int slot, int slots) -> asio::awaitable<void> {
      co_await smooth_start_delay(slot, slots);
      for (int id = state->issued.fetch_add(1); id < args.requests;
           id = state->issued.fetch_add(1)) {
        httpclient::Request req = make_request(
            (args.mixed && !args.url_alt.empty())
                ? (mixed_uses_alt(id, args.mixed_shuffle) ? args.url_alt : args.url)
                : args.url);
        auto request_start = std::chrono::steady_clock::now();
        auto resp = co_await client.async_request(std::move(req));
        latencies->record(
            id, static_cast<std::uint64_t>(
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::steady_clock::now() - request_start)
                        .count()));
        if (resp.http_version == 1) {
          state->h1.fetch_add(1);
        } else if (resp.http_version == 3) {
          state->h2.fetch_add(1);
        }
        if (resp.error.empty() && resp.status >= 200 && resp.status < 500) {
          state->ok.fetch_add(1);
        } else {
          if (state->fail.load() < 3 && !resp.error.empty()) {
            std::cerr << "error=" << resp.error << "\n";
          }
          state->fail.fetch_add(1);
        }
        if (state->completed.fetch_add(1) + 1 == args.requests) {
          done_timer->cancel();
        }
      }
    };

    auto starters = std::min(args.concurrency, args.requests);
    for (int i = 0; i < starters; ++i) {
      asio::co_spawn(ex, issue_loop(i, starters), asio::detached);
    }
  }

  if (state->completed.load() < args.requests) {
    boost::system::error_code ec;
    co_await done_timer->async_wait(asio::redirect_error(asio::use_awaitable, ec));
  }

  auto wall_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - start)
                     .count();
  auto cpu_after = read_process_cpu_stats();
  auto mem = read_process_memory_stats();
  auto latency = summarize_latencies(latencies->samples);
  const auto cpu_user_ms = (cpu_after.user_us - cpu_before.user_us) / 1000;
  const auto cpu_system_ms =
      (cpu_after.system_us - cpu_before.system_us) / 1000;
  std::cout << "requests=" << args.requests << "\n";
  std::cout << "ok=" << state->ok.load() << " fail=" << state->fail.load()
            << " h1=" << state->h1.load() << " h2=" << state->h2.load() << "\n";
  std::cout << "wall_ms=" << wall_ms << "\n";
  std::cout << "p50_us=" << latency.p50_us << " p95_us=" << latency.p95_us
            << " p99_us=" << latency.p99_us
            << " latency_samples=" << latency.samples
            << " cpu_user_ms=" << cpu_user_ms
            << " cpu_system_ms=" << cpu_system_ms << "\n";
  std::cout << "rss_kb=" << mem.rss_kb << " peak_rss_kb=" << mem.peak_rss_kb
            << "\n";
  auto stats = diff_stats(client.stats(), stats_before);
  std::cout << "probe_h1_adopted=" << stats.probe_h1_adopted
            << " probe_h2_marked=" << stats.probe_h2_marked
            << " probe_reconnect=" << stats.probe_reconnect
            << " overflow_fallback=" << stats.overflow_fallback
            << " overflow_fallback_on_h2_origin=" << stats.overflow_fallback_on_h2_origin
            << " url_route_cache_hits=" << stats.url_route_cache_hits
            << " url_route_cache_misses=" << stats.url_route_cache_misses
            << " h1_cached_routes=" << stats.h1_cached_routes
            << " h2_cached_routes=" << stats.h2_cached_routes
            << " detect_waiters=" << stats.detect_waiters
            << " detect_queue_overflow=" << stats.detect_queue_overflow
            << " detect_overflow_to_h1=" << stats.detect_overflow_to_h1
            << " detect_overflow_to_h1_later_h2=" << stats.detect_overflow_to_h1_later_h2
            << " retry_status=" << stats.retry_status
            << " retry_transport=" << stats.retry_transport
            << " h1_conn_created=" << stats.h1_pool.h1_conn_created
            << " h1_idle_hit=" << stats.h1_pool.h1_idle_hit
            << " h1_idle_miss=" << stats.h1_pool.h1_idle_miss
            << " h1_conn_reused=" << stats.h1_pool.h1_conn_reused
            << " h1_return_to_idle=" << stats.h1_pool.h1_return_to_idle
            << " h1_close_after_response=" << stats.h1_pool.h1_close_after_response
            << " h1_reuse_failed=" << stats.h1_pool.h1_reuse_failed
            << " h1_reconnect_after_idle=" << stats.h1_pool.h1_reconnect_after_idle
            << " h1_cancelled=" << stats.h1_pool.h1_cancelled
            << " h1_pool_wait_cancelled=" << stats.h1_pool.h1_pool_wait_cancelled
            << " h1_close_on_cancel=" << stats.h1_pool.h1_close_on_cancel
            << " h1_pool_wait_count=" << stats.h1_pool.h1_pool_wait.count
            << " h1_pool_wait_avg_us=" << avg_us(stats.h1_pool.h1_pool_wait)
            << " h1_pool_wait_max_seen_us=" << stats.h1_pool.h1_pool_wait.max_us
            << " h1_connect_count=" << stats.h1_pool.h1_connect.count
            << " h1_connect_avg_us=" << avg_us(stats.h1_pool.h1_connect)
            << " h1_connect_max_seen_us=" << stats.h1_pool.h1_connect.max_us
            << " h1_acquire_count=" << stats.h1_pool.h1_acquire.count
            << " h1_acquire_avg_us=" << avg_us(stats.h1_pool.h1_acquire)
            << " h1_acquire_max_seen_us=" << stats.h1_pool.h1_acquire.max_us
            << " h1_write_count=" << stats.h1_pool.h1_write.count
            << " h1_write_avg_us=" << avg_us(stats.h1_pool.h1_write)
            << " h1_write_max_seen_us=" << stats.h1_pool.h1_write.max_us
            << " h1_read_headers_count=" << stats.h1_pool.h1_read_headers.count
            << " h1_read_headers_avg_us=" << avg_us(stats.h1_pool.h1_read_headers)
            << " h1_read_headers_max_seen_us="
            << stats.h1_pool.h1_read_headers.max_us
            << " h1_read_body_count=" << stats.h1_pool.h1_read_body.count
            << " h1_read_body_avg_us=" << avg_us(stats.h1_pool.h1_read_body)
            << " h1_read_body_max_seen_us=" << stats.h1_pool.h1_read_body.max_us
            << " h1_exchange_count=" << stats.h1_pool.h1_exchange.count
            << " h1_exchange_avg_us=" << avg_us(stats.h1_pool.h1_exchange)
            << " h1_exchange_max_seen_us=" << stats.h1_pool.h1_exchange.max_us
            << " h2_streams_submitted=" << stats.h2_pool.streams_submitted
            << " h2_streams_completed=" << stats.h2_pool.streams_completed
            << " h2_streams_timed_out=" << stats.h2_pool.streams_timed_out
            << " h2_streams_cancelled=" << stats.h2_pool.streams_cancelled
            << " h2_stream_slot_waits=" << stats.h2_pool.stream_slot_waits
            << " h2_stream_slot_wait_cancelled="
            << stats.h2_pool.stream_slot_wait_cancelled
            << " h2_connect_waits=" << stats.h2_pool.connect_waits
            << " h2_connect_wait_cancelled="
            << stats.h2_pool.connect_wait_cancelled
            << " h2_preconnect_attempts=" << stats.h2_pool.preconnect_attempts
            << " h2_preconnect_success=" << stats.h2_pool.preconnect_success
            << " h2_preconnect_failed=" << stats.h2_pool.preconnect_failed
            << " h2_max_active_streams=" << stats.h2_pool.max_active_streams
            << " h2_max_pending_stream_waiters="
            << stats.h2_pool.max_pending_stream_waiters
            << " h2_peer_max_streams=" << stats.h2_pool.peer_max_concurrent_streams
            << " h2_configured_max_streams="
            << stats.h2_pool.configured_max_concurrent_streams
            << " h2_session_groups=" << stats.h2_pool.session_groups
            << " h2_session_groups_evicted="
            << stats.h2_pool.session_groups_evicted
            << " h2_session_group_cache_hits="
            << stats.h2_pool.session_group_cache_hits
            << " h2_session_group_cache_misses="
            << stats.h2_pool.session_group_cache_misses
            << "\n";
  client.shutdown();
}

}  // namespace

int main(int argc, char** argv) {
  asio::io_context io;
  auto args = parse_args(argc, argv);
  httpclient::HttpClient::Options options;
  options.runtime_profile = args.runtime_profile;
  options.h1.enable_ssl_verify = !args.insecure;
  options.h1.shard_count = static_cast<std::size_t>(std::max(0, args.h1_shards));
  if (args.h1_max_connections_per_origin > 0) {
    options.h1.max_connections_per_origin = static_cast<std::size_t>(
        std::max(1, args.h1_max_connections_per_origin));
  }
  if (args.h1_max_connecting_per_origin > 0) {
    options.h1.max_connecting_per_origin = static_cast<std::size_t>(
        std::max(1, args.h1_max_connecting_per_origin));
  }
  options.h1.max_origins_per_shard =
      static_cast<std::size_t>(std::max(0, args.h1_max_origins_per_shard));
  options.h1.origin_idle_ttl =
      std::chrono::seconds(std::max(0, args.h1_origin_idle_ttl_sec));
  options.h1.stripe_origins_across_shards = args.stripe_h1_origin_shards;
  options.h1.use_lightweight_h1 = !args.disable_lightweight_h1;
  options.h1.use_h1_connection_actor = args.h1_actor;
  options.h1.h1_actor_connections_per_origin =
      static_cast<std::size_t>(std::max(1, args.h1_actor_connections_per_origin));
  options.h2.verify_tls = !args.insecure;
  options.h2.shard_count =
      static_cast<std::size_t>(std::max(0, args.h2_shards));
  if (args.h2_sessions > 0) {
    options.h2.sessions_per_origin =
        static_cast<std::size_t>(std::max(1, args.h2_sessions));
  }
  options.h2.max_concurrent_streams =
      static_cast<std::size_t>(std::max(1, args.h2_max_streams));
  options.origin_waiter_limit =
      static_cast<std::size_t>(std::max(0, args.origin_waiters));
  options.max_cached_origins =
      static_cast<std::size_t>(std::max(0, args.max_cached_origins));
  options.origin_cache_ttl =
      std::chrono::seconds(std::max(0, args.origin_cache_ttl_sec));
  options.h2_failure_ttl =
      std::chrono::seconds(std::max(0, args.h2_failure_ttl_sec));
  options.detection_overflow_policy = args.strict_detect
                                          ? httpclient::HttpClient::DetectionOverflowPolicy::WaitForDetection
                                          : httpclient::HttpClient::DetectionOverflowPolicy::FallbackH1;
  httpclient::HttpClient client(io, options);
  asio::co_spawn(io, run(args, client), asio::detached);
  io.run();
  return 0;
}
