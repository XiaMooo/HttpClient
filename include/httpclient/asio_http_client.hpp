#pragma once

#include "httpclient/request.hpp"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/thread_pool.hpp>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace httpclient {

class AsioHttpClient {
public:
  enum class ProbeProtocol {
    Http11,
    H2,
    Unknown,
  };

  struct ProbeResult {
    ProbeProtocol protocol = ProbeProtocol::Unknown;
    Response response;
  };

  struct Stats {
    struct TimingStats {
      std::uint64_t count = 0;
      std::uint64_t total_us = 0;
      std::uint64_t max_us = 0;
    };

    std::uint64_t h1_conn_created = 0;
    std::uint64_t h1_idle_hit = 0;
    std::uint64_t h1_idle_miss = 0;
    std::uint64_t h1_conn_reused = 0;
    std::uint64_t h1_return_to_idle = 0;
    std::uint64_t h1_close_after_response = 0;
    std::uint64_t h1_reuse_failed = 0;
    std::uint64_t h1_reconnect_after_idle = 0;
    std::uint64_t h1_cancelled = 0;
    std::uint64_t h1_pool_wait_cancelled = 0;
    std::uint64_t h1_close_on_cancel = 0;
    TimingStats h1_pool_wait;
    TimingStats h1_connect;
    TimingStats h1_acquire;
    TimingStats h1_write;
    TimingStats h1_read_headers;
    TimingStats h1_read_body;
    TimingStats h1_exchange;
  };

  struct Options {
    std::size_t shard_count = 0;
    bool auto_shards = false;
    std::size_t max_connections_per_origin = 128;
    std::size_t max_origins_per_shard = 4096;
    std::chrono::seconds origin_idle_ttl{300};
    std::chrono::seconds maintenance_interval{10};
    std::chrono::seconds auto_scale_down_idle_ttl{60};
    std::chrono::milliseconds auto_scale_up_interval{25};
    bool enable_ssl_verify = true;
    bool stripe_origins_across_shards = false;
    bool use_h1_connection_actor = false;
    bool use_lightweight_h1 = true;
    std::size_t h1_actor_connections_per_origin = 8;
  };

  AsioHttpClient();
  explicit AsioHttpClient(Options options);
  ~AsioHttpClient();

  AsioHttpClient(const AsioHttpClient&) = delete;
  AsioHttpClient& operator=(const AsioHttpClient&) = delete;
  AsioHttpClient(AsioHttpClient&&) = delete;
  AsioHttpClient& operator=(AsioHttpClient&&) = delete;

  using ResponseHandler = std::function<void(Response)>;

  boost::asio::awaitable<Response> async_request(Request request);
  void async_request_callback(Request request, ResponseHandler handler);
  boost::asio::awaitable<ProbeResult> async_probe(Request request);
  boost::asio::awaitable<void> preconnect(Request request, std::size_t count);
  Stats stats() const;
  void reset_stats();
  boost::asio::awaitable<void> reset_connections();

private:
  struct Impl;
  std::shared_ptr<Impl> impl_;
};

}  // namespace httpclient
