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
    std::uint64_t h1_conn_created = 0;
    std::uint64_t h1_idle_hit = 0;
    std::uint64_t h1_idle_miss = 0;
    std::uint64_t h1_conn_reused = 0;
    std::uint64_t h1_return_to_idle = 0;
    std::uint64_t h1_close_after_response = 0;
    std::uint64_t h1_reuse_failed = 0;
    std::uint64_t h1_reconnect_after_idle = 0;
  };

  struct Options {
    std::size_t shard_count = 0;
    std::size_t max_connections_per_origin = 64;
    std::size_t max_origins_per_shard = 4096;
    std::chrono::seconds origin_idle_ttl{300};
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
  Stats stats() const;
  boost::asio::awaitable<void> reset_connections();

private:
  struct Impl;
  std::shared_ptr<Impl> impl_;
};

}  // namespace httpclient
