#pragma once

#include "httpclient/request.hpp"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>

#include <atomic>
#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace httpclient {

class H2Client {
public:
  struct Options {
    bool verify_tls = true;
    std::size_t sessions_per_origin = 1;
    std::size_t max_concurrent_streams = 128;
  };

  struct Stats {
    std::uint64_t streams_submitted = 0;
    std::uint64_t streams_completed = 0;
    std::uint64_t streams_timed_out = 0;
    std::uint64_t stream_slot_waits = 0;
    std::uint64_t max_active_streams = 0;
    std::uint64_t max_pending_stream_waiters = 0;
    std::uint64_t peer_max_concurrent_streams = 0;
    std::uint64_t configured_max_concurrent_streams = 0;
  };

  explicit H2Client(boost::asio::io_context& io);
  H2Client(boost::asio::io_context& io, Options options);
  ~H2Client();

  H2Client(const H2Client&) = delete;
  H2Client& operator=(const H2Client&) = delete;

  boost::asio::awaitable<Response> get(std::string url, bool insecure = false);
  boost::asio::awaitable<Response> async_request(Request request,
                                                 bool insecure = false);
  using ResponseHandler = std::function<void(Response)>;
  void async_request_callback(Request request, ResponseHandler handler,
                              bool insecure = false);
  Stats stats() const;
  boost::asio::awaitable<void> reset_connections();
  void shutdown();

private:
  struct Impl;
  std::vector<std::shared_ptr<Impl>> impls_;
  std::atomic<std::size_t> next_impl_{0};
};

}  // namespace httpclient
