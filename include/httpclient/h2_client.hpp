#pragma once

#include "httpclient/request.hpp"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <functional>
#include <list>
#include <mutex>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace httpclient {

class H2Client {
public:
  struct Options {
    bool verify_tls = true;
    std::size_t shard_count = 0;
    bool auto_shards = false;
    std::size_t sessions_per_origin = 1;
    std::size_t max_concurrent_streams = 128;
    std::size_t max_session_groups = 4096;
    std::chrono::seconds session_group_idle_ttl{300};
    std::chrono::seconds maintenance_interval{10};
    std::chrono::seconds auto_scale_down_idle_ttl{60};
    std::chrono::milliseconds auto_scale_up_interval{25};
    bool auto_prewarm_sessions = true;
  };

  struct Stats {
    std::uint64_t streams_submitted = 0;
    std::uint64_t streams_completed = 0;
    std::uint64_t streams_timed_out = 0;
    std::uint64_t streams_cancelled = 0;
    std::uint64_t stream_slot_waits = 0;
    std::uint64_t stream_slot_wait_cancelled = 0;
    std::uint64_t connect_waits = 0;
    std::uint64_t connect_wait_cancelled = 0;
    std::uint64_t preconnect_attempts = 0;
    std::uint64_t preconnect_success = 0;
    std::uint64_t preconnect_failed = 0;
    std::uint64_t max_active_streams = 0;
    std::uint64_t max_pending_stream_waiters = 0;
    std::uint64_t peer_max_concurrent_streams = 0;
    std::uint64_t configured_max_concurrent_streams = 0;
    std::uint64_t session_groups = 0;
    std::uint64_t session_groups_evicted = 0;
    std::uint64_t session_group_cache_hits = 0;
    std::uint64_t session_group_cache_misses = 0;
  };

  explicit H2Client(boost::asio::io_context& io);
  H2Client(boost::asio::io_context& io, Options options);
  ~H2Client();

  H2Client(const H2Client&) = delete;
  H2Client& operator=(const H2Client&) = delete;

  boost::asio::awaitable<Response> get(std::string url, bool insecure = false);
  boost::asio::awaitable<Response> async_request(Request request,
                                                 bool insecure = false);
  boost::asio::awaitable<void> preconnect(Request request, std::size_t count,
                                          bool insecure = false);
  using ResponseHandler = std::function<void(Response)>;
  void async_request_callback(Request request, ResponseHandler handler,
                              bool insecure = false);
  Stats stats() const;
  void reset_stats();
  boost::asio::awaitable<void> reset_connections();
  void shutdown();

private:
  struct Impl;
  struct IoShard;
  struct SessionGroup;
  std::shared_ptr<SessionGroup> group_for(const Request& request);
  bool session_group_idle(const SessionGroup& group) const;
  void evict_session_groups_locked();
  boost::asio::io_context& io_for_session(std::size_t index);
  struct ActiveImplDecision;
  ActiveImplDecision active_impl_count(SessionGroup& group,
                                       std::size_t inflight) const;
  std::size_t choose_impl_index(SessionGroup& group, std::size_t active) const;
  void prewarm_active_impls(const std::shared_ptr<SessionGroup>& group,
                            const Request& request, bool insecure,
                            std::size_t previous_active,
                            std::size_t active) const;
  void schedule_maintenance();
  void run_maintenance();
  void stop_owned_shards();

  boost::asio::io_context& io_;
  Options options_;
  std::vector<std::unique_ptr<IoShard>> shards_;
  mutable std::mutex groups_mu_;
  std::unordered_map<std::string, std::shared_ptr<SessionGroup>> groups_;
  std::list<std::string> group_lru_;
  std::atomic<std::uint64_t> session_groups_evicted_{0};
  std::atomic<std::uint64_t> session_group_cache_hits_{0};
  std::atomic<std::uint64_t> session_group_cache_misses_{0};
  std::unique_ptr<boost::asio::steady_timer> maintenance_timer_;
  std::shared_ptr<int> lifetime_token_{std::make_shared<int>(0)};
};

}  // namespace httpclient
