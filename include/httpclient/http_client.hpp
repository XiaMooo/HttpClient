#pragma once

#include "httpclient/asio_http_client.hpp"
#include "httpclient/h2_client.hpp"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>

#include <chrono>
#include <functional>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace httpclient {

class HttpClient {
public:
  enum class DetectionOverflowPolicy {
    FallbackH1,
    WaitForDetection,
  };

  enum class RuntimeProfile {
    Auto,
    Balanced,
    Throughput,
  };

  struct Options {
    RuntimeProfile runtime_profile = RuntimeProfile::Auto;
    AsioHttpClient::Options h1;
    H2Client::Options h2;
    std::chrono::seconds h2_failure_ttl{30};
    std::size_t origin_waiter_limit = 32;
    std::size_t max_cached_origins = 4096;
    std::chrono::seconds origin_cache_ttl{300};
    bool follow_redirects = false;
    int max_redirects = 20;
    int max_retries = 0;
    std::chrono::milliseconds retry_backoff{0};
    std::vector<int> retry_statuses{429, 502, 503, 504};
    bool enable_cookie_jar = false;
    std::size_t max_cookie_domains = 1024;
    std::size_t max_cookies_per_domain = 64;
    bool auto_decompress = false;
    Request::Timeout timeout;
    std::optional<ProxyConfig> proxy;
    bool trust_env_proxy = false;
    std::vector<std::string> no_proxy;
    std::vector<std::string> default_headers;
    std::vector<QueryParam> default_query_params;
    std::string base_url;
    std::vector<std::function<void(Request&)>> request_hooks;
    std::vector<std::function<void(Response&)>> response_hooks;
    DetectionOverflowPolicy detection_overflow_policy =
        DetectionOverflowPolicy::FallbackH1;
  };

  struct Stats {
    std::uint64_t probe_h1_adopted = 0;
    std::uint64_t probe_h2_marked = 0;
    std::uint64_t probe_reconnect = 0;
    std::uint64_t overflow_fallback = 0;
    std::uint64_t overflow_fallback_on_h2_origin = 0;
    std::uint64_t url_route_cache_hits = 0;
    std::uint64_t url_route_cache_misses = 0;
    std::uint64_t h1_cached_routes = 0;
    std::uint64_t h2_cached_routes = 0;
    std::uint64_t detect_waiters = 0;
    std::uint64_t detect_queue_overflow = 0;
    std::uint64_t detect_overflow_to_h1 = 0;
    std::uint64_t detect_overflow_to_h1_later_h2 = 0;
    AsioHttpClient::Stats h1_pool;
    H2Client::Stats h2_pool;
  };

  HttpClient();
  explicit HttpClient(Options options);
  explicit HttpClient(boost::asio::io_context& io);
  HttpClient(boost::asio::io_context& io, Options options);
  ~HttpClient();

  HttpClient(const HttpClient&) = delete;
  HttpClient& operator=(const HttpClient&) = delete;

  using ResponseHandler = std::function<void(Response)>;

  boost::asio::awaitable<Response> async_request(Request request);
  boost::asio::awaitable<Response> async_get(std::string url);
  boost::asio::awaitable<Response> async_post(
      std::string url, std::string body,
      std::string content_type = "application/octet-stream");
  boost::asio::awaitable<Response> async_put(
      std::string url, std::string body,
      std::string content_type = "application/octet-stream");
  boost::asio::awaitable<Response> async_del(std::string url);
  void async_request_callback(Request request, ResponseHandler handler);
  std::future<Response> request_async(Request request);
  Response request(Request request);
  Response get(std::string url);
  Response post(std::string url, std::string body,
                std::string content_type = "application/octet-stream");
  Response put(std::string url, std::string body,
               std::string content_type = "application/octet-stream");
  Response del(std::string url);
  boost::asio::awaitable<void> preconnect(Request request, std::size_t count);
  Stats stats() const;
  boost::asio::awaitable<void> reset_connections();
  void shutdown();

private:
  struct Impl;
  std::shared_ptr<Impl> impl_;
};

}  // namespace httpclient
