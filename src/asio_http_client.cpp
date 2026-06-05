#include "httpclient/asio_http_client.hpp"
#include "asyncx/asyncx.hpp"
#include "proxy_support.hpp"

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/system/error_code.hpp>
#include <boost/system/system_error.hpp>
#include <openssl/ssl.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cctype>
#include <cstring>
#include <deque>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <sstream>
#include <string_view>
#include <unordered_map>
#include <utility>

namespace httpclient {
namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using tcp = asio::ip::tcp;

namespace {

std::runtime_error stage_error(std::string_view stage, const std::exception& e) {
  std::ostringstream oss;
  oss << stage << ": " << e.what();
  return std::runtime_error(oss.str());
}

struct ParsedUrl {
  std::string scheme;
  std::string host;
  std::string port;
  std::string target;
  bool tls = false;
};

using EffectiveProxy = proxy_support::EffectiveProxy;

ParsedUrl proxy_url_as_parsed(const proxy_support::Url& url) {
  ParsedUrl out;
  out.scheme = url.scheme;
  out.host = url.host;
  out.port = url.port;
  out.target = "/";
  out.tls = url.scheme == "https";
  return out;
}

ParsedUrl parse_url(const std::string& url) {
  auto scheme_end = url.find("://");
  if (scheme_end == std::string::npos) {
    throw std::invalid_argument("url must include scheme");
  }

  ParsedUrl out;
  out.scheme = url.substr(0, scheme_end);
  out.tls = out.scheme == "https";
  if (!out.tls && out.scheme != "http") {
    throw std::invalid_argument("only http and https are supported");
  }

  auto rest_start = scheme_end + 3;
  auto path_start = url.find('/', rest_start);
  std::string authority =
      path_start == std::string::npos ? url.substr(rest_start)
                                      : url.substr(rest_start, path_start - rest_start);
  out.target = path_start == std::string::npos ? "/" : url.substr(path_start);

  auto colon = authority.rfind(':');
  if (colon != std::string::npos) {
    out.host = authority.substr(0, colon);
    out.port = authority.substr(colon + 1);
  } else {
    out.host = authority;
    out.port = out.tls ? "443" : "80";
  }

  if (out.host.empty()) {
    throw std::invalid_argument("url host is empty");
  }
  return out;
}

std::string absolute_uri(const ParsedUrl& url) {
  return url.scheme + "://" + url.host +
         ((url.tls && url.port == "443") || (!url.tls && url.port == "80")
              ? ""
              : ":" + url.port) +
         url.target;
}

http::verb parse_method(const std::string& method) {
  std::string upper = method;
  std::transform(upper.begin(), upper.end(), upper.begin(),
                 [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
  if (upper == "GET") return http::verb::get;
  if (upper == "POST") return http::verb::post;
  if (upper == "PUT") return http::verb::put;
  if (upper == "PATCH") return http::verb::patch;
  if (upper == "DELETE") return http::verb::delete_;
  if (upper == "HEAD") return http::verb::head;
  return http::verb::unknown;
}

std::pair<std::string, std::string> split_header(const std::string& header) {
  auto pos = header.find(':');
  if (pos == std::string::npos) {
    return {header, ""};
  }
  auto name = header.substr(0, pos);
  auto value = header.substr(pos + 1);
  while (!value.empty() && std::isspace(static_cast<unsigned char>(value.front()))) {
    value.erase(value.begin());
  }
  return {name, value};
}

bool header_name_equals(std::string_view a, std::string_view b) {
  if (a.size() != b.size()) {
    return false;
  }
  for (std::size_t i = 0; i < a.size(); ++i) {
    if (std::tolower(static_cast<unsigned char>(a[i])) !=
        std::tolower(static_cast<unsigned char>(b[i]))) {
      return false;
    }
  }
  return true;
}

long parse_status_code(std::string_view headers) {
  auto first_space = headers.find(' ');
  if (first_space == std::string_view::npos || first_space + 4 > headers.size()) {
    return 0;
  }
  long status = 0;
  for (std::size_t i = first_space + 1; i < first_space + 4; ++i) {
    if (headers[i] < '0' || headers[i] > '9') {
      return 0;
    }
    status = status * 10 + (headers[i] - '0');
  }
  return status;
}

std::size_t parse_content_length(std::string_view headers) {
  std::size_t line = 0;
  constexpr std::string_view name = "content-length:";
  while ((line = headers.find("\r\n", line)) != std::string_view::npos) {
    line += 2;
    if (line + name.size() > headers.size()) {
      break;
    }
    bool match = true;
    for (std::size_t i = 0; i < name.size(); ++i) {
      if (std::tolower(static_cast<unsigned char>(headers[line + i])) != name[i]) {
        match = false;
        break;
      }
    }
    if (!match) {
      continue;
    }
    auto pos = line + name.size();
    while (pos < headers.size() &&
           std::isspace(static_cast<unsigned char>(headers[pos]))) {
      ++pos;
    }
    std::size_t len = 0;
    while (pos < headers.size() && headers[pos] >= '0' && headers[pos] <= '9') {
      len = len * 10 + static_cast<std::size_t>(headers[pos] - '0');
      ++pos;
    }
    return len;
  }
  return 0;
}

void append_response_headers(Response& response, std::string_view headers) {
  auto line_start = headers.find("\r\n");
  if (line_start == std::string_view::npos) {
    return;
  }
  line_start += 2;
  while (line_start < headers.size()) {
    auto line_end = headers.find("\r\n", line_start);
    if (line_end == std::string_view::npos || line_end == line_start) {
      break;
    }
    response.headers.emplace_back(headers.substr(line_start, line_end - line_start));
    line_start = line_end + 2;
  }
}

AsioHttpClient::ProbeProtocol selected_alpn(SSL* ssl) {
  const unsigned char* selected = nullptr;
  unsigned int selected_len = 0;
  SSL_get0_alpn_selected(ssl, &selected, &selected_len);
  if (selected_len == 2 && std::memcmp(selected, "h2", 2) == 0) {
    return AsioHttpClient::ProbeProtocol::H2;
  }
  if (selected_len == 8 && std::memcmp(selected, "http/1.1", 8) == 0) {
    return AsioHttpClient::ProbeProtocol::Http11;
  }
  return AsioHttpClient::ProbeProtocol::Unknown;
}

void set_h2_h1_alpn(SSL* ssl) {
  static const unsigned char alpn[] = {
      2, 'h', '2',
      8, 'h', 't', 't', 'p', '/', '1', '.', '1',
  };
  SSL_set_alpn_protos(ssl, alpn, sizeof(alpn));
}

std::chrono::milliseconds effective_timeout(const Request& request,
                                            long Request::Timeout::*field) {
  auto value = request.timeout.*field;
  if (value < 0) {
    value = request.timeout.total_ms >= 0 ? request.timeout.total_ms
                                          : request.timeout_ms;
  }
  if (value < 0) {
    value = request.timeout_ms;
  }
  return std::chrono::milliseconds(value);
}

struct H1ExchangeTimings {
  std::atomic<std::uint64_t>* write_count = nullptr;
  std::atomic<std::uint64_t>* write_total_us = nullptr;
  std::atomic<std::uint64_t>* write_max_us = nullptr;
  std::atomic<std::uint64_t>* read_headers_count = nullptr;
  std::atomic<std::uint64_t>* read_headers_total_us = nullptr;
  std::atomic<std::uint64_t>* read_headers_max_us = nullptr;
  std::atomic<std::uint64_t>* read_body_count = nullptr;
  std::atomic<std::uint64_t>* read_body_total_us = nullptr;
  std::atomic<std::uint64_t>* read_body_max_us = nullptr;
  std::atomic<std::uint64_t>* exchange_count = nullptr;
  std::atomic<std::uint64_t>* exchange_total_us = nullptr;
  std::atomic<std::uint64_t>* exchange_max_us = nullptr;
};

void record_atomic_timing(std::atomic<std::uint64_t>* count,
                          std::atomic<std::uint64_t>* total,
                          std::atomic<std::uint64_t>* max,
                          std::uint64_t us) {
  if (!count || !total || !max) {
    return;
  }
  count->fetch_add(1, std::memory_order_relaxed);
  total->fetch_add(us, std::memory_order_relaxed);
  auto current = max->load(std::memory_order_relaxed);
  while (current < us &&
         !max->compare_exchange_weak(current, us, std::memory_order_relaxed,
                                     std::memory_order_relaxed)) {
  }
}

std::uint64_t elapsed_us_since(std::chrono::steady_clock::time_point start) {
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now() - start)
          .count());
}

template <class Stream>
asio::awaitable<Response> run_light_h1_exchange(Stream& stream, std::string& read_buf,
                                                std::string& write_buf,
                                                const ParsedUrl& url, Request request,
                                                std::chrono::steady_clock::time_point start,
                                                std::chrono::milliseconds write_timeout,
                                                std::chrono::milliseconds read_timeout,
                                                bool absolute_target = false,
                                                const H1ExchangeTimings* timings = nullptr) {
  write_buf.reserve(256 + request.body.size());
  write_buf.clear();
  write_buf.append(request.method.empty() ? "GET" : request.method);
  write_buf.push_back(' ');
  write_buf.append(absolute_target ? absolute_uri(url) : url.target);
  write_buf.append(" HTTP/1.1\r\nHost: ");
  write_buf.append(url.host);
  write_buf.append("\r\n");
  bool has_user_agent = false;
  bool has_accept = false;
  for (const auto& header : request.headers) {
    auto [name, _] = split_header(header);
    has_user_agent = has_user_agent || header_name_equals(name, "user-agent");
    has_accept = has_accept || header_name_equals(name, "accept");
  }
  if (!has_user_agent) {
    write_buf.append("User-Agent: httpclient-asio-light\r\n");
  }
  if (!has_accept) {
    write_buf.append("Accept: */*\r\n");
  }
  for (const auto& header : request.headers) {
    auto [name, value] = split_header(header);
    if (name.empty() || header_name_equals(name, "host") ||
        header_name_equals(name, "content-length") ||
        header_name_equals(name, "connection")) {
      continue;
    }
    write_buf.append(name);
    write_buf.append(": ");
    write_buf.append(value);
    write_buf.append("\r\n");
  }
  if (!request.body.empty()) {
    write_buf.append("Content-Length: ");
    write_buf.append(std::to_string(request.body.size()));
    write_buf.append("\r\n");
  }
  write_buf.append("Connection: keep-alive\r\n\r\n");
  write_buf.append(request.body);

  auto exchange_started = std::chrono::steady_clock::now();
  auto write_started = exchange_started;
  try {
    if constexpr (requires { beast::get_lowest_layer(stream).expires_after(write_timeout); }) {
      beast::get_lowest_layer(stream).expires_after(write_timeout);
    } else {
      stream.expires_after(write_timeout);
    }
    co_await asio::async_write(stream, asio::buffer(write_buf), asio::use_awaitable);
  } catch (const std::exception& e) {
    throw stage_error("h1_write", e);
  }
  if (timings) {
    record_atomic_timing(timings->write_count, timings->write_total_us,
                         timings->write_max_us, elapsed_us_since(write_started));
  }

  read_buf.clear();
  std::array<char, 32768> tmp{};
  std::size_t header_end = std::string::npos;
  auto read_headers_started = std::chrono::steady_clock::now();
  for (;;) {
    std::size_t n = 0;
    try {
      if constexpr (requires { beast::get_lowest_layer(stream).expires_after(read_timeout); }) {
        beast::get_lowest_layer(stream).expires_after(read_timeout);
      } else {
        stream.expires_after(read_timeout);
      }
      n = co_await stream.async_read_some(asio::buffer(tmp), asio::use_awaitable);
    } catch (const std::exception& e) {
      throw stage_error("h1_read_headers", e);
    }
    read_buf.append(tmp.data(), n);
    header_end = read_buf.find("\r\n\r\n");
    if (header_end != std::string::npos) {
      break;
    }
    if (read_buf.size() > 65536) {
      throw std::runtime_error("h1 response headers too large");
    }
  }
  if (timings) {
    record_atomic_timing(timings->read_headers_count,
                         timings->read_headers_total_us,
                         timings->read_headers_max_us,
                         elapsed_us_since(read_headers_started));
  }

  auto headers = std::string_view(read_buf.data(), header_end + 4);
  auto content_length = parse_content_length(headers);
  auto body_start = header_end + 4;
  auto have_body = read_buf.size() - body_start;
  if (request.on_body_chunk && have_body > 0) {
    auto chunk = std::string_view(read_buf.data() + body_start,
                                  std::min(have_body, content_length));
    request.on_body_chunk(chunk);
  }
  auto read_body_started = std::chrono::steady_clock::now();
  while (have_body < content_length) {
    auto remaining = content_length - have_body;
    std::size_t n = 0;
    try {
      if constexpr (requires { beast::get_lowest_layer(stream).expires_after(read_timeout); }) {
        beast::get_lowest_layer(stream).expires_after(read_timeout);
      } else {
        stream.expires_after(read_timeout);
      }
      n = co_await stream.async_read_some(
          asio::buffer(tmp.data(), std::min(tmp.size(), remaining)), asio::use_awaitable);
    } catch (const std::exception& e) {
      throw stage_error("h1_read_body", e);
    }
    if (request.store_response_body) {
      read_buf.append(tmp.data(), n);
    }
    if (request.on_body_chunk && n > 0) {
      request.on_body_chunk(std::string_view(tmp.data(), n));
    }
    have_body += n;
  }
  if (timings) {
    record_atomic_timing(timings->read_body_count,
                         timings->read_body_total_us,
                         timings->read_body_max_us,
                         elapsed_us_since(read_body_started));
  }

  Response response;
  response.status = parse_status_code(headers);
  response.http_version = 1;
  if (request.store_response_headers) {
    append_response_headers(response, headers);
  }
  if (request.store_response_body && content_length > 0) {
    response.body.assign(read_buf.data() + body_start, content_length);
  }
  if (request.measure_total_time) {
    response.total_time_sec =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
  }
  if (timings) {
    record_atomic_timing(timings->exchange_count, timings->exchange_total_us,
                         timings->exchange_max_us,
                         elapsed_us_since(exchange_started));
  }
  co_return response;
}

void add_proxy_authorization(Request& request,
                             const proxy_support::EffectiveProxy& proxy) {
  if (!proxy.authorization.empty() &&
      !request.header("Proxy-Authorization").has_value()) {
    request.set_header("Proxy-Authorization", proxy.authorization);
  }
}

template <class Stream>
asio::awaitable<Response> run_http_exchange(Stream& stream, beast::flat_buffer& buffer,
                                            const ParsedUrl& url,
                                            Request request,
                                            std::chrono::steady_clock::time_point start,
                                            std::chrono::milliseconds write_timeout,
                                            std::chrono::milliseconds read_timeout,
                                            bool absolute_target = false,
                                            const H1ExchangeTimings* timings = nullptr) {
  auto exchange_started = std::chrono::steady_clock::now();
  buffer.consume(buffer.size());
  http::request<http::string_body> req{
      parse_method(request.method), absolute_target ? absolute_uri(url) : url.target,
      11};
  req.set(http::field::host, url.host);
  req.set(http::field::user_agent, "httpclient-asio");
  for (const auto& header : request.headers) {
    auto [name, value] = split_header(header);
    if (!name.empty()) {
      req.set(name, value);
    }
  }
  req.body() = std::move(request.body);
  req.prepare_payload();

  auto write_started = std::chrono::steady_clock::now();
  if constexpr (requires { beast::get_lowest_layer(stream).expires_after(write_timeout); }) {
    beast::get_lowest_layer(stream).expires_after(write_timeout);
  } else {
    stream.expires_after(write_timeout);
  }
  co_await http::async_write(stream, req, asio::use_awaitable);
  if (timings) {
    record_atomic_timing(timings->write_count, timings->write_total_us,
                         timings->write_max_us, elapsed_us_since(write_started));
  }

  http::response<http::string_body> res;
  auto read_started = std::chrono::steady_clock::now();
  if constexpr (requires { beast::get_lowest_layer(stream).expires_after(read_timeout); }) {
    beast::get_lowest_layer(stream).expires_after(read_timeout);
  } else {
    stream.expires_after(read_timeout);
  }
  co_await http::async_read(stream, buffer, res, asio::use_awaitable);
  if (timings) {
    record_atomic_timing(timings->read_headers_count,
                         timings->read_headers_total_us,
                         timings->read_headers_max_us,
                         elapsed_us_since(read_started));
  }
  Response response;
  response.status = static_cast<long>(res.result_int());
  if (request.store_response_body) {
    response.body = std::move(res.body());
  }
  response.total_time_sec =
      std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
  response.http_version = res.version() == 11 ? 1 : 0;
  if (request.store_response_headers) {
    for (auto const& field : res.base()) {
      response.headers.emplace_back(std::string(field.name_string()) + ": " +
                                    std::string(field.value()));
    }
  }
  if (request.measure_total_time) {
    response.total_time_sec =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
  }
  if (timings) {
    record_atomic_timing(timings->exchange_count, timings->exchange_total_us,
                         timings->exchange_max_us,
                         elapsed_us_since(exchange_started));
  }
  co_return response;
}

}  // namespace

struct AsioHttpClient::Impl : std::enable_shared_from_this<Impl> {
  struct OriginPool;
  struct H1TlsActor;

  struct AtomicTimingStats {
    std::atomic<std::uint64_t> count{0};
    std::atomic<std::uint64_t> total_us{0};
    std::atomic<std::uint64_t> max_us{0};
  };

  struct Shard {
    asio::io_context io;
    asio::executor_work_guard<asio::io_context::executor_type> work;
    std::jthread thread;
    asio::steady_timer maintenance_timer;
    std::unordered_map<std::string, std::shared_ptr<OriginPool>> origins;
    std::list<std::string> origin_lru;

    Shard() : io(1), work(asio::make_work_guard(io)), maintenance_timer(io) {}
  };

  struct PlainConnection {
    explicit PlainConnection(asio::any_io_executor executor) : stream(executor) {
      read_buffer.reserve(8192);
      write_buffer.reserve(8192);
    }
    beast::tcp_stream stream;
    beast::flat_buffer buffer;
    std::string read_buffer;
    std::string write_buffer;
  };

  struct TlsConnection {
    TlsConnection(asio::any_io_executor executor, asio::ssl::context& ssl_ctx)
        : stream(executor, ssl_ctx) {
      read_buffer.reserve(8192);
      write_buffer.reserve(8192);
    }

    beast::ssl_stream<beast::tcp_stream> stream;
    beast::flat_buffer buffer;
    std::string read_buffer;
    std::string write_buffer;
  };

  struct NestedTlsConnection {
    NestedTlsConnection(asio::any_io_executor executor,
                        asio::ssl::context& outer_ctx,
                        asio::ssl::context& inner_ctx)
        : stream(beast::ssl_stream<beast::tcp_stream>(executor, outer_ctx),
                 inner_ctx) {
      read_buffer.reserve(8192);
      write_buffer.reserve(8192);
    }

    beast::ssl_stream<beast::ssl_stream<beast::tcp_stream>> stream;
    beast::flat_buffer buffer;
    std::string read_buffer;
    std::string write_buffer;
  };

  struct Waiter {
    explicit Waiter(asio::any_io_executor executor) : timer(std::move(executor)) {}
    asio::steady_timer timer;
    bool woken = false;
    bool cancelled = false;
    bool reserved_idle = false;
  };

  using WaiterPtr = std::shared_ptr<Waiter>;
  using WaiterQueue = std::deque<WaiterPtr>;

  struct OriginPool {
    OriginPool(asio::any_io_executor executor, bool verify_tls)
        : executor(executor),
          strand(executor),
          ssl_ctx(asio::ssl::context::tls_client) {
      if (verify_tls) {
        ssl_ctx.set_default_verify_paths();
      } else {
        ssl_ctx.set_verify_mode(asio::ssl::verify_none);
      }
    }

    asio::any_io_executor executor;
    asio::strand<asio::any_io_executor> strand;
    asio::ssl::context ssl_ctx;
    std::vector<std::unique_ptr<PlainConnection>> idle_plain;
    std::vector<std::unique_ptr<TlsConnection>> idle_tls;
    WaiterQueue wait_plain;
    WaiterQueue wait_tls;
    std::vector<std::shared_ptr<H1TlsActor>> h1_tls_actors;
    std::list<std::string>::iterator lru_it;
    std::chrono::steady_clock::time_point last_used{};
    std::size_t next_h1_tls_actor = 0;
    std::size_t active_plain = 0;
    std::size_t active_tls = 0;
    std::size_t connecting_plain = 0;
    std::size_t connecting_tls = 0;
    // Returned idle connections are reserved for the waiter we wake. This keeps
    // hot H1 pools FIFO-fair and prevents new arrivals from repeatedly stealing
    // the just-returned connection before the waiter resumes on the shard.
    std::size_t reserved_idle_plain = 0;
    std::size_t reserved_idle_tls = 0;
    bool lru_linked = false;
  };

  struct Counters {
    std::atomic<std::uint64_t> h1_conn_created{0};
    std::atomic<std::uint64_t> h1_idle_hit{0};
    std::atomic<std::uint64_t> h1_idle_miss{0};
    std::atomic<std::uint64_t> h1_conn_reused{0};
    std::atomic<std::uint64_t> h1_return_to_idle{0};
    std::atomic<std::uint64_t> h1_close_after_response{0};
    std::atomic<std::uint64_t> h1_reuse_failed{0};
    std::atomic<std::uint64_t> h1_reconnect_after_idle{0};
    std::atomic<std::uint64_t> h1_cancelled{0};
    std::atomic<std::uint64_t> h1_pool_wait_cancelled{0};
    std::atomic<std::uint64_t> h1_close_on_cancel{0};
    AtomicTimingStats h1_pool_wait;
    AtomicTimingStats h1_connect;
    AtomicTimingStats h1_acquire;
    AtomicTimingStats h1_write;
    AtomicTimingStats h1_read_headers;
    AtomicTimingStats h1_read_body;
    AtomicTimingStats h1_exchange;
  };

  static std::uint64_t elapsed_us(std::chrono::steady_clock::time_point start) {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start)
            .count());
  }

  static void record_timing(AtomicTimingStats& stats, std::uint64_t us) {
    stats.count.fetch_add(1, std::memory_order_relaxed);
    stats.total_us.fetch_add(us, std::memory_order_relaxed);
    auto current = stats.max_us.load(std::memory_order_relaxed);
    while (current < us &&
           !stats.max_us.compare_exchange_weak(current, us, std::memory_order_relaxed,
                                               std::memory_order_relaxed)) {
    }
  }

  static AsioHttpClient::Stats::TimingStats snapshot_timing(
      const AtomicTimingStats& stats) {
    return AsioHttpClient::Stats::TimingStats{
        stats.count.load(std::memory_order_relaxed),
        stats.total_us.load(std::memory_order_relaxed),
        stats.max_us.load(std::memory_order_relaxed),
    };
  }

  static void reset_timing(AtomicTimingStats& stats) {
    stats.count.store(0, std::memory_order_relaxed);
    stats.total_us.store(0, std::memory_order_relaxed);
    stats.max_us.store(0, std::memory_order_relaxed);
  }

  void reset_stats() {
    stats_.h1_conn_created.store(0, std::memory_order_relaxed);
    stats_.h1_idle_hit.store(0, std::memory_order_relaxed);
    stats_.h1_idle_miss.store(0, std::memory_order_relaxed);
    stats_.h1_conn_reused.store(0, std::memory_order_relaxed);
    stats_.h1_return_to_idle.store(0, std::memory_order_relaxed);
    stats_.h1_close_after_response.store(0, std::memory_order_relaxed);
    stats_.h1_reuse_failed.store(0, std::memory_order_relaxed);
    stats_.h1_reconnect_after_idle.store(0, std::memory_order_relaxed);
    stats_.h1_cancelled.store(0, std::memory_order_relaxed);
    stats_.h1_pool_wait_cancelled.store(0, std::memory_order_relaxed);
    stats_.h1_close_on_cancel.store(0, std::memory_order_relaxed);
    reset_timing(stats_.h1_pool_wait);
    reset_timing(stats_.h1_connect);
    reset_timing(stats_.h1_acquire);
    reset_timing(stats_.h1_write);
    reset_timing(stats_.h1_read_headers);
    reset_timing(stats_.h1_read_body);
    reset_timing(stats_.h1_exchange);
  }

  H1ExchangeTimings h1_exchange_timings() {
    return H1ExchangeTimings{
        &stats_.h1_write.count,
        &stats_.h1_write.total_us,
        &stats_.h1_write.max_us,
        &stats_.h1_read_headers.count,
        &stats_.h1_read_headers.total_us,
        &stats_.h1_read_headers.max_us,
        &stats_.h1_read_body.count,
        &stats_.h1_read_body.total_us,
        &stats_.h1_read_body.max_us,
        &stats_.h1_exchange.count,
        &stats_.h1_exchange.total_us,
        &stats_.h1_exchange.max_us,
    };
  }

  static void remove_waiter(WaiterQueue& queue, const WaiterPtr& waiter) {
    queue.erase(std::remove(queue.begin(), queue.end(), waiter), queue.end());
  }

  static void wake_waiter(const WaiterPtr& waiter) {
    if (!waiter || waiter->cancelled) {
      return;
    }
    waiter->woken = true;
    waiter->timer.cancel();
  }

  static void close_plain(PlainConnection& conn) {
    boost::system::error_code ec;
    conn.stream.socket().shutdown(tcp::socket::shutdown_both, ec);
    conn.stream.socket().close(ec);
  }

  static void close_tls(TlsConnection& conn) {
    boost::system::error_code ec;
    beast::get_lowest_layer(conn.stream).socket().shutdown(tcp::socket::shutdown_both, ec);
    beast::get_lowest_layer(conn.stream).socket().close(ec);
  }

  static bool is_operation_aborted_exception(const std::exception& e) {
    if (auto* system_error = dynamic_cast<const boost::system::system_error*>(&e)) {
      return system_error->code() == asio::error::operation_aborted;
    }
    const std::string_view message(e.what());
    return message == "Operation canceled" || message == "operation_aborted";
  }

  static bool wake_one(WaiterQueue& waiters) {
    while (!waiters.empty()) {
      auto waiter = std::move(waiters.front());
      waiters.pop_front();
      if (waiter && !waiter->cancelled) {
        wake_waiter(waiter);
        return true;
      }
    }
    return false;
  }

  static bool wake_one_with_idle_reservation(WaiterQueue& waiters) {
    while (!waiters.empty()) {
      auto waiter = std::move(waiters.front());
      waiters.pop_front();
      if (waiter && !waiter->cancelled) {
        waiter->reserved_idle = true;
        wake_waiter(waiter);
        return true;
      }
    }
    return false;
  }

  static bool pool_idle(const std::shared_ptr<OriginPool>& pool) {
    return pool && pool->active_plain == pool->idle_plain.size() &&
           pool->active_tls == pool->idle_tls.size() && pool->wait_plain.empty() &&
           pool->wait_tls.empty() && pool->h1_tls_actors.empty();
  }

  static void close_pool_idle(OriginPool& pool) {
    for (auto& conn : pool.idle_plain) {
      if (conn) {
        close_plain(*conn);
      }
    }
    for (auto& conn : pool.idle_tls) {
      if (conn) {
        close_tls(*conn);
      }
    }
    pool.idle_plain.clear();
    pool.idle_tls.clear();
    pool.active_plain = 0;
    pool.active_tls = 0;
    pool.connecting_plain = 0;
    pool.connecting_tls = 0;
    pool.reserved_idle_plain = 0;
    pool.reserved_idle_tls = 0;
  }

  struct AcquiredConnection {
    std::unique_ptr<PlainConnection> plain;
    std::unique_ptr<TlsConnection> tls;
    bool reused = false;
  };

  struct RequestCancelState {
    std::atomic<bool> cancelled{false};
    PlainConnection* plain = nullptr;
    TlsConnection* tls = nullptr;
    WaiterPtr waiter;
  };

  struct InflightGuard {
    Impl* self = nullptr;
    bool active = false;

    explicit InflightGuard(Impl* owner) : self(owner), active(owner != nullptr) {
      if (!active) {
        return;
      }
      auto value = self->global_inflight_.fetch_add(1, std::memory_order_relaxed) + 1;
      self->maybe_expand_auto_shards(value);
    }

    InflightGuard(const InflightGuard&) = delete;
    InflightGuard& operator=(const InflightGuard&) = delete;

    InflightGuard(InflightGuard&& other) noexcept
        : self(other.self), active(other.active) {
      other.active = false;
    }

    ~InflightGuard() {
      if (active && self) {
        self->global_inflight_.fetch_sub(1, std::memory_order_relaxed);
      }
    }
  };

  void cancel_request_on_shard(const std::shared_ptr<RequestCancelState>& state) {
    if (!state) {
      return;
    }
    state->cancelled.store(true, std::memory_order_release);
    if (state->plain) {
      ++stats_.h1_close_on_cancel;
      close_plain(*state->plain);
    }
    if (state->tls) {
      ++stats_.h1_close_on_cancel;
      close_tls(*state->tls);
    }
    if (state->waiter && !state->waiter->cancelled) {
      state->waiter->cancelled = true;
      state->waiter->timer.cancel();
    }
  }

  struct H1TlsActor : std::enable_shared_from_this<H1TlsActor> {
    struct Item {
      Request request;
      std::chrono::steady_clock::time_point start;
      Response response;
      std::shared_ptr<asio::steady_timer> notify;
    };

    H1TlsActor(Impl* owner, asio::any_io_executor executor,
               std::shared_ptr<OriginPool> pool, ParsedUrl url)
        : strand(asio::make_strand(executor)),
          owner(owner),
          pool(std::move(pool)),
          url(std::move(url)) {}

    asio::awaitable<Response> submit(Request request,
                                     std::chrono::steady_clock::time_point start) {
      auto ex = co_await asio::this_coro::executor;
      auto item = std::make_shared<Item>();
      item->request = std::move(request);
      item->start = start;
      item->notify = std::make_shared<asio::steady_timer>(ex);
      item->notify->expires_at(asio::steady_timer::time_point::max());
      pending_count.fetch_add(1, std::memory_order_relaxed);

      auto self = shared_from_this();
      co_await asio::co_spawn(
          strand,
          [self, item]() -> asio::awaitable<void> {
            self->queue.push_back(item);
            if (!self->running) {
              self->running = true;
              asio::co_spawn(self->strand, self->run(), asio::detached);
            }
            co_return;
          },
          asio::use_awaitable);

      boost::system::error_code ec;
      co_await item->notify->async_wait(asio::redirect_error(asio::use_awaitable, ec));
      co_return std::move(item->response);
    }

    asio::awaitable<void> run() {
      for (;;) {
        if (queue.empty()) {
          running = false;
          co_return;
        }

        auto item = std::move(queue.front());
        queue.pop_front();
        pending_count.fetch_sub(1, std::memory_order_relaxed);

        try {
          auto connect_timeout =
              effective_timeout(item->request, &Request::Timeout::connect_ms);
          auto write_timeout =
              effective_timeout(item->request, &Request::Timeout::write_ms);
          auto read_timeout =
              effective_timeout(item->request, &Request::Timeout::read_ms);
          co_await ensure_connected(item->request, connect_timeout);
          auto timings = owner ? owner->h1_exchange_timings() : H1ExchangeTimings{};
          if (owner && owner->options_.use_lightweight_h1) {
            item->response =
                co_await run_light_h1_exchange(conn->stream, conn->read_buffer,
                                               conn->write_buffer, url,
                                               std::move(item->request), item->start,
                                               write_timeout, read_timeout, false,
                                               &timings);
          } else {
            item->response =
                co_await run_http_exchange(conn->stream, conn->buffer, url,
                                           std::move(item->request), item->start,
                                           write_timeout, read_timeout, false,
                                           &timings);
          }
        } catch (const std::exception& e) {
          item->response.error = e.what();
          if (item->request.measure_total_time) {
            item->response.total_time_sec =
                std::chrono::duration<double>(std::chrono::steady_clock::now() -
                                              item->start)
                    .count();
          }
          close();
        }

        if (item->notify) {
          item->notify->cancel();
        }
      }
    }

    asio::awaitable<void> ensure_connected(const Request& request,
                                           std::chrono::milliseconds timeout) {
      if (conn) {
        co_return;
      }

      auto connect_started = std::chrono::steady_clock::now();
      conn = std::make_unique<TlsConnection>(strand, pool->ssl_ctx);
      if (request.verify_peer && request.verify_host) {
        conn->stream.set_verify_mode(asio::ssl::verify_peer);
      } else {
        conn->stream.set_verify_mode(asio::ssl::verify_none);
      }

      if (!SSL_set_tlsext_host_name(conn->stream.native_handle(), url.host.c_str())) {
        throw beast::system_error(
            beast::error_code(static_cast<int>(::ERR_get_error()),
                              asio::error::get_ssl_category()));
      }

      tcp::resolver resolver(strand);
      beast::get_lowest_layer(conn->stream).expires_after(timeout);
      auto results = co_await resolver.async_resolve(url.host, url.port,
                                                     asio::use_awaitable);
      co_await beast::get_lowest_layer(conn->stream)
          .async_connect(results, asio::use_awaitable);
      boost::system::error_code option_ec;
      beast::get_lowest_layer(conn->stream)
          .socket()
          .set_option(tcp::no_delay(true), option_ec);
      co_await conn->stream.async_handshake(asio::ssl::stream_base::client,
                                            asio::use_awaitable);
      if (owner) {
        ++owner->stats_.h1_conn_created;
        owner->record_timing(owner->stats_.h1_connect,
                             owner->elapsed_us(connect_started));
      }
    }

    std::size_t pending() const {
      return pending_count.load(std::memory_order_relaxed);
    }

    void close() {
      if (!conn) {
        return;
      }
      boost::system::error_code ec;
      beast::get_lowest_layer(conn->stream).socket().shutdown(tcp::socket::shutdown_both,
                                                              ec);
      beast::get_lowest_layer(conn->stream).socket().close(ec);
      conn.reset();
    }

    asio::strand<asio::any_io_executor> strand;
    Impl* owner = nullptr;
    std::shared_ptr<OriginPool> pool;
    ParsedUrl url;
    std::unique_ptr<TlsConnection> conn;
    std::deque<std::shared_ptr<Item>> queue;
    std::atomic<std::size_t> pending_count{0};
    bool running = false;
  };

  explicit Impl(Options options) : options_(options) {
    if (options_.shard_count == 0) {
      auto n = std::thread::hardware_concurrency();
      auto cap = options_.auto_shards ? std::min<std::size_t>(16, n)
                                      : std::min<std::size_t>(4, n);
      options_.shard_count = std::max<std::size_t>(1, cap);
    }
    shards_.reserve(options_.shard_count);
    for (std::size_t i = 0; i < options_.shard_count; ++i) {
      shards_.push_back(std::make_unique<Shard>());
    }
    for (auto& shard : shards_) {
      auto* shard_ptr = shard.get();
      shard->thread = std::jthread([shard_ptr] { shard_ptr->io.run(); });
    }
  }

  ~Impl() {
    for (auto& shard : shards_) {
      shard->maintenance_timer.cancel();
      shard->work.reset();
      shard->io.stop();
    }
  }

  asio::awaitable<void> reset_connections() {
    for (auto& shard : shards_) {
      co_await asio::co_spawn(
          shard->io, [shard = shard.get()]() -> asio::awaitable<void> {
            for (auto& [_, pool] : shard->origins) {
              auto closed_plain = pool->idle_plain.size();
              auto closed_tls = pool->idle_tls.size();
              for (auto& conn : pool->idle_plain) {
                if (conn) {
                  close_plain(*conn);
                }
              }
              for (auto& conn : pool->idle_tls) {
                if (conn) {
                  close_tls(*conn);
                }
              }
              pool->idle_plain.clear();
              pool->idle_tls.clear();
              pool->active_plain =
                  closed_plain > pool->active_plain ? 0 : pool->active_plain - closed_plain;
              pool->active_tls =
                  closed_tls > pool->active_tls ? 0 : pool->active_tls - closed_tls;
              pool->connecting_plain = 0;
              pool->connecting_tls = 0;
              pool->reserved_idle_plain = 0;
              pool->reserved_idle_tls = 0;
              wake_one(pool->wait_plain);
              wake_one(pool->wait_tls);
              for (auto& actor : pool->h1_tls_actors) {
                if (actor) {
                  actor->close();
                }
              }
              pool->h1_tls_actors.clear();
            }
            co_return;
          },
          asio::use_awaitable);
    }
  }

  void start_maintenance() {
    if (options_.maintenance_interval.count() <= 0) {
      return;
    }
    for (auto& shard : shards_) {
      schedule_maintenance(*shard);
    }
  }

  Shard& pick_shard(const ParsedUrl& url) {
    auto key = url.scheme + "://" + url.host + ":" + url.port;
    auto idx = std::hash<std::string>{}(key) % shards_.size();
    return *shards_[idx];
  }

  Shard& pick_request_shard(const ParsedUrl& url) {
    if (!options_.stripe_origins_across_shards) {
      return pick_shard(url);
    }
    auto key = url.scheme + "://" + url.host + ":" + url.port;
    auto base = std::hash<std::string>{}(key);
    std::size_t shard_limit = shards_.size();
    if (options_.auto_shards) {
      shard_limit = active_auto_shards_.load(std::memory_order_relaxed);
      shard_limit = std::max<std::size_t>(1, std::min(shard_limit, shards_.size()));
    }
    auto seq = next_request_shard_.fetch_add(1, std::memory_order_relaxed);
    return *shards_[(base + seq) % shard_limit];
  }

  void maybe_expand_auto_shards(std::size_t inflight) {
    if (!options_.auto_shards || shards_.size() <= 1) {
      return;
    }
    auto current = active_auto_shards_.load(std::memory_order_relaxed);
    auto target = current;
    if (inflight >= 512) {
      target = std::min<std::size_t>(16, shards_.size());
    } else if (inflight >= 128) {
      target = std::min<std::size_t>(8, shards_.size());
    } else if (inflight >= 96) {
      target = std::min<std::size_t>(4, shards_.size());
    } else if (inflight >= 32) {
      target = std::min<std::size_t>(2, shards_.size());
    }
    if (target <= current) {
      return;
    }
    auto now = std::chrono::steady_clock::now();
    const bool urgent = inflight >= 128;
    if (!urgent) {
      std::lock_guard<std::mutex> lock(auto_scale_mu_);
      if (now - last_auto_scale_up_ < options_.auto_scale_up_interval) {
        return;
      }
      last_auto_scale_up_ = now;
    }
    // Shards and threads are already constructed. Scaling the active routing
    // window directly avoids the first-burst pool wait that otherwise appears
    // when a high-concurrency H1 workload ramps from one shard by powers of two.
    auto wanted = target;
    while (wanted > current &&
           !active_auto_shards_.compare_exchange_weak(
               current, wanted, std::memory_order_relaxed,
               std::memory_order_relaxed)) {
      wanted = target;
    }
  }

  void schedule_maintenance(Shard& shard) {
    shard.maintenance_timer.expires_after(options_.maintenance_interval);
    std::weak_ptr<Impl> weak_self = shared_from_this();
    shard.maintenance_timer.async_wait(
        [&shard, weak_self](boost::system::error_code ec) {
          if (ec) {
            return;
          }
          auto self = weak_self.lock();
          if (!self) {
            return;
          }
          self->run_maintenance(shard);
          self->schedule_maintenance(shard);
        });
  }

  void run_maintenance(Shard& shard) {
    evict_idle_origins(shard, true);
    if (!options_.auto_shards || &shard != shards_.front().get()) {
      return;
    }
    auto now = std::chrono::steady_clock::now();
    auto inflight = global_inflight_.load(std::memory_order_relaxed);
    std::lock_guard<std::mutex> lock(auto_scale_mu_);
    if (inflight > 8) {
      last_auto_busy_ = now;
      return;
    }
    if (now - last_auto_busy_ < options_.auto_scale_down_idle_ttl) {
      return;
    }
    auto current = active_auto_shards_.load(std::memory_order_relaxed);
    if (current > 1) {
      active_auto_shards_.store(std::max<std::size_t>(1, current / 2),
                                std::memory_order_relaxed);
      last_auto_busy_ = now;
    }
  }

  std::shared_ptr<OriginPool> get_origin_pool(Shard& shard, const ParsedUrl& url) {
    auto key = url.scheme + "://" + url.host + ":" + url.port;
    auto it = shard.origins.find(key);
    if (it != shard.origins.end()) {
      touch_origin(shard, key, *it->second);
      return it->second;
    }
    evict_idle_origins(shard);
    auto pool =
        std::make_shared<OriginPool>(shard.io.get_executor(), options_.enable_ssl_verify);
    pool->last_used = std::chrono::steady_clock::now();
    shard.origin_lru.push_front(key);
    pool->lru_it = shard.origin_lru.begin();
    pool->lru_linked = true;
    shard.origins.emplace(std::move(key), pool);
    return pool;
  }

  void touch_origin(Shard& shard, const std::string& key, OriginPool& pool) {
    pool.last_used = std::chrono::steady_clock::now();
    if (!pool.lru_linked) {
      shard.origin_lru.push_front(key);
      pool.lru_it = shard.origin_lru.begin();
      pool.lru_linked = true;
      return;
    }
    shard.origin_lru.splice(shard.origin_lru.begin(), shard.origin_lru, pool.lru_it);
  }

  void evict_idle_origins(Shard& shard, bool periodic = false) {
    const auto max_origins = options_.max_origins_per_shard;
    if (max_origins == 0) {
      return;
    }
    const auto now = std::chrono::steady_clock::now();
    const auto ttl = options_.origin_idle_ttl;
    auto should_evict_by_ttl = [&](const std::shared_ptr<OriginPool>& pool) {
      return ttl.count() > 0 && pool &&
             now - pool->last_used >= ttl;
    };

    for (auto it = shard.origin_lru.rbegin(); it != shard.origin_lru.rend();) {
      auto map_it = shard.origins.find(*it);
      if (map_it == shard.origins.end()) {
        auto erase_it = std::next(it).base();
        it = std::make_reverse_iterator(shard.origin_lru.erase(erase_it));
        continue;
      }
      auto& pool = map_it->second;
      if (pool_idle(pool) && (should_evict_by_ttl(pool) ||
                              shard.origins.size() > max_origins)) {
        close_pool_idle(*pool);
        pool->lru_linked = false;
        auto erase_key_it = std::next(it).base();
        it = std::make_reverse_iterator(shard.origin_lru.erase(erase_key_it));
        shard.origins.erase(map_it);
        continue;
      }
      if (!periodic && shard.origins.size() <= max_origins) {
        break;
      }
      ++it;
    }
  }

  asio::awaitable<AcquiredConnection> acquire_plain(
      std::shared_ptr<OriginPool> pool, const ParsedUrl& url,
      std::chrono::milliseconds connect_timeout,
      std::chrono::milliseconds pool_timeout,
      std::shared_ptr<RequestCancelState> cancel_state = {}) {
    auto acquire_started = std::chrono::steady_clock::now();
    const auto pool_deadline = std::chrono::steady_clock::now() + pool_timeout;
    const auto max_connections =
        std::max<std::size_t>(1, options_.max_connections_per_origin);
    const auto max_connecting =
        std::max<std::size_t>(1, options_.max_connecting_per_origin);
    bool woke_from_wait = false;
    bool has_idle_reservation = false;
    for (;;) {
      AcquiredConnection conn;
      if (!pool->idle_plain.empty() &&
          (has_idle_reservation ||
           (pool->reserved_idle_plain == 0 &&
            (woke_from_wait || pool->wait_plain.empty())))) {
        if (has_idle_reservation && pool->reserved_idle_plain > 0) {
          --pool->reserved_idle_plain;
        }
        conn.plain = std::move(pool->idle_plain.back());
        pool->idle_plain.pop_back();
        conn.reused = true;
        ++stats_.h1_idle_hit;
        ++stats_.h1_conn_reused;
        record_timing(stats_.h1_acquire, elapsed_us(acquire_started));
        co_return conn;
      }
      if (has_idle_reservation && pool->idle_plain.empty()) {
        if (pool->reserved_idle_plain > 0) {
          --pool->reserved_idle_plain;
        }
        has_idle_reservation = false;
      }
      if (pool->active_plain < max_connections &&
          pool->connecting_plain < max_connecting &&
          (woke_from_wait ||
           (pool->reserved_idle_plain == 0 && pool->wait_plain.empty()))) {
        ++pool->active_plain;
        ++pool->connecting_plain;
        ++stats_.h1_idle_miss;
        try {
          auto connect_started = std::chrono::steady_clock::now();
          auto new_conn =
              std::make_unique<PlainConnection>(co_await asio::this_coro::executor);
          ++stats_.h1_conn_created;
          tcp::resolver resolver(co_await asio::this_coro::executor);
          new_conn->stream.expires_after(connect_timeout);
          auto results = co_await resolver.async_resolve(url.host, url.port,
                                                         asio::use_awaitable);
          co_await new_conn->stream.async_connect(results, asio::use_awaitable);
          boost::system::error_code option_ec;
          new_conn->stream.socket().set_option(tcp::no_delay(true), option_ec);
          if (pool->connecting_plain > 0) {
            --pool->connecting_plain;
          }
          wake_one(pool->wait_plain);
          record_timing(stats_.h1_connect, elapsed_us(connect_started));
          record_timing(stats_.h1_acquire, elapsed_us(acquire_started));
          co_return AcquiredConnection{std::move(new_conn), {}, false};
        } catch (...) {
          if (pool->connecting_plain > 0) {
            --pool->connecting_plain;
          }
          if (pool->active_plain > 0) {
            --pool->active_plain;
          }
          wake_one(pool->wait_plain);
          throw;
        }
      }

      auto waiter =
          std::make_shared<Waiter>(co_await asio::this_coro::executor);
      if (cancel_state) {
        cancel_state->waiter = waiter;
      }
      waiter->timer.expires_at(pool_timeout.count() > 0
                                   ? pool_deadline
                                   : asio::steady_timer::time_point::max());
      pool->wait_plain.push_back(waiter);
      boost::system::error_code ec;
      auto wait_started = std::chrono::steady_clock::now();
      co_await waiter->timer.async_wait(asio::redirect_error(asio::use_awaitable, ec));
      record_timing(stats_.h1_pool_wait, elapsed_us(wait_started));
      if (cancel_state) {
        cancel_state->waiter.reset();
      }
      if (ec == asio::error::operation_aborted && !waiter->woken) {
        waiter->cancelled = true;
        remove_waiter(pool->wait_plain, waiter);
        ++stats_.h1_pool_wait_cancelled;
        throw std::runtime_error("h1 pool wait cancelled");
      }
      if (ec != asio::error::operation_aborted &&
          pool_timeout.count() > 0 &&
            std::chrono::steady_clock::now() >= pool_deadline) {
        throw std::runtime_error("h1 pool timeout");
      }
      woke_from_wait = waiter->woken;
      has_idle_reservation = waiter->reserved_idle;
    }
  }

  asio::awaitable<AcquiredConnection> acquire_plain_direct(
      const ParsedUrl& connect_url, std::chrono::milliseconds connect_timeout) {
    auto new_conn =
        std::make_unique<PlainConnection>(co_await asio::this_coro::executor);
    tcp::resolver resolver(co_await asio::this_coro::executor);
    new_conn->stream.expires_after(connect_timeout);
    auto results = co_await resolver.async_resolve(connect_url.host, connect_url.port,
                                                   asio::use_awaitable);
    co_await new_conn->stream.async_connect(results, asio::use_awaitable);
    boost::system::error_code option_ec;
    new_conn->stream.socket().set_option(tcp::no_delay(true), option_ec);
    co_return AcquiredConnection{std::move(new_conn), {}, false};
  }

  asio::awaitable<std::unique_ptr<TlsConnection>> connect_https_proxy(
      std::shared_ptr<OriginPool> pool, const ParsedUrl& proxy_url,
      const Request& request, std::chrono::milliseconds connect_timeout) {
    auto conn =
        std::make_unique<TlsConnection>(co_await asio::this_coro::executor,
                                        pool->ssl_ctx);
    if (request.verify_peer && request.verify_host && options_.enable_ssl_verify) {
      conn->stream.set_verify_mode(asio::ssl::verify_peer);
    } else {
      conn->stream.set_verify_mode(asio::ssl::verify_none);
    }
    if (!SSL_set_tlsext_host_name(conn->stream.native_handle(),
                                  proxy_url.host.c_str())) {
      throw beast::system_error(
          beast::error_code(static_cast<int>(::ERR_get_error()),
                            asio::error::get_ssl_category()));
    }
    tcp::resolver resolver(co_await asio::this_coro::executor);
    beast::get_lowest_layer(conn->stream).expires_after(connect_timeout);
    auto results = co_await resolver.async_resolve(proxy_url.host, proxy_url.port,
                                                   asio::use_awaitable);
    co_await beast::get_lowest_layer(conn->stream)
        .async_connect(results, asio::use_awaitable);
    boost::system::error_code option_ec;
    beast::get_lowest_layer(conn->stream)
        .socket()
        .set_option(tcp::no_delay(true), option_ec);
    co_await conn->stream.async_handshake(asio::ssl::stream_base::client,
                                          asio::use_awaitable);
    co_return conn;
  }

  asio::awaitable<std::unique_ptr<TlsConnection>> connect_tls_direct(
      std::shared_ptr<OriginPool> pool, const ParsedUrl& connect_url,
      const ParsedUrl& tls_url, const Request& request,
      std::chrono::milliseconds connect_timeout) {
    auto new_conn =
        std::make_unique<TlsConnection>(co_await asio::this_coro::executor,
                                        pool->ssl_ctx);
    if (request.verify_peer && request.verify_host && options_.enable_ssl_verify) {
      new_conn->stream.set_verify_mode(asio::ssl::verify_peer);
    } else {
      new_conn->stream.set_verify_mode(asio::ssl::verify_none);
    }
    if (!SSL_set_tlsext_host_name(new_conn->stream.native_handle(),
                                  tls_url.host.c_str())) {
      throw beast::system_error(
          beast::error_code(static_cast<int>(::ERR_get_error()),
                            asio::error::get_ssl_category()));
    }

    tcp::resolver resolver(co_await asio::this_coro::executor);
    beast::get_lowest_layer(new_conn->stream).expires_after(connect_timeout);
    auto results = co_await resolver.async_resolve(connect_url.host, connect_url.port,
                                                   asio::use_awaitable);
    co_await beast::get_lowest_layer(new_conn->stream)
        .async_connect(results, asio::use_awaitable);
    boost::system::error_code option_ec;
    beast::get_lowest_layer(new_conn->stream)
        .socket()
        .set_option(tcp::no_delay(true), option_ec);
    co_return new_conn;
  }

  asio::awaitable<std::unique_ptr<PlainConnection>> open_plain_connection(
      const ParsedUrl& url, std::chrono::milliseconds connect_timeout) {
    auto conn =
        std::make_unique<PlainConnection>(co_await asio::this_coro::executor);
    tcp::resolver resolver(co_await asio::this_coro::executor);
    conn->stream.expires_after(connect_timeout);
    auto results = co_await resolver.async_resolve(url.host, url.port,
                                                   asio::use_awaitable);
    co_await conn->stream.async_connect(results, asio::use_awaitable);
    boost::system::error_code option_ec;
    conn->stream.socket().set_option(tcp::no_delay(true), option_ec);
    co_return conn;
  }

  asio::awaitable<std::unique_ptr<TlsConnection>> open_tls_connection(
      std::shared_ptr<OriginPool> pool, const ParsedUrl& url, const Request& request,
      std::chrono::milliseconds connect_timeout) {
    auto conn =
        std::make_unique<TlsConnection>(co_await asio::this_coro::executor,
                                        pool->ssl_ctx);
    if (request.verify_peer && request.verify_host && options_.enable_ssl_verify) {
      conn->stream.set_verify_mode(asio::ssl::verify_peer);
    } else {
      conn->stream.set_verify_mode(asio::ssl::verify_none);
    }

    if (!SSL_set_tlsext_host_name(conn->stream.native_handle(), url.host.c_str())) {
      throw beast::system_error(
          beast::error_code(static_cast<int>(::ERR_get_error()),
                            asio::error::get_ssl_category()));
    }

    tcp::resolver resolver(co_await asio::this_coro::executor);
    beast::get_lowest_layer(conn->stream).expires_after(connect_timeout);
    auto results = co_await resolver.async_resolve(url.host, url.port,
                                                   asio::use_awaitable);
    co_await beast::get_lowest_layer(conn->stream)
        .async_connect(results, asio::use_awaitable);
    boost::system::error_code option_ec;
    beast::get_lowest_layer(conn->stream)
        .socket()
        .set_option(tcp::no_delay(true), option_ec);
    co_await conn->stream.async_handshake(asio::ssl::stream_base::client,
                                          asio::use_awaitable);
    co_return conn;
  }

  asio::awaitable<AcquiredConnection> acquire_tls(
      std::shared_ptr<OriginPool> pool, const ParsedUrl& url, const Request& request,
      std::chrono::milliseconds connect_timeout,
      std::chrono::milliseconds pool_timeout,
      std::shared_ptr<RequestCancelState> cancel_state = {}) {
    auto acquire_started = std::chrono::steady_clock::now();
    const auto pool_deadline = std::chrono::steady_clock::now() + pool_timeout;
    const auto max_connections =
        std::max<std::size_t>(1, options_.max_connections_per_origin);
    const auto max_connecting =
        std::max<std::size_t>(1, options_.max_connecting_per_origin);
    bool woke_from_wait = false;
    bool has_idle_reservation = false;
    for (;;) {
      AcquiredConnection conn;
      if (!pool->idle_tls.empty() &&
          (has_idle_reservation ||
           (pool->reserved_idle_tls == 0 &&
            (woke_from_wait || pool->wait_tls.empty())))) {
        if (has_idle_reservation && pool->reserved_idle_tls > 0) {
          --pool->reserved_idle_tls;
        }
        conn.tls = std::move(pool->idle_tls.back());
        pool->idle_tls.pop_back();
        conn.reused = true;
        ++stats_.h1_idle_hit;
        ++stats_.h1_conn_reused;
        record_timing(stats_.h1_acquire, elapsed_us(acquire_started));
        co_return conn;
      }
      if (has_idle_reservation && pool->idle_tls.empty()) {
        if (pool->reserved_idle_tls > 0) {
          --pool->reserved_idle_tls;
        }
        has_idle_reservation = false;
      }
      if (pool->active_tls >= max_connections ||
          pool->connecting_tls >= max_connecting ||
          (!woke_from_wait &&
           (pool->reserved_idle_tls > 0 || !pool->wait_tls.empty()))) {
        auto waiter =
            std::make_shared<Waiter>(co_await asio::this_coro::executor);
        if (cancel_state) {
          cancel_state->waiter = waiter;
        }
        waiter->timer.expires_at(pool_timeout.count() > 0
                                     ? pool_deadline
                                     : asio::steady_timer::time_point::max());
        pool->wait_tls.push_back(waiter);
        boost::system::error_code ec;
        auto wait_started = std::chrono::steady_clock::now();
        co_await waiter->timer.async_wait(asio::redirect_error(asio::use_awaitable, ec));
        record_timing(stats_.h1_pool_wait, elapsed_us(wait_started));
        if (cancel_state) {
          cancel_state->waiter.reset();
        }
        if (ec == asio::error::operation_aborted && !waiter->woken) {
          waiter->cancelled = true;
          remove_waiter(pool->wait_tls, waiter);
          ++stats_.h1_pool_wait_cancelled;
          throw std::runtime_error("h1 pool wait cancelled");
        }
        if (ec != asio::error::operation_aborted &&
            pool_timeout.count() > 0 &&
            std::chrono::steady_clock::now() >= pool_deadline) {
          throw std::runtime_error("h1 pool timeout");
        }
        woke_from_wait = waiter->woken;
        has_idle_reservation = waiter->reserved_idle;
        continue;
      }
      ++pool->active_tls;
      ++pool->connecting_tls;
      ++stats_.h1_idle_miss;
      break;
    }

    try {
      auto connect_started = std::chrono::steady_clock::now();
      auto new_conn = co_await open_tls_connection(pool, url, request,
                                                   connect_timeout);
      if (pool->connecting_tls > 0) {
        --pool->connecting_tls;
      }
      wake_one(pool->wait_tls);
      ++stats_.h1_conn_created;
      record_timing(stats_.h1_connect, elapsed_us(connect_started));
      record_timing(stats_.h1_acquire, elapsed_us(acquire_started));
      co_return AcquiredConnection{{}, std::move(new_conn), false};
    } catch (...) {
      if (pool->connecting_tls > 0) {
        --pool->connecting_tls;
      }
      if (pool->active_tls > 0) {
        --pool->active_tls;
      }
      wake_one(pool->wait_tls);
      throw;
    }
  }

  asio::awaitable<std::unique_ptr<TlsConnection>> connect_probe_tls(
      std::shared_ptr<OriginPool> pool, const ParsedUrl& url, const Request& request,
      std::chrono::milliseconds connect_timeout,
      std::chrono::milliseconds pool_timeout) {
    auto acquire_started = std::chrono::steady_clock::now();
    const auto max_connections =
        std::max<std::size_t>(1, options_.max_connections_per_origin);
    const auto max_connecting =
        std::max<std::size_t>(1, options_.max_connecting_per_origin);
    const auto pool_deadline = std::chrono::steady_clock::now() + pool_timeout;
    bool has_idle_reservation = false;
    while (((pool->active_tls >= max_connections ||
             pool->connecting_tls >= max_connecting) &&
            pool->idle_tls.empty()) ||
           (!has_idle_reservation && pool->reserved_idle_tls > 0)) {
      auto waiter =
          std::make_shared<Waiter>(co_await asio::this_coro::executor);
      waiter->timer.expires_at(pool_timeout.count() > 0
                                   ? pool_deadline
                                   : asio::steady_timer::time_point::max());
      pool->wait_tls.push_back(waiter);
      boost::system::error_code ec;
      auto wait_started = std::chrono::steady_clock::now();
      co_await waiter->timer.async_wait(asio::redirect_error(asio::use_awaitable, ec));
      record_timing(stats_.h1_pool_wait, elapsed_us(wait_started));
      if (ec == asio::error::operation_aborted && !waiter->woken) {
        waiter->cancelled = true;
        remove_waiter(pool->wait_tls, waiter);
        ++stats_.h1_pool_wait_cancelled;
        throw std::runtime_error("h1 pool wait cancelled");
      }
      if (ec != asio::error::operation_aborted &&
          pool_timeout.count() > 0 &&
          std::chrono::steady_clock::now() >= pool_deadline) {
        throw std::runtime_error("h1 pool timeout");
      }
      has_idle_reservation = waiter->reserved_idle;
    }
    if (!pool->idle_tls.empty()) {
      if (has_idle_reservation && pool->reserved_idle_tls > 0) {
        --pool->reserved_idle_tls;
      }
      auto conn = std::move(pool->idle_tls.back());
      pool->idle_tls.pop_back();
      set_h2_h1_alpn(conn->stream.native_handle());
      record_timing(stats_.h1_acquire, elapsed_us(acquire_started));
      co_return conn;
    }
    ++pool->active_tls;
    ++pool->connecting_tls;
    try {
      auto connect_started = std::chrono::steady_clock::now();
      auto conn =
          std::make_unique<TlsConnection>(co_await asio::this_coro::executor,
                                          pool->ssl_ctx);
      if (request.verify_peer && request.verify_host && options_.enable_ssl_verify) {
        conn->stream.set_verify_mode(asio::ssl::verify_peer);
      } else {
        conn->stream.set_verify_mode(asio::ssl::verify_none);
      }

      if (!SSL_set_tlsext_host_name(conn->stream.native_handle(), url.host.c_str())) {
        throw beast::system_error(
            beast::error_code(static_cast<int>(::ERR_get_error()),
                              asio::error::get_ssl_category()));
      }
      set_h2_h1_alpn(conn->stream.native_handle());

      tcp::resolver resolver(co_await asio::this_coro::executor);
      beast::get_lowest_layer(conn->stream).expires_after(connect_timeout);
      auto results = co_await resolver.async_resolve(url.host, url.port,
                                                     asio::use_awaitable);
      co_await beast::get_lowest_layer(conn->stream)
          .async_connect(results, asio::use_awaitable);
      boost::system::error_code option_ec;
      beast::get_lowest_layer(conn->stream)
          .socket()
          .set_option(tcp::no_delay(true), option_ec);
      co_await conn->stream.async_handshake(asio::ssl::stream_base::client,
                                            asio::use_awaitable);
      if (pool->connecting_tls > 0) {
        --pool->connecting_tls;
      }
      wake_one(pool->wait_tls);
      record_timing(stats_.h1_connect, elapsed_us(connect_started));
      record_timing(stats_.h1_acquire, elapsed_us(acquire_started));
      co_return conn;
    } catch (...) {
      if (pool->connecting_tls > 0) {
        --pool->connecting_tls;
      }
      if (pool->active_tls > 0) {
        --pool->active_tls;
      }
      wake_one(pool->wait_tls);
      throw;
    }
  }

  asio::awaitable<Response> run_pooled_request(std::shared_ptr<OriginPool> pool,
                                               ParsedUrl url, Request request,
                                               std::chrono::steady_clock::time_point start,
                                               std::shared_ptr<RequestCancelState> cancel_state =
                                                   {}) {
    auto connect_timeout =
        effective_timeout(request, &Request::Timeout::connect_ms);
    auto pool_timeout = effective_timeout(request, &Request::Timeout::pool_ms);
    auto write_timeout = effective_timeout(request, &Request::Timeout::write_ms);
    auto read_timeout = effective_timeout(request, &Request::Timeout::read_ms);
    auto proxy = proxy_support::proxy_for_request(request);
    auto timings = h1_exchange_timings();
    if (proxy.enabled) {
      auto proxy_connect_url = proxy_url_as_parsed(proxy.url);
      if (url.tls) {
        std::unique_ptr<TlsConnection> tls_conn;
        if (proxy.scheme == proxy_support::Scheme::Socks5) {
          tls_conn = co_await connect_tls_direct(
              pool, proxy_connect_url, url, request, connect_timeout);
          co_await proxy_support::establish_socks5_tunnel(
              beast::get_lowest_layer(tls_conn->stream), url.host, url.port,
              proxy.url);
          beast::get_lowest_layer(tls_conn->stream).expires_after(connect_timeout);
          co_await tls_conn->stream.async_handshake(
              asio::ssl::stream_base::client, asio::use_awaitable);
        } else if (proxy.scheme == proxy_support::Scheme::Https) {
          auto nested = std::make_unique<NestedTlsConnection>(
              co_await asio::this_coro::executor, pool->ssl_ctx, pool->ssl_ctx);
          if (request.verify_peer && request.verify_host &&
              options_.enable_ssl_verify) {
            nested->stream.next_layer().set_verify_mode(asio::ssl::verify_peer);
            nested->stream.set_verify_mode(asio::ssl::verify_peer);
          } else {
            nested->stream.next_layer().set_verify_mode(asio::ssl::verify_none);
            nested->stream.set_verify_mode(asio::ssl::verify_none);
          }
          if (!SSL_set_tlsext_host_name(nested->stream.next_layer().native_handle(),
                                        proxy.url.host.c_str()) ||
              !SSL_set_tlsext_host_name(nested->stream.native_handle(),
                                        url.host.c_str())) {
            throw beast::system_error(
                beast::error_code(static_cast<int>(::ERR_get_error()),
                                  asio::error::get_ssl_category()));
          }
          tcp::resolver resolver(co_await asio::this_coro::executor);
          beast::get_lowest_layer(nested->stream).expires_after(connect_timeout);
          auto results = co_await resolver.async_resolve(proxy.url.host,
                                                         proxy.url.port,
                                                         asio::use_awaitable);
          co_await beast::get_lowest_layer(nested->stream)
              .async_connect(results, asio::use_awaitable);
          boost::system::error_code option_ec;
          beast::get_lowest_layer(nested->stream)
              .socket()
              .set_option(tcp::no_delay(true), option_ec);
          co_await nested->stream.next_layer().async_handshake(
              asio::ssl::stream_base::client, asio::use_awaitable);
          co_await proxy_support::establish_http_connect_tunnel(
              nested->stream.next_layer(), url.host, url.port,
              proxy.authorization);
          co_await nested->stream.async_handshake(
              asio::ssl::stream_base::client, asio::use_awaitable);
          if (options_.use_lightweight_h1) {
            co_return co_await run_light_h1_exchange(
                nested->stream, nested->read_buffer, nested->write_buffer, url,
                std::move(request), start, write_timeout, read_timeout);
          }
          co_return co_await run_http_exchange(nested->stream, nested->buffer, url,
                                               std::move(request), start,
                                               write_timeout, read_timeout);
        } else {
          tls_conn = co_await connect_tls_direct(
              pool, proxy_connect_url, url, request, connect_timeout);
          co_await proxy_support::establish_http_connect_tunnel(
              beast::get_lowest_layer(tls_conn->stream), url.host, url.port,
              proxy.authorization);
          beast::get_lowest_layer(tls_conn->stream).expires_after(connect_timeout);
          co_await tls_conn->stream.async_handshake(
              asio::ssl::stream_base::client, asio::use_awaitable);
        }
        if (options_.use_lightweight_h1) {
          co_return co_await run_light_h1_exchange(
              tls_conn->stream, tls_conn->read_buffer, tls_conn->write_buffer, url,
              std::move(request), start, write_timeout, read_timeout);
        }
        co_return co_await run_http_exchange(tls_conn->stream, tls_conn->buffer, url,
                                             std::move(request), start, write_timeout,
                                             read_timeout);
      }

      if (proxy.scheme == proxy_support::Scheme::Https) {
        auto tls_conn = co_await connect_https_proxy(
            pool, proxy_connect_url, request, connect_timeout);
        add_proxy_authorization(request, proxy);
        if (options_.use_lightweight_h1) {
          co_return co_await run_light_h1_exchange(
              tls_conn->stream, tls_conn->read_buffer, tls_conn->write_buffer, url,
              std::move(request), start, write_timeout, read_timeout, true);
        }
        co_return co_await run_http_exchange(tls_conn->stream, tls_conn->buffer, url,
                                             std::move(request), start,
                                             write_timeout, read_timeout, true);
      }

      auto conn = co_await acquire_plain_direct(proxy_connect_url, connect_timeout);
      auto* plain_conn = conn.plain.get();
      if (proxy.scheme == proxy_support::Scheme::Socks5) {
        co_await proxy_support::establish_socks5_tunnel(
            plain_conn->stream, url.host, url.port, proxy.url);
        if (options_.use_lightweight_h1) {
          co_return co_await run_light_h1_exchange(
              plain_conn->stream, plain_conn->read_buffer, plain_conn->write_buffer,
              url, std::move(request), start, write_timeout, read_timeout);
        }
        co_return co_await run_http_exchange(plain_conn->stream, plain_conn->buffer,
                                             url, std::move(request), start,
                                             write_timeout, read_timeout);
      }
      add_proxy_authorization(request, proxy);
      if (options_.use_lightweight_h1) {
        co_return co_await run_light_h1_exchange(
            plain_conn->stream, plain_conn->read_buffer, plain_conn->write_buffer,
            url, std::move(request), start, write_timeout, read_timeout, true);
      }
      co_return co_await run_http_exchange(plain_conn->stream, plain_conn->buffer,
                                           url, std::move(request), start,
                                           write_timeout, read_timeout, true);
    }

    if (url.tls) {
      auto conn = co_await acquire_tls(pool, url, request, connect_timeout,
                                      pool_timeout, cancel_state);
      auto* tls_conn = conn.tls.get();
      if (cancel_state) {
        cancel_state->tls = tls_conn;
      }
      Response response;
      try {
        if (options_.use_lightweight_h1) {
          response = co_await run_light_h1_exchange(tls_conn->stream,
                                                    tls_conn->read_buffer,
                                                    tls_conn->write_buffer, url,
                                                    std::move(request), start,
                                                    write_timeout, read_timeout, false,
                                                    &timings);
        } else {
          response =
              co_await run_http_exchange(tls_conn->stream, tls_conn->buffer, url,
                                         std::move(request), start, write_timeout,
                                         read_timeout, false, &timings);
        }
      } catch (const std::exception& e) {
        if (cancel_state) {
          cancel_state->tls = nullptr;
        }
        if (conn.tls) {
          close_tls(*conn.tls);
        }
        if (pool->active_tls > 0) {
          --pool->active_tls;
        }
        wake_one(pool->wait_tls);
        if ((cancel_state && cancel_state->cancelled.load(std::memory_order_acquire)) ||
            is_operation_aborted_exception(e)) {
          ++stats_.h1_cancelled;
        }
        if (conn.reused) {
          ++stats_.h1_reuse_failed;
          ++stats_.h1_reconnect_after_idle;
          Response response;
          response.error = e.what();
          response.http_version = 1;
          response.reused_connection = true;
          if (request.measure_total_time) {
            response.total_time_sec =
                std::chrono::duration<double>(std::chrono::steady_clock::now() - start)
                    .count();
          }
          co_return response;
        }
        throw;
      }
      if (cancel_state) {
        cancel_state->tls = nullptr;
      }
      response.reused_connection = conn.reused;
      if (response.error.empty()) {
        pool->idle_tls.push_back(std::move(conn.tls));
        ++stats_.h1_return_to_idle;
        if (wake_one_with_idle_reservation(pool->wait_tls)) {
          ++pool->reserved_idle_tls;
        }
      } else {
        ++stats_.h1_close_after_response;
        if (pool->active_tls > 0) {
          --pool->active_tls;
        }
        wake_one(pool->wait_tls);
        if (conn.reused) {
          ++stats_.h1_reuse_failed;
          ++stats_.h1_reconnect_after_idle;
        }
      }
      co_return response;
    }

    auto conn = co_await acquire_plain(pool, url, connect_timeout, pool_timeout,
                                       cancel_state);
    auto* plain_conn = conn.plain.get();
    if (cancel_state) {
      cancel_state->plain = plain_conn;
    }
    Response response;
    try {
      if (options_.use_lightweight_h1) {
        response = co_await run_light_h1_exchange(plain_conn->stream,
                                                  plain_conn->read_buffer,
                                                  plain_conn->write_buffer, url,
                                                  std::move(request), start,
                                                  write_timeout, read_timeout, false,
                                                  &timings);
      } else {
        response =
            co_await run_http_exchange(plain_conn->stream, plain_conn->buffer, url,
                                       std::move(request), start, write_timeout,
                                       read_timeout, false, &timings);
      }
    } catch (const std::exception& e) {
      if (cancel_state) {
        cancel_state->plain = nullptr;
      }
      if (conn.plain) {
        close_plain(*conn.plain);
      }
      if (pool->active_plain > 0) {
        --pool->active_plain;
      }
      wake_one(pool->wait_plain);
      if ((cancel_state && cancel_state->cancelled.load(std::memory_order_acquire)) ||
          is_operation_aborted_exception(e)) {
        ++stats_.h1_cancelled;
      }
      if (conn.reused) {
        ++stats_.h1_reuse_failed;
        ++stats_.h1_reconnect_after_idle;
        Response response;
        response.error = e.what();
        response.http_version = 1;
        response.reused_connection = true;
        if (request.measure_total_time) {
          response.total_time_sec =
              std::chrono::duration<double>(std::chrono::steady_clock::now() - start)
                  .count();
        }
        co_return response;
      }
      throw;
    }
    if (cancel_state) {
      cancel_state->plain = nullptr;
    }
    response.reused_connection = conn.reused;
    if (response.error.empty()) {
      pool->idle_plain.push_back(std::move(conn.plain));
      ++stats_.h1_return_to_idle;
      if (wake_one_with_idle_reservation(pool->wait_plain)) {
        ++pool->reserved_idle_plain;
      }
    } else {
      ++stats_.h1_close_after_response;
      if (pool->active_plain > 0) {
        --pool->active_plain;
      }
      wake_one(pool->wait_plain);
      if (conn.reused) {
        ++stats_.h1_reuse_failed;
        ++stats_.h1_reconnect_after_idle;
      }
    }
    co_return response;
  }

  asio::awaitable<ProbeResult> probe_request(Request request) {
    auto start = std::chrono::steady_clock::now();
    ProbeResult result;
    try {
      auto url = parse_url(request.url);
      if (!url.tls) {
        result.protocol = ProbeProtocol::Http11;
        result.response = co_await this->request(std::move(request));
        co_return result;
      }

      auto& shard = pick_shard(url);
      co_return co_await asio::co_spawn(
          shard.io,
          [self = shared_from_this(), shard = &shard, url, request = std::move(request),
           start]() mutable -> asio::awaitable<ProbeResult> {
            auto pool = self->get_origin_pool(*shard, url);
            ProbeResult result;
            auto connect_timeout =
                effective_timeout(request, &Request::Timeout::connect_ms);
            auto pool_timeout = effective_timeout(request, &Request::Timeout::pool_ms);
            auto write_timeout = effective_timeout(request, &Request::Timeout::write_ms);
            auto read_timeout = effective_timeout(request, &Request::Timeout::read_ms);
            auto conn = co_await self->connect_probe_tls(pool, url, request,
                                                         connect_timeout,
                                                         pool_timeout);
            auto timings = self->h1_exchange_timings();
            auto protocol = selected_alpn(conn->stream.native_handle());
            result.protocol = protocol;
            if (protocol == ProbeProtocol::H2) {
              boost::system::error_code ec;
              beast::get_lowest_layer(conn->stream)
                  .socket()
                  .shutdown(tcp::socket::shutdown_both, ec);
              beast::get_lowest_layer(conn->stream).socket().close(ec);
              if (pool->active_tls > 0) {
                --pool->active_tls;
              }
              wake_one(pool->wait_tls);
              co_return result;
            }

            if (self->options_.use_lightweight_h1) {
              result.response = co_await run_light_h1_exchange(conn->stream, conn->read_buffer,
                                                               conn->write_buffer, url,
                                                               std::move(request), start,
                                                               write_timeout,
                                                               read_timeout, false,
                                                               &timings);
            } else {
              result.response =
                  co_await run_http_exchange(conn->stream, conn->buffer, url,
                                             std::move(request), start,
                                             write_timeout, read_timeout, false,
                                             &timings);
            }
            if (result.response.error.empty()) {
              pool->idle_tls.push_back(std::move(conn));
              if (wake_one_with_idle_reservation(pool->wait_tls)) {
                ++pool->reserved_idle_tls;
              }
            } else {
              if (pool->active_tls > 0) {
                --pool->active_tls;
              }
              wake_one(pool->wait_tls);
            }
            if (request.measure_total_time) {
              result.response.total_time_sec =
                  std::chrono::duration<double>(std::chrono::steady_clock::now() - start)
                      .count();
            }
            co_return result;
          },
          asio::use_awaitable);
    } catch (const std::exception& e) {
      result.protocol = ProbeProtocol::Unknown;
      result.response.error = e.what();
      if (request.measure_total_time) {
        result.response.total_time_sec =
            std::chrono::duration<double>(std::chrono::steady_clock::now() - start)
                .count();
      }
      co_return result;
    }
  }

  asio::awaitable<Response> run_h1_actor_request(std::shared_ptr<OriginPool> pool,
                                                 ParsedUrl url, Request request,
                                                 std::chrono::steady_clock::time_point start) {
    auto actors_per_origin = options_.h1_actor_connections_per_origin;
    auto cancel_state = co_await asio::this_coro::cancellation_state;
    auto actor = co_await asio::co_spawn(
        pool->strand,
        [owner = this, pool, url, actors_per_origin]() mutable
            -> asio::awaitable<std::shared_ptr<H1TlsActor>> {
          auto target = std::max<std::size_t>(
              1, std::min<std::size_t>(actors_per_origin, 256));
          while (pool->h1_tls_actors.size() < target) {
            pool->h1_tls_actors.push_back(
                std::make_shared<H1TlsActor>(owner, pool->executor, pool, url));
          }
          auto actor = pool->h1_tls_actors.front();
          auto best_pending = actor ? actor->pending() : std::size_t{0};
          for (auto& candidate : pool->h1_tls_actors) {
            if (!candidate) {
              continue;
            }
            auto pending = candidate->pending();
            if (!actor || pending < best_pending) {
              actor = candidate;
              best_pending = pending;
            }
          }
          co_return actor;
        },
        asio::bind_cancellation_slot(cancel_state.slot(), asio::use_awaitable));

    co_return co_await actor->submit(std::move(request), start);
  }

  asio::awaitable<Response> request(Request request) {
    auto start = std::chrono::steady_clock::now();
    InflightGuard inflight(this);
    try {
      co_await asio::this_coro::reset_cancellation_state(
          asio::enable_total_cancellation());
      auto url = parse_url(request.url);
      auto& shard = pick_request_shard(url);
      auto cancel_state = co_await asio::this_coro::cancellation_state;
      auto child_cancel = std::make_shared<asio::cancellation_signal>();
      auto request_cancel = std::make_shared<RequestCancelState>();
      auto parent_slot = cancel_state.slot();
      if (parent_slot.is_connected()) {
        parent_slot.assign([self = shared_from_this(), shard = &shard, request_cancel,
                            child_cancel](asio::cancellation_type_t type) {
          child_cancel->emit(type);
          asio::post(shard->io, [self, request_cancel] {
            self->cancel_request_on_shard(request_cancel);
          });
        });
      }

      co_return co_await asio::co_spawn(
          shard.io,
          [self = shared_from_this(), shard = &shard, url, request = std::move(request),
           start, request_cancel, child_cancel]() mutable -> asio::awaitable<Response> {
            (void)child_cancel;
            auto pool = self->get_origin_pool(*shard, url);
            if (self->options_.use_h1_connection_actor && url.tls) {
              co_return co_await self->run_h1_actor_request(pool, url, std::move(request),
                                                            start);
            }
            co_return co_await self->run_pooled_request(pool, url, std::move(request),
                                                        start, request_cancel);
          },
          asio::bind_cancellation_slot(child_cancel->slot(), asio::use_awaitable));
    } catch (const std::exception& e) {
      Response response;
      response.error = e.what();
      if (request.measure_total_time) {
        response.total_time_sec =
            std::chrono::duration<double>(std::chrono::steady_clock::now() - start)
                .count();
      }
      co_return response;
    }
  }

  asio::awaitable<void> preconnect(Request request, std::size_t count) {
    if (count == 0) {
      co_return;
    }
    auto url = parse_url(request.url);
    std::vector<Shard*> targets;
    if (options_.stripe_origins_across_shards) {
      auto shard_limit = shards_.size();
      if (options_.auto_shards) {
        auto current = active_auto_shards_.load(std::memory_order_relaxed);
        auto wanted = std::min<std::size_t>(shards_.size(),
                                            std::max<std::size_t>(1, count));
        while (wanted > current &&
               !active_auto_shards_.compare_exchange_weak(
                   current, wanted, std::memory_order_relaxed,
                   std::memory_order_relaxed)) {
        }
        shard_limit = active_auto_shards_.load(std::memory_order_relaxed);
        shard_limit = std::max<std::size_t>(
            1, std::min<std::size_t>(shard_limit, shards_.size()));
      }
      targets.reserve(shard_limit);
      for (std::size_t i = 0; i < shard_limit; ++i) {
        targets.push_back(shards_[i].get());
      }
    } else {
      targets.push_back(&pick_shard(url));
    }

    for (std::size_t i = 0; i < targets.size(); ++i) {
      auto* shard = targets[i];
      auto remaining = count > i ? count - i : 0;
      auto target_count = (remaining + targets.size() - 1) / targets.size();
      if (target_count == 0) {
        continue;
      }
      co_await asio::co_spawn(
          shard->io,
          [self = shared_from_this(), shard, url, request, target_count]() mutable
              -> asio::awaitable<void> {
          auto pool = self->get_origin_pool(*shard, url);
          const auto max_connections =
              std::max<std::size_t>(1, self->options_.max_connections_per_origin);
          const auto max_connecting =
              std::max<std::size_t>(1, self->options_.max_connecting_per_origin);
          const auto worker_count =
              std::max<std::size_t>(1, std::min(target_count, max_connecting));
          auto connect_timeout =
              effective_timeout(request, &Request::Timeout::connect_ms);
          std::atomic<std::size_t> launched{0};
          co_await asyncx::for_each_limited(
              worker_count, worker_count,
              [&](std::size_t) -> asio::awaitable<void> {
                for (;;) {
                  const auto active = url.tls ? pool->active_tls : pool->active_plain;
                  const auto connecting =
                      url.tls ? pool->connecting_tls : pool->connecting_plain;
                  const auto idle =
                      url.tls ? pool->idle_tls.size() : pool->idle_plain.size();
                  if (idle >= target_count || active >= max_connections ||
                      connecting >= max_connecting) {
                    co_return;
                  }
                  auto index = launched.fetch_add(1, std::memory_order_relaxed);
                  if (index >= target_count) {
                    co_return;
                  }

                  if (url.tls) {
                    ++pool->active_tls;
                    ++pool->connecting_tls;
                  } else {
                    ++pool->active_plain;
                    ++pool->connecting_plain;
                  }
                  try {
                    auto connect_started = std::chrono::steady_clock::now();
                    if (url.tls) {
                      auto conn =
                          co_await self->open_tls_connection(pool, url, request,
                                                             connect_timeout);
                      if (pool->connecting_tls > 0) {
                        --pool->connecting_tls;
                      }
                      ++self->stats_.h1_conn_created;
                      self->record_timing(self->stats_.h1_connect,
                                          self->elapsed_us(connect_started));
                      pool->idle_tls.push_back(std::move(conn));
                      ++self->stats_.h1_return_to_idle;
                      if (self->wake_one_with_idle_reservation(pool->wait_tls)) {
                        ++pool->reserved_idle_tls;
                      }
                    } else {
                      auto conn =
                          co_await self->open_plain_connection(url, connect_timeout);
                      if (pool->connecting_plain > 0) {
                        --pool->connecting_plain;
                      }
                      ++self->stats_.h1_conn_created;
                      self->record_timing(self->stats_.h1_connect,
                                          self->elapsed_us(connect_started));
                      pool->idle_plain.push_back(std::move(conn));
                      ++self->stats_.h1_return_to_idle;
                      if (self->wake_one_with_idle_reservation(pool->wait_plain)) {
                        ++pool->reserved_idle_plain;
                      }
                    }
                  } catch (...) {
                    if (url.tls) {
                      if (pool->connecting_tls > 0) {
                        --pool->connecting_tls;
                      }
                      if (pool->active_tls > 0) {
                        --pool->active_tls;
                      }
                      self->wake_one(pool->wait_tls);
                    } else {
                      if (pool->connecting_plain > 0) {
                        --pool->connecting_plain;
                      }
                      if (pool->active_plain > 0) {
                        --pool->active_plain;
                      }
                      self->wake_one(pool->wait_plain);
                    }
                    co_return;
                  }
                }
              },
              [](std::size_t, std::monostate) {});
          co_return;
          },
          asio::use_awaitable);
    }
  }

  void submit(Request request, AsioHttpClient::ResponseHandler handler) {
    auto start = std::chrono::steady_clock::now();
    auto inflight = std::make_shared<InflightGuard>(this);
    ParsedUrl url;
    try {
      url = parse_url(request.url);
    } catch (const std::exception& e) {
      Response response;
      response.error = e.what();
      if (request.measure_total_time) {
        response.total_time_sec =
            std::chrono::duration<double>(std::chrono::steady_clock::now() - start)
                .count();
      }
      handler(std::move(response));
      return;
    }

    auto& shard = pick_request_shard(url);
    auto self = shared_from_this();
    asio::co_spawn(
        shard.io,
        [self, shard = &shard, url = std::move(url), request = std::move(request), start,
         handler = std::move(handler), inflight = std::move(inflight)]() mutable
            -> asio::awaitable<void> {
          auto pool = self->get_origin_pool(*shard, url);
          Response response;
          try {
            if (self->options_.use_h1_connection_actor && url.tls) {
              response =
                  co_await self->run_h1_actor_request(pool, url, std::move(request),
                                                      start);
            } else {
              response =
                  co_await self->run_pooled_request(pool, url, std::move(request),
                                                    start);
            }
          } catch (const std::exception& e) {
            response.error = e.what();
            if (request.measure_total_time) {
              response.total_time_sec =
                  std::chrono::duration<double>(std::chrono::steady_clock::now() -
                                                start)
                      .count();
            }
          }
          (void)inflight;
          handler(std::move(response));
        },
        asio::detached);
  }

  Options options_;
  std::vector<std::unique_ptr<Shard>> shards_;
  std::atomic<std::size_t> next_request_shard_{0};
  std::atomic<std::size_t> active_auto_shards_{1};
  std::atomic<std::size_t> global_inflight_{0};
  std::mutex auto_scale_mu_;
  std::chrono::steady_clock::time_point last_auto_scale_up_{};
  std::chrono::steady_clock::time_point last_auto_busy_{
      std::chrono::steady_clock::now()};
  Counters stats_;
};

AsioHttpClient::AsioHttpClient() : AsioHttpClient(Options{}) {}

AsioHttpClient::AsioHttpClient(Options options)
    : impl_(std::make_shared<Impl>(options)) {
  impl_->start_maintenance();
}

AsioHttpClient::~AsioHttpClient() = default;

asio::awaitable<Response> AsioHttpClient::async_request(Request request) {
  return impl_->request(std::move(request));
}

void AsioHttpClient::async_request_callback(Request request,
                                            ResponseHandler handler) {
  impl_->submit(std::move(request), std::move(handler));
}

asio::awaitable<AsioHttpClient::ProbeResult> AsioHttpClient::async_probe(
    Request request) {
  return impl_->probe_request(std::move(request));
}

asio::awaitable<void> AsioHttpClient::preconnect(Request request,
                                                 std::size_t count) {
  co_return co_await impl_->preconnect(std::move(request), count);
}

AsioHttpClient::Stats AsioHttpClient::stats() const {
  return AsioHttpClient::Stats{
      impl_->stats_.h1_conn_created.load(),
      impl_->stats_.h1_idle_hit.load(),
      impl_->stats_.h1_idle_miss.load(),
      impl_->stats_.h1_conn_reused.load(),
      impl_->stats_.h1_return_to_idle.load(),
      impl_->stats_.h1_close_after_response.load(),
      impl_->stats_.h1_reuse_failed.load(),
      impl_->stats_.h1_reconnect_after_idle.load(),
      impl_->stats_.h1_cancelled.load(),
      impl_->stats_.h1_pool_wait_cancelled.load(),
      impl_->stats_.h1_close_on_cancel.load(),
      Impl::snapshot_timing(impl_->stats_.h1_pool_wait),
      Impl::snapshot_timing(impl_->stats_.h1_connect),
      Impl::snapshot_timing(impl_->stats_.h1_acquire),
      Impl::snapshot_timing(impl_->stats_.h1_write),
      Impl::snapshot_timing(impl_->stats_.h1_read_headers),
      Impl::snapshot_timing(impl_->stats_.h1_read_body),
      Impl::snapshot_timing(impl_->stats_.h1_exchange),
  };
}

void AsioHttpClient::reset_stats() {
  impl_->reset_stats();
}

asio::awaitable<void> AsioHttpClient::reset_connections() {
  co_return co_await impl_->reset_connections();
}

}  // namespace httpclient
