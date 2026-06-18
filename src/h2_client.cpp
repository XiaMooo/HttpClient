#include "httpclient/h2_client.hpp"
#include "asyncx/asyncx.hpp"
#include "proxy_support.hpp"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/beast/core/tcp_stream.hpp>
#include <boost/system/error_code.hpp>
#include <nghttp2/nghttp2.h>
#include <openssl/err.h>
#include <openssl/ssl.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cctype>
#include <cstring>
#include <deque>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

namespace httpclient {
namespace asio = boost::asio;
namespace ssl = boost::asio::ssl;
using tcp = asio::ip::tcp;

namespace {

struct ParsedUrl {
  std::string scheme;
  std::string host;
  std::string port;
  std::string target;
};

ParsedUrl parse_url(const std::string& url) {
  auto scheme_end = url.find("://");
  if (scheme_end == std::string::npos) {
    throw std::invalid_argument("url must include scheme");
  }
  ParsedUrl out;
  out.scheme = url.substr(0, scheme_end);
  if (out.scheme != "https") {
    throw std::invalid_argument("h2 client currently supports https only");
  }
  auto rest_start = scheme_end + 3;
  auto path_start = url.find('/', rest_start);
  std::string authority =
      path_start == std::string::npos ? url.substr(rest_start)
                                      : url.substr(rest_start, path_start - rest_start);
  out.target = path_start == std::string::npos ? "/" : url.substr(path_start);
  auto colon = authority.rfind(':');
  if (colon == std::string::npos) {
    out.host = authority;
    out.port = "443";
  } else {
    out.host = authority.substr(0, colon);
    out.port = authority.substr(colon + 1);
  }
  return out;
}

nghttp2_nv nv(const char* name, std::string_view value) {
  return nghttp2_nv{
      reinterpret_cast<uint8_t*>(const_cast<char*>(name)),
      reinterpret_cast<uint8_t*>(const_cast<char*>(value.data())),
      std::strlen(name),
      value.size(),
      NGHTTP2_NV_FLAG_NONE,
  };
}

bool header_name_equals(const uint8_t* name, size_t len, std::string_view expected) {
  return len == expected.size() &&
         std::memcmp(name, expected.data(), expected.size()) == 0;
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

std::pair<std::string_view, std::string_view> split_header_view(
    std::string_view header) {
  auto pos = header.find(':');
  if (pos == std::string_view::npos) {
    return {header, std::string_view{}};
  }
  auto name = header.substr(0, pos);
  auto value = header.substr(pos + 1);
  while (!value.empty() && std::isspace(static_cast<unsigned char>(value.front()))) {
    value.remove_prefix(1);
  }
  return {name, value};
}

bool has_uppercase_ascii(std::string_view value) {
  for (char ch : value) {
    if (ch >= 'A' && ch <= 'Z') {
      return true;
    }
  }
  return false;
}

struct H2HeaderView {
  std::string_view name;
  std::string_view value;
  std::string lower_name;
};

bool h2_skip_header(std::string_view name) {
  return name.empty() || name.front() == ':' || header_name_equals(name, "host") ||
         header_name_equals(name, "connection") ||
         header_name_equals(name, "keep-alive") ||
         header_name_equals(name, "proxy-connection") ||
         header_name_equals(name, "transfer-encoding") ||
         header_name_equals(name, "upgrade") ||
         header_name_equals(name, "content-length");
}

std::string transport_key(const ParsedUrl& url,
                          const proxy_support::EffectiveProxy& proxy) {
  std::string key = url.scheme + "://" + url.host + ":" + url.port;
  if (!proxy.enabled) {
    key.append("|direct");
    return key;
  }
  key.append("|proxy=");
  key.append(proxy.url.scheme);
  key.append("://");
  key.append(proxy.url.username);
  key.push_back('@');
  key.append(proxy.url.host);
  key.push_back(':');
  key.append(proxy.url.port);
  if (!proxy.authorization.empty()) {
    key.append("|auth=");
    key.append(std::to_string(std::hash<std::string>{}(proxy.authorization)));
  } else if (!proxy.url.password.empty()) {
    key.append("|auth=socks5:");
    key.append(std::to_string(std::hash<std::string>{}(proxy.url.password)));
  }
  return key;
}

long parse_status(const uint8_t* value, size_t len) {
  long status = 0;
  for (size_t i = 0; i < len; ++i) {
    auto c = value[i];
    if (c < '0' || c > '9') {
      break;
    }
    status = status * 10 + static_cast<long>(c - '0');
  }
  return status;
}

}  // namespace

struct H2Client::Impl : std::enable_shared_from_this<Impl> {
  using Stream = ssl::stream<tcp::socket>;
  using NestedStream = ssl::stream<ssl::stream<tcp::socket>>;

  explicit Impl(asio::io_context& io, Options options)
      : io_(io),
        strand_(asio::make_strand(io)),
        ssl_ctx_(ssl::context::tls_client),
        deadline_timer_(strand_),
        options_(options) {
    stream_limit_snapshot_.store(stream_limit(), std::memory_order_relaxed);
  }

  ~Impl() {
    shutdown_now();
    if (session_) {
      nghttp2_session_del(session_);
    }
    if (callbacks_) {
      nghttp2_session_callbacks_del(callbacks_);
    }
  }

  struct Counters {
    std::atomic<std::uint64_t> streams_submitted{0};
    std::atomic<std::uint64_t> streams_completed{0};
    std::atomic<std::uint64_t> streams_timed_out{0};
    std::atomic<std::uint64_t> streams_cancelled{0};
    std::atomic<std::uint64_t> stream_slot_waits{0};
    std::atomic<std::uint64_t> stream_slot_wait_cancelled{0};
    std::atomic<std::uint64_t> connect_waits{0};
    std::atomic<std::uint64_t> connect_wait_cancelled{0};
    std::atomic<std::uint64_t> preconnect_attempts{0};
    std::atomic<std::uint64_t> preconnect_success{0};
    std::atomic<std::uint64_t> preconnect_failed{0};
    std::atomic<std::uint64_t> max_active_streams{0};
    std::atomic<std::uint64_t> max_pending_stream_waiters{0};
  };

  static void update_max(std::atomic<std::uint64_t>& target, std::uint64_t value) {
    auto current = target.load(std::memory_order_relaxed);
    while (current < value &&
           !target.compare_exchange_weak(current, value, std::memory_order_relaxed)) {}
  }

  void reset_stats() {
    stats_.streams_submitted.store(0, std::memory_order_relaxed);
    stats_.streams_completed.store(0, std::memory_order_relaxed);
    stats_.streams_timed_out.store(0, std::memory_order_relaxed);
    stats_.streams_cancelled.store(0, std::memory_order_relaxed);
    stats_.stream_slot_waits.store(0, std::memory_order_relaxed);
    stats_.stream_slot_wait_cancelled.store(0, std::memory_order_relaxed);
    stats_.connect_waits.store(0, std::memory_order_relaxed);
    stats_.connect_wait_cancelled.store(0, std::memory_order_relaxed);
    stats_.preconnect_attempts.store(0, std::memory_order_relaxed);
    stats_.preconnect_success.store(0, std::memory_order_relaxed);
    stats_.preconnect_failed.store(0, std::memory_order_relaxed);
    stats_.max_active_streams.store(0, std::memory_order_relaxed);
    stats_.max_pending_stream_waiters.store(0, std::memory_order_relaxed);
  }

  static std::chrono::milliseconds effective_timeout(
      const Request& request, long Request::Timeout::*field) {
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

  static void set_h2_alpn(SSL* handle) {
    static const unsigned char alpn[] = {2, 'h', '2'};
    SSL_set_alpn_protos(handle, alpn, sizeof(alpn));
  }

  struct StreamState {
    struct BodySource {
      std::string body;
      std::size_t offset = 0;
    };

    Response response;
    bool done = false;
    bool store_body = true;
    bool store_headers = true;
    BodyChunkHandler on_body_chunk;
    bool callback_mode = false;
    BodySource body_source;
    bool has_body_source = false;
    std::chrono::steady_clock::time_point deadline{};
    std::chrono::steady_clock::time_point write_deadline{};
    std::chrono::steady_clock::time_point read_deadline{};
    std::chrono::milliseconds write_timeout{0};
    std::chrono::milliseconds read_timeout{0};
    bool response_started = false;
    std::chrono::steady_clock::time_point start{};
    H2Client::ResponseHandler handler;

    void reset_for_reuse() {
      response.status = 0;
      response.body.clear();
      response.headers.clear();
      response.error.clear();
      response.final_url.clear();
      response.redirect_count = 0;
      response.total_time_sec = 0.0;
      response.namelookup_time_sec = 0.0;
      response.connect_time_sec = 0.0;
      response.appconnect_time_sec = 0.0;
      response.pretransfer_time_sec = 0.0;
      response.starttransfer_time_sec = 0.0;
      response.num_connects = 0;
      response.http_version = 0;
      response.reused_connection = false;
      response.primary_ip.clear();
      done = false;
      store_body = true;
      store_headers = true;
      on_body_chunk = BodyChunkHandler{};
      callback_mode = false;
      body_source.body.clear();
      body_source.offset = 0;
      has_body_source = false;
      deadline = {};
      write_deadline = {};
      read_deadline = {};
      write_timeout = std::chrono::milliseconds{0};
      read_timeout = std::chrono::milliseconds{0};
      response_started = false;
      start = {};
      handler = H2Client::ResponseHandler{};
    }
  };

  enum class DeadlineKind : unsigned char {
    Total,
    Write,
    Read,
  };

  struct StreamEntry {
    int32_t id = 0;
    bool in_use = false;
    StreamState state;
  };

  struct StreamIndexEntry {
    int32_t stream_id = 0;
    std::size_t index = 0;
  };

  struct DeadlineEntry {
    std::chrono::steady_clock::time_point deadline{};
    int32_t stream_id = 0;
    DeadlineKind kind = DeadlineKind::Total;

    bool operator>(const DeadlineEntry& other) const {
      return deadline > other.deadline;
    }
  };

  struct StreamSlot {
    Impl* self = nullptr;
    bool active = false;

    StreamSlot() = default;
    StreamSlot(Impl* owner, bool armed) : self(owner), active(armed) {}
    StreamSlot(const StreamSlot&) = delete;
    StreamSlot& operator=(const StreamSlot&) = delete;
    StreamSlot(StreamSlot&& other) noexcept : self(other.self), active(other.active) {
      other.self = nullptr;
      other.active = false;
    }
    StreamSlot& operator=(StreamSlot&& other) noexcept {
      if (this != &other) {
        release();
        self = other.self;
        active = other.active;
        other.self = nullptr;
        other.active = false;
      }
      return *this;
    }
    ~StreamSlot() { release(); }
    void dismiss() { active = false; }
    void release();
  };

  struct ConnectingGuard {
    Impl& self;
    bool armed = true;
    ~ConnectingGuard() {
      if (armed) {
        self.connecting_ = false;
        self.connecting_snapshot_.store(false, std::memory_order_relaxed);
        self.notify_connect_waiters();
      }
    }
    void dismiss() { armed = false; }
  };

  struct Waiter {
    explicit Waiter(asio::any_io_executor executor) : timer(std::move(executor)) {}
    asio::steady_timer timer;
    bool woken = false;
    bool cancelled = false;
  };

  using WaiterPtr = std::shared_ptr<Waiter>;
  using WaiterQueue = std::deque<WaiterPtr>;

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

  std::size_t state_waiter_pool_limit() const {
    return std::max<std::size_t>(64, options_.max_concurrent_streams * 2);
  }

  WaiterPtr acquire_state_waiter() {
    if (!state_waiter_pool_.empty()) {
      auto waiter = std::move(state_waiter_pool_.back());
      state_waiter_pool_.pop_back();
      waiter->woken = false;
      waiter->cancelled = false;
      waiter->timer.cancel();
      waiter->timer.expires_at(asio::steady_timer::time_point::max());
      return waiter;
    }
    auto waiter = std::make_shared<Waiter>(strand_);
    waiter->timer.expires_at(asio::steady_timer::time_point::max());
    return waiter;
  }

  void release_state_waiter(WaiterPtr waiter) {
    if (!waiter) {
      return;
    }
    waiter->woken = false;
    waiter->cancelled = false;
    waiter->timer.cancel();
    waiter->timer.expires_at(asio::steady_timer::time_point::max());
    if (state_waiter_pool_.size() < state_waiter_pool_limit()) {
      state_waiter_pool_.push_back(std::move(waiter));
    }
  }

  asio::awaitable<void> ensure_connected(const ParsedUrl& url, bool insecure,
                                         const Request& request) {
    auto proxy = proxy_support::proxy_for_request(request);
    if (connected_) {
      co_return;
    }
    if (connecting_) {
      ++stats_.connect_waits;
      auto waiter = std::make_shared<Waiter>(strand_);
      waiter->timer.expires_at(asio::steady_timer::time_point::max());
      connect_waiters_.push_back(waiter);
      boost::system::error_code ec;
      co_await waiter->timer.async_wait(asio::redirect_error(asio::use_awaitable, ec));
      if (ec == asio::error::operation_aborted && !waiter->woken) {
        waiter->cancelled = true;
        remove_waiter(connect_waiters_, waiter);
        ++stats_.connect_wait_cancelled;
        throw std::runtime_error("h2 connect wait cancelled");
      }
      if (connected_) {
        co_return;
      }
      if (!connect_error_.empty()) {
        throw std::runtime_error(connect_error_);
      }
      throw std::runtime_error("h2 connect failed");
    }
    if (goaway_received_) {
      close_connection("h2 goaway");
    }
    if (connected_) {
      co_return;
    }
    try {
      connect_error_.clear();
      ++io_generation_;
      connecting_ = true;
      connecting_snapshot_.store(true, std::memory_order_relaxed);
      ConnectingGuard guard{*this};
      insecure_ = insecure;
      if (!callbacks_) {
        nghttp2_session_callbacks_new(&callbacks_);
        nghttp2_session_callbacks_set_on_header_callback(callbacks_, &Impl::on_header);
        nghttp2_session_callbacks_set_on_data_chunk_recv_callback(callbacks_,
                                                                  &Impl::on_data);
        nghttp2_session_callbacks_set_on_frame_recv_callback(callbacks_, &Impl::on_frame);
      }
      if (!session_) {
        nghttp2_session_client_new(&session_, callbacks_, this);
        const auto stream_capacity =
            std::max<std::size_t>(32, options_.max_concurrent_streams +
                                          options_.max_concurrent_streams / 4 + 16);
        streams_.reserve(stream_capacity);
        free_stream_indices_.reserve(stream_capacity);
        stream_index_.reserve(stream_capacity);
        state_waiters_.reserve(stream_capacity);
      }

      if (options_.verify_tls && !insecure) {
        ssl_ctx_.set_default_verify_paths();
      } else {
        ssl_ctx_.set_verify_mode(ssl::verify_none);
      }

      tcp::resolver resolver(strand_);
      const auto& connect_host = proxy.enabled ? proxy.url.host : url.host;
      const auto& connect_port = proxy.enabled ? proxy.url.port : url.port;
      auto endpoints = co_await resolver.async_resolve(connect_host, connect_port,
                                                       asio::use_awaitable);
      if (proxy.enabled && proxy.scheme == proxy_support::Scheme::Https) {
        nested_stream_ =
            std::make_unique<NestedStream>(Stream(strand_, ssl_ctx_), ssl_ctx_);
        if (insecure || !options_.verify_tls) {
          nested_stream_->next_layer().set_verify_mode(ssl::verify_none);
          nested_stream_->set_verify_mode(ssl::verify_none);
        }
        if (!SSL_set_tlsext_host_name(
                nested_stream_->next_layer().native_handle(),
                proxy.url.host.c_str())) {
          throw std::runtime_error("HTTPS proxy SNI setup failed");
        }
        if (!SSL_set_tlsext_host_name(nested_stream_->native_handle(),
                                      url.host.c_str())) {
          throw std::runtime_error("SNI setup failed");
        }
        co_await asio::async_connect(nested_stream_->next_layer().next_layer(),
                                     endpoints, asio::use_awaitable);
        boost::system::error_code option_ec;
        nested_stream_->next_layer().next_layer().set_option(tcp::no_delay(true),
                                                             option_ec);
        co_await nested_stream_->next_layer().async_handshake(
            ssl::stream_base::client, asio::use_awaitable);
        co_await proxy_support::establish_http_connect_tunnel(
            nested_stream_->next_layer(), url.host, url.port, proxy.authorization);
        set_h2_alpn(nested_stream_->native_handle());
        co_await nested_stream_->async_handshake(ssl::stream_base::client,
                                                 asio::use_awaitable);
      } else {
        stream_ = std::make_unique<Stream>(strand_, ssl_ctx_);
        if (insecure || !options_.verify_tls) {
          stream_->set_verify_mode(ssl::verify_none);
        }
        if (!SSL_set_tlsext_host_name(stream_->native_handle(), url.host.c_str())) {
          throw std::runtime_error("SNI setup failed");
        }
        set_h2_alpn(stream_->native_handle());
        co_await asio::async_connect(stream_->next_layer(), endpoints,
                                     asio::use_awaitable);
        boost::system::error_code option_ec;
        stream_->next_layer().set_option(tcp::no_delay(true), option_ec);
        if (proxy.enabled) {
          if (proxy.scheme == proxy_support::Scheme::Socks5) {
            co_await proxy_support::establish_socks5_tunnel(
                stream_->next_layer(), url.host, url.port, proxy.url);
          } else {
            co_await proxy_support::establish_http_connect_tunnel(
                stream_->next_layer(), url.host, url.port, proxy.authorization);
          }
        }
        co_await stream_->async_handshake(ssl::stream_base::client,
                                          asio::use_awaitable);
      }

      const unsigned char* selected = nullptr;
      unsigned int selected_len = 0;
      SSL_get0_alpn_selected(active_native_handle(), &selected, &selected_len);
      if (selected_len != 2 || std::memcmp(selected, "h2", 2) != 0) {
        throw std::runtime_error("server did not negotiate h2");
      }

      nghttp2_settings_entry iv[] = {
          {NGHTTP2_SETTINGS_ENABLE_PUSH, 0},
          {NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS, 1000},
      };
      nghttp2_submit_settings(session_, NGHTTP2_FLAG_NONE, iv, 2);
      pump_output();
      connected_ = true;
      connecting_ = false;
      connected_snapshot_.store(true, std::memory_order_relaxed);
      connecting_snapshot_.store(false, std::memory_order_relaxed);
      guard.dismiss();
      notify_connect_waiters();

      auto generation = io_generation_;
      reading_ = true;
      auto self = shared_from_this();
      asio::co_spawn(
          strand_,
          [self, generation]() -> asio::awaitable<void> {
            co_await self->read_loop(generation);
          },
          asio::detached);
      } catch (const std::exception& e) {
      connect_error_ = e.what();
      close_connection(connect_error_);
      connecting_ = false;
      connecting_snapshot_.store(false, std::memory_order_relaxed);
      if (connect_error_ == "Operation canceled" ||
          connect_error_ == "operation_aborted") {
        ++stats_.connect_wait_cancelled;
      }
      notify_connect_waiters();
      throw;
    }
  }

  asio::awaitable<Response> get(std::string url_text, bool insecure) {
    Request request;
    request.url = std::move(url_text);
    request.method = "GET";
    co_return co_await async_request(std::move(request), insecure);
  }

  asio::awaitable<Response> async_request(Request request, bool insecure) {
    auto self = shared_from_this();
    co_await asio::this_coro::reset_cancellation_state(
        asio::enable_total_cancellation());
    auto cancel_state = co_await asio::this_coro::cancellation_state;
    co_return co_await asio::co_spawn(
        strand_, self->request_on_strand(std::move(request), insecure),
        asio::bind_cancellation_slot(cancel_state.slot(), asio::use_awaitable));
  }

  void async_request_callback(Request request, H2Client::ResponseHandler handler,
                              bool insecure) {
    auto self = shared_from_this();
    asio::co_spawn(
        strand_,
        [self, request = std::move(request), handler = std::move(handler), insecure]()
            -> asio::awaitable<void> {
          auto start = std::chrono::steady_clock::now();
          try {
            auto url = parse_url(request.url);
            co_await self->ensure_connected(url, insecure, request);
            auto slot = co_await self->acquire_stream_slot(
                effective_timeout(request, &Request::Timeout::pool_ms));
            auto stream_id = self->submit_stream(std::move(request), url, start);
            auto* stream = self->find_stream(stream_id);
            if (stream == nullptr) {
              throw std::runtime_error("h2 stream state missing");
            }
            stream->callback_mode = true;
            stream->handler = std::move(handler);
            slot.dismiss();
          } catch (const std::exception& e) {
            Response response;
            response.error = e.what();
            response.total_time_sec =
                std::chrono::duration<double>(std::chrono::steady_clock::now() -
                                              start)
                    .count();
            handler(std::move(response));
          }
        },
        asio::detached);
  }

  void preconnect(Request request, bool insecure) {
    auto self = shared_from_this();
    asio::co_spawn(
        strand_,
        [self, request = std::move(request), insecure]() mutable
            -> asio::awaitable<void> {
          try {
            auto url = parse_url(request.url);
            ++self->stats_.preconnect_attempts;
            co_await self->ensure_connected(url, insecure, request);
            ++self->stats_.preconnect_success;
          } catch (...) {
            ++self->stats_.preconnect_failed;
          }
          co_return;
        },
        asio::detached);
  }

  asio::awaitable<void> preconnect_wait(Request request, bool insecure) {
    auto self = shared_from_this();
    co_await asio::co_spawn(
        strand_,
        [self, request = std::move(request), insecure]() mutable
            -> asio::awaitable<void> {
          auto url = parse_url(request.url);
          ++self->stats_.preconnect_attempts;
          try {
            co_await self->ensure_connected(url, insecure, request);
            ++self->stats_.preconnect_success;
          } catch (...) {
            ++self->stats_.preconnect_failed;
          }
          co_return;
        },
        asio::use_awaitable);
  }

  asio::awaitable<Response> request_on_strand(Request request, bool insecure) {
    auto start = std::chrono::steady_clock::now();
    try {
      auto url = parse_url(request.url);
      co_await ensure_connected(url, insecure, request);
      auto slot = co_await acquire_stream_slot(
          effective_timeout(request, &Request::Timeout::pool_ms));
      auto stream_id = submit_stream(std::move(request), url, start);
      auto* state = find_stream(stream_id);
      if (state == nullptr) {
        throw std::runtime_error("h2 stream state missing");
      }

      if (!state->done) {
        auto waiter = acquire_state_waiter();
        state_waiters_[stream_id] = waiter;
        boost::system::error_code ec;
        co_await waiter->timer.async_wait(asio::redirect_error(asio::use_awaitable, ec));
        state_waiters_.erase(stream_id);
        if (ec == asio::error::operation_aborted && !waiter->woken) {
          cancel_stream(stream_id, "h2 stream cancelled");
          slot.release();
          release_state_waiter(std::move(waiter));
          throw std::runtime_error("h2 stream cancelled");
        }
        release_state_waiter(std::move(waiter));
      }
      state = find_stream(stream_id);
      if (state == nullptr || !state->done) {
        cancel_stream(stream_id, "h2 stream timeout");
        slot.release();
        throw std::runtime_error("h2 stream timeout");
      }

      auto response = std::move(state->response);
      response.total_time_sec =
          std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
      response.http_version = 3;
      erase_stream(stream_id);
      slot.release();
      ++stats_.streams_completed;
      co_return response;
    } catch (const std::exception& e) {
      Response response;
      response.error = e.what();
      response.total_time_sec =
          std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
      co_return response;
    }
  }

  int32_t submit_stream(Request request, const ParsedUrl& url,
                        std::chrono::steady_clock::time_point start) {
    if (goaway_received_) {
      close_connection("h2 goaway");
      throw std::runtime_error("h2 goaway retry");
    }
    std::string content_length;
    const std::string_view method =
        request.method.empty() ? std::string_view("GET")
                               : std::string_view(request.method);
    if (!request.body.empty()) {
      content_length = std::to_string(request.body.size());
    }

    std::vector<H2HeaderView> extra_headers;
    extra_headers.reserve(request.headers.size());
    bool has_accept = false;
    for (const auto& header : request.headers) {
      auto [name, value] = split_header_view(header);
      if (h2_skip_header(name)) {
        continue;
      }
      has_accept = has_accept || header_name_equals(name, "accept");
      auto& entry = extra_headers.emplace_back();
      entry.name = name;
      entry.value = value;
      if (has_uppercase_ascii(entry.name)) {
        entry.lower_name.assign(entry.name);
        std::transform(entry.lower_name.begin(), entry.lower_name.end(),
                       entry.lower_name.begin(), [](unsigned char c) {
                         return static_cast<char>(std::tolower(c));
                       });
        entry.name = entry.lower_name;
      }
    }

    const auto max_header_count = 6 + extra_headers.size();
    std::array<nghttp2_nv, 16> stack_headers{};
    std::vector<nghttp2_nv> heap_headers;
    if (max_header_count > stack_headers.size()) {
      heap_headers.reserve(max_header_count);
    }
    std::size_t header_count = 0;
    auto push_header = [&](nghttp2_nv header) {
      if (max_header_count <= stack_headers.size()) {
        stack_headers[header_count++] = header;
      } else {
        heap_headers.push_back(header);
      }
    };
    push_header(nv(":method", method));
    push_header(nv(":scheme", url.scheme));
    push_header(nv(":path", url.target));
    push_header(nv(":authority", url.host));
    if (!has_accept) {
      push_header(nv("accept", "*/*"));
    }
    if (!request.body.empty()) {
      push_header(nv("content-length", content_length));
    }
    for (const auto& header : extra_headers) {
      push_header(nghttp2_nv{
          reinterpret_cast<uint8_t*>(const_cast<char*>(header.name.data())),
          reinterpret_cast<uint8_t*>(const_cast<char*>(header.value.data())),
          header.name.size(),
          header.value.size(),
          NGHTTP2_NV_FLAG_NONE,
      });
    }
    auto* headers = max_header_count <= stack_headers.size() ? stack_headers.data()
                                                             : heap_headers.data();
    auto header_size = max_header_count <= stack_headers.size()
                           ? header_count
                           : heap_headers.size();

    const bool has_body = !request.body.empty();
    int32_t expected_stream_id = nghttp2_session_get_next_stream_id(session_);
    auto* state = &acquire_stream_entry(expected_stream_id).state;
    if (has_body) {
      state->body_source.body = std::move(request.body);
      state->body_source.offset = 0;
      state->has_body_source = true;
    }
    nghttp2_data_provider data_provider{};
    nghttp2_data_provider* provider = nullptr;
    if (has_body) {
      data_provider.source.ptr = &state->body_source;
      data_provider.read_callback = &Impl::on_data_source_read;
      provider = &data_provider;
    }

    int32_t stream_id =
        nghttp2_submit_request(session_, nullptr, headers, header_size, provider,
                               nullptr);
    if (stream_id < 0) {
      release_stream_entry(expected_stream_id);
      throw std::runtime_error(nghttp2_strerror(stream_id));
    }
    if (stream_id != expected_stream_id) {
      auto* entry = find_stream_entry(expected_stream_id);
      if (entry != nullptr) {
        erase_stream_index(expected_stream_id);
        entry->id = stream_id;
        stream_index_.push_back(
            StreamIndexEntry{stream_id,
                             static_cast<std::size_t>(entry - streams_.data())});
        state = &entry->state;
      } else {
        state = &acquire_stream_entry(stream_id).state;
      }
    }
    ++stats_.streams_submitted;
    state->store_body = request.store_response_body;
    state->store_headers = request.store_response_headers;
    if (state->store_headers && state->response.headers.capacity() < 8) {
      state->response.headers.reserve(8);
    }
    state->on_body_chunk = std::move(request.on_body_chunk);
    const auto now = std::chrono::steady_clock::now();
    auto total_timeout =
        effective_timeout(request, &Request::Timeout::total_ms);
    auto write_timeout =
        effective_timeout(request, &Request::Timeout::write_ms);
    auto read_timeout =
        effective_timeout(request, &Request::Timeout::read_ms);
    state->deadline = now + total_timeout;
    state->write_timeout = write_timeout;
    state->read_timeout = read_timeout;
    state->write_deadline = now + write_timeout;
    state->read_deadline = std::chrono::steady_clock::time_point::max();
    state->start = start;
    add_deadline(stream_id, DeadlineKind::Total, state->deadline);
    add_deadline(stream_id, DeadlineKind::Write, state->write_deadline);
    pump_output();
    arm_deadline_timer();
    return stream_id;
  }

  void complete_stream(int32_t stream_id) {
    auto* entry = find_stream_entry(stream_id);
    if (entry == nullptr) {
      return;
    }
    entry->state.done = true;
    if (!entry->state.callback_mode) {
      auto waiter = state_waiters_.find(stream_id);
      if (waiter != state_waiters_.end() && waiter->second) {
        wake_waiter(waiter->second);
      }
      return;
    }

    auto response = std::move(entry->state.response);
    response.total_time_sec =
        std::chrono::duration<double>(std::chrono::steady_clock::now() -
                                      entry->state.start)
            .count();
    response.http_version = 3;
    auto handler = std::move(entry->state.handler);
    release_stream_entry(stream_id);
    release_stream_slot();
    ++stats_.streams_completed;
    if (handler) {
      handler(std::move(response));
    }
  }

  void cancel_stream(int32_t stream_id, const char* reason) {
    auto* stream = find_stream(stream_id);
    if (stream == nullptr) {
      return;
    }
    stream->response.error = reason;
    stream->done = true;
    ++stats_.streams_cancelled;
    nghttp2_submit_rst_stream(session_, NGHTTP2_FLAG_NONE, stream_id,
                              NGHTTP2_CANCEL);
    auto waiter = state_waiters_.find(stream_id);
    if (waiter != state_waiters_.end() && waiter->second) {
      wake_waiter(waiter->second);
    }
    erase_stream(stream_id);
    pump_output();
  }

  std::size_t stream_limit() const {
    auto configured = std::max<std::size_t>(1, options_.max_concurrent_streams);
    auto peer = std::max<std::size_t>(1, peer_max_concurrent_streams_);
    return std::min(configured, peer);
  }

  std::size_t available_stream_slots_snapshot() const {
    if (!connected_snapshot_.load(std::memory_order_relaxed) ||
        stopping_snapshot_.load(std::memory_order_relaxed)) {
      return 0;
    }
    auto limit = stream_limit_snapshot_.load(std::memory_order_relaxed);
    auto active = active_streams_snapshot_.load(std::memory_order_relaxed);
    return limit > active ? limit - active : 0;
  }

  bool connected_snapshot() const {
    return connected_snapshot_.load(std::memory_order_relaxed) &&
           !stopping_snapshot_.load(std::memory_order_relaxed);
  }

  bool connecting_snapshot() const {
    return connecting_snapshot_.load(std::memory_order_relaxed);
  }

  asio::awaitable<StreamSlot> acquire_stream_slot(
      std::chrono::milliseconds pool_timeout) {
    const auto deadline = std::chrono::steady_clock::now() + pool_timeout;
    for (;;) {
      if (stopping_) {
        throw std::runtime_error("h2 shutdown");
      }
      if (active_streams_ < stream_limit()) {
        ++active_streams_;
        active_streams_snapshot_.store(active_streams_, std::memory_order_relaxed);
        update_max(stats_.max_active_streams, active_streams_);
        co_return StreamSlot{this, true};
      }
      ++stats_.stream_slot_waits;
      auto waiter = std::make_shared<Waiter>(strand_);
      waiter->timer.expires_at(pool_timeout.count() > 0
                                   ? deadline
                                   : asio::steady_timer::time_point::max());
      stream_waiters_.push_back(waiter);
      update_max(stats_.max_pending_stream_waiters, stream_waiters_.size());
      boost::system::error_code ec;
      co_await waiter->timer.async_wait(asio::redirect_error(asio::use_awaitable, ec));
      if (ec == asio::error::operation_aborted && !waiter->woken) {
        waiter->cancelled = true;
        remove_waiter(stream_waiters_, waiter);
        ++stats_.stream_slot_wait_cancelled;
        throw std::runtime_error("h2 stream slot wait cancelled");
      }
      if (ec != asio::error::operation_aborted &&
          pool_timeout.count() > 0 &&
          std::chrono::steady_clock::now() >= deadline) {
        throw std::runtime_error("h2 stream slot timeout");
      }
    }
  }

  void release_stream_slot() {
    if (active_streams_ > 0) {
      --active_streams_;
    }
    active_streams_snapshot_.store(active_streams_, std::memory_order_relaxed);
    wake_stream_waiters();
  }

  void add_deadline(int32_t stream_id, DeadlineKind kind,
                    std::chrono::steady_clock::time_point deadline) {
    if (deadline == std::chrono::steady_clock::time_point::max()) {
      return;
    }
    deadline_heap_.push_back(DeadlineEntry{deadline, stream_id, kind});
    std::push_heap(deadline_heap_.begin(), deadline_heap_.end(),
                   std::greater<DeadlineEntry>{});
  }

  void wake_stream_waiters() {
    while (!stream_waiters_.empty() && active_streams_ < stream_limit()) {
      auto waiter = std::move(stream_waiters_.front());
      stream_waiters_.pop_front();
      if (waiter && !waiter->cancelled) {
        wake_waiter(waiter);
        return;
      }
    }
  }

  void arm_deadline_timer() {
    prune_deadline_heap();
    if (deadline_heap_.empty() || deadline_timer_armed_) {
      return;
    }
    deadline_timer_armed_ = true;
    deadline_timer_.expires_at(deadline_heap_.front().deadline);
    auto self = shared_from_this();
    deadline_timer_.async_wait(
        asio::bind_executor(strand_, [self](boost::system::error_code ec) {
          self->deadline_timer_armed_ = false;
          if (!ec) {
            self->expire_streams();
          }
          self->arm_deadline_timer();
        }));
  }

  void expire_streams() {
    auto now = std::chrono::steady_clock::now();
    bool expired_any = false;
    for (;;) {
      prune_deadline_heap();
      if (deadline_heap_.empty() || deadline_heap_.front().deadline > now) {
        break;
      }
      auto entry = deadline_heap_.front();
      std::pop_heap(deadline_heap_.begin(), deadline_heap_.end(),
                    std::greater<DeadlineEntry>{});
      deadline_heap_.pop_back();
      auto stream_id = entry.stream_id;
      auto* stream = find_stream(stream_id);
      if (stream == nullptr || stream->done) {
        continue;
      }
      const auto current_deadline = deadline_for(*stream, entry.kind);
      if (current_deadline != entry.deadline) {
        continue;
      }
      expired_any = true;
      stream->response.error = timeout_message(entry.kind);
      stream->done = true;
      ++stats_.streams_timed_out;
      nghttp2_submit_rst_stream(session_, NGHTTP2_FLAG_NONE, stream_id,
                                NGHTTP2_CANCEL);
      if (stream->callback_mode) {
        auto response = std::move(stream->response);
        response.total_time_sec =
            std::chrono::duration<double>(std::chrono::steady_clock::now() -
                                          stream->start)
                .count();
        response.http_version = 3;
        auto handler = std::move(stream->handler);
        release_stream_entry(stream_id);
        release_stream_slot();
        if (handler) {
          handler(std::move(response));
        }
        continue;
      }
      auto waiter = state_waiters_.find(stream_id);
      if (waiter != state_waiters_.end() && waiter->second) {
        wake_waiter(waiter->second);
      }
    }
    if (expired_any) {
      pump_output();
    }
  }

  void prune_deadline_heap() {
    while (!deadline_heap_.empty()) {
      auto& entry = deadline_heap_.front();
      auto* stream = find_stream(entry.stream_id);
      if (stream != nullptr && !stream->done) {
        const auto current_deadline = deadline_for(*stream, entry.kind);
        if (current_deadline == entry.deadline) {
          return;
        }
      }
      std::pop_heap(deadline_heap_.begin(), deadline_heap_.end(),
                    std::greater<DeadlineEntry>{});
      deadline_heap_.pop_back();
    }
  }

  static std::chrono::steady_clock::time_point deadline_for(
      const StreamState& state, DeadlineKind kind) {
    switch (kind) {
      case DeadlineKind::Total:
        return state.deadline;
      case DeadlineKind::Write:
        return state.write_deadline;
      case DeadlineKind::Read:
        return state.read_deadline;
    }
    return state.deadline;
  }

  static const char* timeout_message(DeadlineKind kind) {
    switch (kind) {
      case DeadlineKind::Total:
        return "h2 stream timeout";
      case DeadlineKind::Write:
        return "h2 write timeout";
      case DeadlineKind::Read:
        return "h2 read timeout";
    }
    return "h2 stream timeout";
  }

  void pump_output() {
    if (stopping_ || !has_active_stream()) {
      return;
    }
    const uint8_t* data = nullptr;
    for (;;) {
      ssize_t len = nghttp2_session_mem_send(session_, &data);
      if (len < 0) {
        throw std::runtime_error(nghttp2_strerror(static_cast<int>(len)));
      }
      if (len == 0) {
        break;
      }
      pending_write_buffer_.insert(pending_write_buffer_.end(), data, data + len);
    }
    if (!writing_ && !pending_write_buffer_.empty()) {
      writing_ = true;
      auto generation = io_generation_;
      auto self = shared_from_this();
      asio::co_spawn(
          strand_,
          [self, generation]() -> asio::awaitable<void> {
            co_await self->write_loop(generation);
          },
          asio::detached);
    }
  }

  asio::awaitable<void> write_loop(std::uint64_t generation) {
    try {
      while (!stopping_ && generation == io_generation_ && has_active_stream()) {
        if (active_write_buffer_.empty()) {
          active_write_buffer_.swap(pending_write_buffer_);
        }
        if (active_write_buffer_.empty()) {
          writing_ = false;
          co_return;
        }
        boost::system::error_code ec;
        co_await async_write_active(asio::buffer(active_write_buffer_), ec);
        if (ec) {
          if (generation != io_generation_ && ec == asio::error::operation_aborted) {
            writing_ = false;
            release_closed_streams();
            co_return;
          }
          throw boost::system::system_error(ec);
        }
        active_write_buffer_.clear();
      }
    } catch (...) {
      fail_all_streams("h2 write failed");
      shutdown_now();
    }
    writing_ = false;
    release_closed_streams();
  }

  asio::awaitable<void> read_loop(std::uint64_t generation) {
    std::array<uint8_t, 16384> buf{};
    try {
      for (;;) {
        if (stopping_ || generation != io_generation_ || !has_active_stream()) {
          break;
        }
        boost::system::error_code ec;
        std::size_t n = co_await async_read_some_active(asio::buffer(buf), ec);
        if (ec) {
          if (generation != io_generation_ && ec == asio::error::operation_aborted) {
            reading_ = false;
            release_closed_streams();
            co_return;
          }
          throw boost::system::system_error(ec);
        }
        ssize_t rv = nghttp2_session_mem_recv(session_, buf.data(), n);
        if (rv < 0) {
          break;
        }
        pump_output();
      }
    } catch (...) {
      fail_all_streams("h2 read failed");
      shutdown_now();
    }
    reading_ = false;
    release_closed_streams();
  }

  static int on_header(nghttp2_session*, const nghttp2_frame* frame,
                       const uint8_t* name, size_t namelen, const uint8_t* value,
                       size_t valuelen, uint8_t, void* user_data) {
    auto* self = static_cast<Impl*>(user_data);
    if (frame->hd.type != NGHTTP2_HEADERS ||
        frame->headers.cat != NGHTTP2_HCAT_RESPONSE) {
      return 0;
    }
    auto* stream = self->find_stream(frame->hd.stream_id);
    if (stream == nullptr) {
      return 0;
    }
    stream->response_started = true;
    stream->write_deadline = std::chrono::steady_clock::time_point::max();
    if (stream->read_timeout.count() > 0) {
      stream->read_deadline =
          std::chrono::steady_clock::now() + stream->read_timeout;
      self->add_deadline(frame->hd.stream_id, DeadlineKind::Read,
                         stream->read_deadline);
      self->arm_deadline_timer();
    }
    if (header_name_equals(name, namelen, ":status")) {
      stream->response.status = parse_status(value, valuelen);
    } else if (stream->store_headers) {
      std::string header;
      header.reserve(namelen + valuelen + 2);
      header.append(reinterpret_cast<const char*>(name), namelen);
      header.append(": ");
      header.append(reinterpret_cast<const char*>(value), valuelen);
      stream->response.headers.push_back(std::move(header));
    }
    return 0;
  }

  static int on_data(nghttp2_session*, uint8_t, int32_t stream_id,
                     const uint8_t* data, size_t len, void* user_data) {
    auto* self = static_cast<Impl*>(user_data);
    auto* stream = self->find_stream(stream_id);
    if (stream != nullptr) {
      stream->response_started = true;
      if (stream->read_timeout.count() > 0) {
        stream->read_deadline =
            std::chrono::steady_clock::now() + stream->read_timeout;
        self->add_deadline(stream_id, DeadlineKind::Read, stream->read_deadline);
        self->arm_deadline_timer();
      }
      if (stream->on_body_chunk && len > 0) {
        stream->on_body_chunk(std::string_view(
            reinterpret_cast<const char*>(data), len));
      }
      if (stream->store_body) {
        stream->response.body.append(reinterpret_cast<const char*>(data), len);
      }
    }
    return 0;
  }

  static ssize_t on_data_source_read(nghttp2_session*, int32_t, uint8_t* buf,
                                     size_t length, uint32_t* data_flags,
                                     nghttp2_data_source* source, void*) {
    auto* body = static_cast<StreamState::BodySource*>(source->ptr);
    if (body == nullptr) {
      *data_flags |= NGHTTP2_DATA_FLAG_EOF;
      return 0;
    }
    auto remaining = body->body.size() - body->offset;
    auto n = std::min(length, remaining);
    if (n > 0) {
      std::memcpy(buf, body->body.data() + body->offset, n);
      body->offset += n;
    }
    if (body->offset >= body->body.size()) {
      *data_flags |= NGHTTP2_DATA_FLAG_EOF;
    }
    return static_cast<ssize_t>(n);
  }

  static int on_frame(nghttp2_session*, const nghttp2_frame* frame, void* user_data) {
    auto* self = static_cast<Impl*>(user_data);
    if (frame->hd.type == NGHTTP2_SETTINGS &&
        (frame->hd.flags & NGHTTP2_FLAG_ACK) == 0) {
      for (std::size_t i = 0; i < frame->settings.niv; ++i) {
        const auto& setting = frame->settings.iv[i];
        if (setting.settings_id == NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS) {
          self->peer_max_concurrent_streams_ =
              std::max<std::size_t>(1, setting.value);
          self->stream_limit_snapshot_.store(self->stream_limit(),
                                             std::memory_order_relaxed);
          self->wake_stream_waiters();
        }
      }
    }
    if ((frame->hd.type == NGHTTP2_DATA || frame->hd.type == NGHTTP2_HEADERS) &&
        (frame->hd.flags & NGHTTP2_FLAG_END_STREAM)) {
      self->complete_stream(frame->hd.stream_id);
    } else if (frame->hd.type == NGHTTP2_RST_STREAM) {
      auto* stream = self->find_stream(frame->hd.stream_id);
      if (stream != nullptr) {
        stream->response.error = "h2 stream reset";
      }
      self->complete_stream(frame->hd.stream_id);
    } else if (frame->hd.type == NGHTTP2_GOAWAY) {
      self->goaway_received_ = true;
    }
    return 0;
  }

  void request_shutdown() {
    auto self = shared_from_this();
    asio::dispatch(strand_, [self] { self->shutdown_now(); });
  }

  void request_reset_if_idle() {
    auto self = shared_from_this();
    asio::dispatch(strand_, [self] {
      if (self->idle()) {
        self->reset_now();
      }
    });
  }

  asio::awaitable<void> reset() {
    co_await asio::co_spawn(
        strand_,
        [self = shared_from_this()]() -> asio::awaitable<void> {
          self->reset_now();
          co_return;
        },
        asio::use_awaitable);
  }

  void shutdown_now() {
    if (stopping_ && !has_active_stream()) {
      return;
    }
    stopping_ = true;
    stopping_snapshot_.store(true, std::memory_order_relaxed);
    close_active_stream();
    active_write_buffer_.clear();
    pending_write_buffer_.clear();
    writing_ = false;
    connected_ = false;
    connecting_ = false;
    connected_snapshot_.store(false, std::memory_order_relaxed);
    connecting_snapshot_.store(false, std::memory_order_relaxed);
    goaway_received_ = false;
    active_streams_ = 0;
    active_streams_snapshot_.store(0, std::memory_order_relaxed);
    deadline_heap_.clear();
    deadline_timer_.cancel();
    deadline_timer_armed_ = false;
    wake_all_stream_waiters();
    fail_all_streams("h2 shutdown");
  }

  void reset_now() {
    if (stopping_) {
      return;
    }
    ++io_generation_;
    close_connection("h2 reset");
  }

  void close_connection(const std::string& error) {
    close_active_stream();
    active_write_buffer_.clear();
    pending_write_buffer_.clear();
    writing_ = false;
    connected_ = false;
    connecting_ = false;
    connected_snapshot_.store(false, std::memory_order_relaxed);
    connecting_snapshot_.store(false, std::memory_order_relaxed);
    goaway_received_ = false;
    active_streams_ = 0;
    active_streams_snapshot_.store(0, std::memory_order_relaxed);
    deadline_heap_.clear();
    deadline_timer_.cancel();
    deadline_timer_armed_ = false;
    wake_all_stream_waiters();
    fail_all_streams(error);
    if (session_) {
      nghttp2_session_del(session_);
      session_ = nullptr;
    }
    peer_max_concurrent_streams_ = 100;
    stream_limit_snapshot_.store(stream_limit(), std::memory_order_relaxed);
  }

  bool has_active_stream() const {
    return stream_ != nullptr || nested_stream_ != nullptr;
  }

  SSL* active_native_handle() {
    if (nested_stream_) {
      return nested_stream_->native_handle();
    }
    if (stream_) {
      return stream_->native_handle();
    }
    return nullptr;
  }

  template <class ConstBufferSequence>
  asio::awaitable<void> async_write_active(const ConstBufferSequence& buffers,
                                           boost::system::error_code& ec) {
    if (nested_stream_) {
      co_await asio::async_write(
          *nested_stream_, buffers, asio::redirect_error(asio::use_awaitable, ec));
      co_return;
    }
    co_await asio::async_write(
        *stream_, buffers, asio::redirect_error(asio::use_awaitable, ec));
  }

  template <class MutableBufferSequence>
  asio::awaitable<std::size_t> async_read_some_active(
      const MutableBufferSequence& buffers, boost::system::error_code& ec) {
    if (nested_stream_) {
      co_return co_await nested_stream_->async_read_some(
          buffers, asio::redirect_error(asio::use_awaitable, ec));
    }
    co_return co_await stream_->async_read_some(
        buffers, asio::redirect_error(asio::use_awaitable, ec));
  }

  void close_active_stream() {
    boost::system::error_code ec;
    if (nested_stream_) {
      nested_stream_->lowest_layer().shutdown(tcp::socket::shutdown_both, ec);
      nested_stream_->lowest_layer().close(ec);
      retired_nested_streams_.push_back(std::move(nested_stream_));
    }
    if (stream_) {
      stream_->lowest_layer().shutdown(tcp::socket::shutdown_both, ec);
      stream_->lowest_layer().close(ec);
      retired_streams_.push_back(std::move(stream_));
    }
  }

  void release_closed_streams() {
    if (reading_ || writing_) {
      return;
    }
    retired_nested_streams_.clear();
    retired_streams_.clear();
  }

  void wake_all_stream_waiters() {
    while (!stream_waiters_.empty()) {
      auto waiter = std::move(stream_waiters_.front());
      stream_waiters_.pop_front();
      if (waiter && !waiter->cancelled) {
        wake_waiter(waiter);
      }
    }
  }

  void notify_connect_waiters() {
    while (!connect_waiters_.empty()) {
      auto waiter = std::move(connect_waiters_.front());
      connect_waiters_.pop_front();
      if (waiter && !waiter->cancelled) {
        wake_waiter(waiter);
      }
    }
  }

  void fail_all_streams(const std::string& error) {
    std::vector<std::pair<H2Client::ResponseHandler, Response>> callbacks;
    std::vector<int32_t> callback_streams;
    for (auto& entry : streams_) {
      if (!entry.in_use || entry.state.done) {
        continue;
      }
      entry.state.response.error = error;
      entry.state.response.total_time_sec =
          std::chrono::duration<double>(std::chrono::steady_clock::now() -
                                        entry.state.start)
              .count();
      entry.state.done = true;
      if (entry.state.callback_mode) {
        callback_streams.push_back(entry.id);
        callbacks.emplace_back(std::move(entry.state.handler),
                               std::move(entry.state.response));
        continue;
      }
      auto waiter = state_waiters_.find(entry.id);
      if (waiter != state_waiters_.end() && waiter->second) {
        wake_waiter(waiter->second);
      }
    }
    for (auto stream_id : callback_streams) {
      release_stream_entry(stream_id);
      if (active_streams_ > 0) {
        --active_streams_;
      }
    }
    wake_stream_waiters();
    for (auto& [handler, response] : callbacks) {
      if (handler) {
        handler(std::move(response));
      }
    }
  }

  StreamEntry* find_stream_entry(int32_t stream_id) {
    for (auto it = stream_index_.begin(); it != stream_index_.end(); ++it) {
      if (it->stream_id != stream_id) {
        continue;
      }
      if (it->index >= streams_.size()) {
        stream_index_.erase(it);
        return nullptr;
      }
      auto& entry = streams_[it->index];
      if (!entry.in_use || entry.id != stream_id) {
        stream_index_.erase(it);
        return nullptr;
      }
      return &entry;
    }
    return nullptr;
  }

  StreamState* find_stream(int32_t stream_id) {
    auto* entry = find_stream_entry(stream_id);
    if (entry == nullptr) {
      return nullptr;
    }
    return &entry->state;
  }

  void erase_stream(int32_t stream_id) {
    release_stream_entry(stream_id);
  }

  StreamEntry& acquire_stream_entry(int32_t stream_id) {
    while (!free_stream_indices_.empty()) {
      auto index = free_stream_indices_.back();
      free_stream_indices_.pop_back();
      if (index < streams_.size() && !streams_[index].in_use) {
        auto& entry = streams_[index];
        entry.id = stream_id;
        entry.in_use = true;
        entry.state.reset_for_reuse();
        stream_index_.push_back(StreamIndexEntry{stream_id, index});
        return entry;
      }
    }
    streams_.push_back(StreamEntry{stream_id, true, StreamState{}});
    stream_index_.push_back(StreamIndexEntry{stream_id, streams_.size() - 1});
    return streams_.back();
  }

  void release_stream_entry(int32_t stream_id) {
    auto* entry = find_stream_entry(stream_id);
    if (entry != nullptr) {
      erase_stream_index(stream_id);
      entry->state.reset_for_reuse();
      entry->id = 0;
      entry->in_use = false;
      free_stream_indices_.push_back(static_cast<std::size_t>(entry - streams_.data()));
    }
  }

  void erase_stream_index(int32_t stream_id) {
    auto it = std::find_if(stream_index_.begin(), stream_index_.end(),
                           [&](const StreamIndexEntry& entry) {
                             return entry.stream_id == stream_id;
                           });
    if (it != stream_index_.end()) {
      *it = stream_index_.back();
      stream_index_.pop_back();
    }
  }

  bool idle() const {
    return !connecting_ && active_streams_ == 0 && stream_waiters_.empty() &&
           state_waiters_.empty();
  }

  asio::io_context& io_;
  asio::strand<asio::io_context::executor_type> strand_;
  ssl::context ssl_ctx_;
  asio::steady_timer deadline_timer_;
  Options options_;
  bool insecure_ = false;
  bool connected_ = false;
  bool connecting_ = false;
  bool stopping_ = false;
  std::atomic<bool> connected_snapshot_{false};
  std::atomic<bool> connecting_snapshot_{false};
  std::atomic<bool> stopping_snapshot_{false};
  std::atomic<std::size_t> active_streams_snapshot_{0};
  std::atomic<std::size_t> stream_limit_snapshot_{100};
  std::uint64_t io_generation_ = 0;
  std::unique_ptr<Stream> stream_;
  std::unique_ptr<NestedStream> nested_stream_;
  std::vector<std::unique_ptr<Stream>> retired_streams_;
  std::vector<std::unique_ptr<NestedStream>> retired_nested_streams_;
  std::string connect_error_;
  nghttp2_session_callbacks* callbacks_ = nullptr;
  nghttp2_session* session_ = nullptr;
  std::vector<StreamEntry> streams_;
  std::vector<std::size_t> free_stream_indices_;
  std::vector<StreamIndexEntry> stream_index_;
  std::unordered_map<int32_t, WaiterPtr> state_waiters_;
  std::vector<DeadlineEntry> deadline_heap_;
  std::vector<uint8_t> active_write_buffer_;
  std::vector<uint8_t> pending_write_buffer_;
  WaiterQueue connect_waiters_;
  WaiterQueue stream_waiters_;
  std::vector<WaiterPtr> state_waiter_pool_;
  std::size_t active_streams_ = 0;
  std::size_t peer_max_concurrent_streams_ = 100;
  bool deadline_timer_armed_ = false;
  bool writing_ = false;
  bool reading_ = false;
  bool goaway_received_ = false;
  Counters stats_;
};

void H2Client::Impl::StreamSlot::release() {
  if (active && self) {
    active = false;
    self->release_stream_slot();
  }
}

H2Client::H2Client(asio::io_context& io) : H2Client(io, Options{}) {}

struct H2Client::IoShard {
  asio::io_context io;
  asio::executor_work_guard<asio::io_context::executor_type> work;
  std::jthread thread;

  IoShard() : io(1), work(asio::make_work_guard(io)) {}
};

struct H2Client::SessionGroup {
  std::vector<std::shared_ptr<Impl>> impls;
  std::atomic<std::size_t> next{0};
  std::atomic<std::size_t> inflight{0};
  std::atomic<std::size_t> active_impls{1};
  std::chrono::steady_clock::time_point last_used{};
  std::chrono::steady_clock::time_point last_scale_up{};
  std::chrono::steady_clock::time_point last_busy{
      std::chrono::steady_clock::now()};
  std::list<std::string>::iterator lru_it;
  bool lru_linked = false;
};

struct H2Client::ActiveImplDecision {
  std::size_t previous = 1;
  std::size_t active = 1;
};

H2Client::H2Client(asio::io_context& io, Options options)
    : io_(io), options_(options) {
  auto shard_count = options_.shard_count;
  if (shard_count == 0) {
    return;
  }
  shards_.reserve(shard_count);
  for (std::size_t i = 0; i < shard_count; ++i) {
    auto shard = std::make_unique<IoShard>();
    auto* shard_ptr = shard.get();
    shard->thread = std::jthread([shard_ptr] { shard_ptr->io.run(); });
    shards_.push_back(std::move(shard));
  }
  schedule_maintenance();
}

H2Client::~H2Client() {
  lifetime_token_.reset();
  if (maintenance_timer_) {
    maintenance_timer_->cancel();
  }
  stop_owned_shards();
}

boost::asio::io_context& H2Client::io_for_session(std::size_t index) {
  if (shards_.empty()) {
    return io_;
  }
  return shards_[index % shards_.size()]->io;
}

H2Client::ActiveImplDecision H2Client::active_impl_count(
    SessionGroup& group, std::size_t inflight) const {
  if (!options_.auto_shards) {
    return ActiveImplDecision{group.impls.size(), group.impls.size()};
  }
  auto current = group.active_impls.load(std::memory_order_relaxed);
  auto previous = current;
  auto target = current;
  if (inflight >= 256) {
    target = std::min<std::size_t>(group.impls.size(), 4);
  } else if (inflight >= 128) {
    target = std::min<std::size_t>(group.impls.size(), 3);
  } else if (inflight >= 64) {
    target = std::min<std::size_t>(group.impls.size(), 2);
  }
  if (target <= current) {
    auto active = std::max<std::size_t>(1, current);
    return ActiveImplDecision{active, active};
  }
  auto now = std::chrono::steady_clock::now();
  const bool urgent_scale_up = inflight >= 128;
  std::lock_guard<std::mutex> lock(groups_mu_);
  current = group.active_impls.load(std::memory_order_relaxed);
  previous = current;
  if (target > current &&
      (urgent_scale_up ||
       now - group.last_scale_up >= options_.auto_scale_up_interval)) {
    auto next = urgent_scale_up ? target
                                : std::min<std::size_t>(target, current + 1);
    group.active_impls.store(next, std::memory_order_relaxed);
    group.last_scale_up = now;
    group.last_busy = now;
  }
  auto active = std::max<std::size_t>(
      1, group.active_impls.load(std::memory_order_relaxed));
  return ActiveImplDecision{std::max<std::size_t>(1, previous), active};
}

void H2Client::prewarm_active_impls(const std::shared_ptr<SessionGroup>& group,
                                    const Request& request, bool insecure,
                                    std::size_t previous_active,
                                    std::size_t active) const {
  if (!options_.auto_shards || !options_.auto_prewarm_sessions || !group ||
      active <= previous_active) {
    return;
  }
  auto end = std::min<std::size_t>(active, group->impls.size());
  for (std::size_t i = previous_active; i < end; ++i) {
    if (group->impls[i] && group->impls[i]->idle()) {
      group->impls[i]->preconnect(request, insecure);
    }
  }
}

std::size_t H2Client::choose_impl_index(SessionGroup& group,
                                        std::size_t active) const {
  active = std::max<std::size_t>(
      1, std::min<std::size_t>(active, group.impls.size()));
  auto start = group.next.fetch_add(1, std::memory_order_relaxed) % active;

  std::size_t best = active;
  std::size_t best_slots = 0;
  for (std::size_t offset = 0; offset < active; ++offset) {
    auto idx = (start + offset) % active;
    auto& impl = group.impls[idx];
    if (!impl) {
      continue;
    }
    auto slots = impl->available_stream_slots_snapshot();
    if (slots > best_slots) {
      best = idx;
      best_slots = slots;
      if (slots >= options_.max_concurrent_streams / 2) {
        break;
      }
    }
  }
  if (best < active) {
    return best;
  }

  for (std::size_t offset = 0; offset < active; ++offset) {
    auto idx = (start + offset) % active;
    auto& impl = group.impls[idx];
    if (impl && impl->connecting_snapshot()) {
      return idx;
    }
  }

  for (std::size_t offset = 0; offset < active; ++offset) {
    auto idx = (start + offset) % active;
    auto& impl = group.impls[idx];
    if (impl && impl->connected_snapshot()) {
      return idx;
    }
  }

  return start;
}

void H2Client::schedule_maintenance() {
  if (options_.maintenance_interval.count() <= 0 || shards_.empty()) {
    return;
  }
  if (!maintenance_timer_) {
    maintenance_timer_ =
        std::make_unique<asio::steady_timer>(shards_.front()->io);
  }
  maintenance_timer_->expires_after(options_.maintenance_interval);
  std::weak_ptr<int> weak_token = lifetime_token_;
  maintenance_timer_->async_wait([this, weak_token](boost::system::error_code ec) {
    if (weak_token.expired()) {
      return;
    }
    if (ec) {
      return;
    }
    run_maintenance();
    schedule_maintenance();
  });
}

void H2Client::run_maintenance() {
  std::lock_guard<std::mutex> lock(groups_mu_);
  auto now = std::chrono::steady_clock::now();
  for (auto& [_, group] : groups_) {
    if (!group || !options_.auto_shards) {
      continue;
    }
    auto inflight = group->inflight.load(std::memory_order_relaxed);
    if (inflight > 8) {
      group->last_busy = now;
      continue;
    }
    if (now - group->last_busy < options_.auto_scale_down_idle_ttl) {
      continue;
    }
    auto current = group->active_impls.load(std::memory_order_relaxed);
    if (current <= 1) {
      continue;
    }
    auto wanted = current - 1;
    group->active_impls.store(wanted, std::memory_order_relaxed);
    group->last_busy = now;
    for (std::size_t i = wanted; i < group->impls.size(); ++i) {
      if (group->impls[i] && group->impls[i]->idle()) {
        group->impls[i]->request_reset_if_idle();
      }
    }
  }
  evict_session_groups_locked();
}

void H2Client::stop_owned_shards() {
  for (auto& shard : shards_) {
    if (!shard) {
      continue;
    }
    shard->work.reset();
    shard->io.stop();
  }
}

bool H2Client::session_group_idle(const SessionGroup& group) const {
  return std::all_of(group.impls.begin(), group.impls.end(),
                     [](const std::shared_ptr<Impl>& impl) {
                       return impl && impl->idle();
                     });
}

void H2Client::evict_session_groups_locked() {
  const auto max_groups = options_.max_session_groups;
  if (max_groups == 0) {
    return;
  }
  const auto now = std::chrono::steady_clock::now();
  const auto ttl = options_.session_group_idle_ttl;
  auto expired = [&](const SessionGroup& group) {
    return ttl.count() > 0 && now - group.last_used >= ttl;
  };

  for (auto it = group_lru_.rbegin(); it != group_lru_.rend();) {
    auto map_it = groups_.find(*it);
    if (map_it == groups_.end()) {
      auto erase_it = std::next(it).base();
      it = std::make_reverse_iterator(group_lru_.erase(erase_it));
      continue;
    }
    auto& group = *map_it->second;
    if (session_group_idle(group) &&
        (expired(group) || groups_.size() >= max_groups)) {
      for (auto& impl : group.impls) {
        if (impl) {
          impl->request_shutdown();
        }
      }
      group.lru_linked = false;
      auto erase_it = std::next(it).base();
      it = std::make_reverse_iterator(group_lru_.erase(erase_it));
      groups_.erase(map_it);
      session_groups_evicted_.fetch_add(1, std::memory_order_relaxed);
      continue;
    }
    if (groups_.size() < max_groups) {
      break;
    }
    ++it;
  }
}

std::shared_ptr<H2Client::SessionGroup> H2Client::group_for(const Request& request) {
  auto url = parse_url(request.url);
  auto proxy = proxy_support::proxy_for_request(request);
  auto key = transport_key(url, proxy);
  struct LocalEntry {
    const H2Client* owner = nullptr;
    std::string key;
    std::weak_ptr<SessionGroup> group;
  };
  thread_local std::array<LocalEntry, 32> local_cache{};
  thread_local std::size_t local_cursor = 0;
  for (auto& entry : local_cache) {
    if (entry.owner == this && entry.key == key) {
      if (auto group = entry.group.lock()) {
        session_group_cache_hits_.fetch_add(1, std::memory_order_relaxed);
        if (groups_mu_.try_lock()) {
          std::lock_guard<std::mutex> lock(groups_mu_, std::adopt_lock);
          auto it = groups_.find(key);
          if (it != groups_.end() && it->second == group) {
            group->last_used = std::chrono::steady_clock::now();
            if (group->lru_linked) {
              group_lru_.splice(group_lru_.begin(), group_lru_, group->lru_it);
            }
          } else {
            entry = LocalEntry{};
          }
        }
        if (entry.owner == this) {
          return group;
        }
      }
      entry = LocalEntry{};
      break;
    }
  }

  std::lock_guard<std::mutex> lock(groups_mu_);
  auto it = groups_.find(key);
  if (it != groups_.end()) {
    session_group_cache_hits_.fetch_add(1, std::memory_order_relaxed);
    it->second->last_used = std::chrono::steady_clock::now();
    if (it->second->lru_linked) {
      group_lru_.splice(group_lru_.begin(), group_lru_, it->second->lru_it);
    }
    auto& entry = local_cache[local_cursor++ % local_cache.size()];
    entry.owner = this;
    entry.key = key;
    entry.group = it->second;
    return it->second;
  }
  session_group_cache_misses_.fetch_add(1, std::memory_order_relaxed);
  evict_session_groups_locked();
  auto group = std::make_shared<SessionGroup>();
  auto n = std::max<std::size_t>(1, options_.sessions_per_origin);
  group->impls.reserve(n);
  for (std::size_t i = 0; i < n; ++i) {
    group->impls.push_back(std::make_shared<Impl>(io_for_session(i), options_));
  }
  group->active_impls.store(options_.auto_shards ? 1 : group->impls.size(),
                            std::memory_order_relaxed);
  group->last_used = std::chrono::steady_clock::now();
  group_lru_.push_front(key);
  group->lru_it = group_lru_.begin();
  group->lru_linked = true;
  groups_.emplace(key, group);
  auto& entry = local_cache[local_cursor++ % local_cache.size()];
  entry.owner = this;
  entry.key = key;
  entry.group = group;
  return group;
}

asio::awaitable<Response> H2Client::get(std::string url, bool insecure) {
  Request request;
  request.url = std::move(url);
  request.method = "GET";
  co_return co_await async_request(std::move(request), insecure);
}

asio::awaitable<Response> H2Client::async_request(Request request, bool insecure) {
  auto group = group_for(request);
  auto inflight = group->inflight.fetch_add(1, std::memory_order_relaxed) + 1;
  auto decision = active_impl_count(*group, inflight);
  prewarm_active_impls(group, request, insecure, decision.previous,
                       decision.active);
  auto idx = choose_impl_index(*group, decision.active);
  try {
    auto response =
        co_await group->impls[idx]->async_request(std::move(request), insecure);
    group->inflight.fetch_sub(1, std::memory_order_relaxed);
    co_return response;
  } catch (...) {
    group->inflight.fetch_sub(1, std::memory_order_relaxed);
    throw;
  }
}

asio::awaitable<void> H2Client::preconnect(Request request, std::size_t count,
                                           bool insecure) {
  if (count == 0) {
    co_return;
  }
  auto group = group_for(request);
  auto target = std::min<std::size_t>(std::max<std::size_t>(1, count),
                                      group->impls.size());
  if (options_.auto_shards) {
    auto current = group->active_impls.load(std::memory_order_relaxed);
    if (target > current) {
      group->active_impls.store(target, std::memory_order_relaxed);
    }
  }
  co_await asyncx::for_each_limited(
      target, target,
      [&](std::size_t index) -> asio::awaitable<void> {
        if (index < group->impls.size() && group->impls[index]) {
          co_await group->impls[index]->preconnect_wait(request, insecure);
        }
        co_return;
      },
      [](std::size_t, std::monostate) {});
}

void H2Client::async_request_callback(Request request, ResponseHandler handler,
                                      bool insecure) {
  auto group = group_for(request);
  auto inflight = group->inflight.fetch_add(1, std::memory_order_relaxed) + 1;
  auto decision = active_impl_count(*group, inflight);
  prewarm_active_impls(group, request, insecure, decision.previous,
                       decision.active);
  auto idx = choose_impl_index(*group, decision.active);
  group->impls[idx]->async_request_callback(
      std::move(request),
      [group = std::move(group), handler = std::move(handler)](Response response) mutable {
        group->inflight.fetch_sub(1, std::memory_order_relaxed);
        handler(std::move(response));
      },
      insecure);
}

H2Client::Stats H2Client::stats() const {
  Stats out;
  std::lock_guard<std::mutex> lock(groups_mu_);
  out.session_groups = groups_.size();
  out.session_groups_evicted =
      session_groups_evicted_.load(std::memory_order_relaxed);
  out.session_group_cache_hits =
      session_group_cache_hits_.load(std::memory_order_relaxed);
  out.session_group_cache_misses =
      session_group_cache_misses_.load(std::memory_order_relaxed);
  for (const auto& [_, group] : groups_) {
    for (const auto& impl : group->impls) {
      out.streams_submitted += impl->stats_.streams_submitted.load();
      out.streams_completed += impl->stats_.streams_completed.load();
      out.streams_timed_out += impl->stats_.streams_timed_out.load();
      out.streams_cancelled += impl->stats_.streams_cancelled.load();
      out.stream_slot_waits += impl->stats_.stream_slot_waits.load();
      out.stream_slot_wait_cancelled +=
          impl->stats_.stream_slot_wait_cancelled.load();
      out.connect_waits += impl->stats_.connect_waits.load();
      out.connect_wait_cancelled += impl->stats_.connect_wait_cancelled.load();
      out.preconnect_attempts += impl->stats_.preconnect_attempts.load();
      out.preconnect_success += impl->stats_.preconnect_success.load();
      out.preconnect_failed += impl->stats_.preconnect_failed.load();
      out.max_active_streams =
          std::max<std::uint64_t>(out.max_active_streams,
                                  impl->stats_.max_active_streams.load());
      out.max_pending_stream_waiters =
          std::max<std::uint64_t>(out.max_pending_stream_waiters,
                                  impl->stats_.max_pending_stream_waiters.load());
      out.peer_max_concurrent_streams =
          std::max<std::uint64_t>(out.peer_max_concurrent_streams,
                                  impl->peer_max_concurrent_streams_);
      out.configured_max_concurrent_streams =
          std::max<std::uint64_t>(out.configured_max_concurrent_streams,
                                  impl->options_.max_concurrent_streams);
    }
  }
  return out;
}

void H2Client::reset_stats() {
  std::lock_guard<std::mutex> lock(groups_mu_);
  session_groups_evicted_.store(0, std::memory_order_relaxed);
  session_group_cache_hits_.store(0, std::memory_order_relaxed);
  session_group_cache_misses_.store(0, std::memory_order_relaxed);
  for (auto& [_, group] : groups_) {
    for (auto& impl : group->impls) {
      impl->reset_stats();
    }
  }
}

void H2Client::shutdown() {
  std::lock_guard<std::mutex> lock(groups_mu_);
  for (auto& [_, group] : groups_) {
    for (auto& impl : group->impls) {
      impl->request_shutdown();
    }
  }
}

asio::awaitable<void> H2Client::reset_connections() {
  std::vector<std::shared_ptr<Impl>> impls;
  {
    std::lock_guard<std::mutex> lock(groups_mu_);
    for (auto& [_, group] : groups_) {
      impls.insert(impls.end(), group->impls.begin(), group->impls.end());
    }
  }
  for (auto& impl : impls) {
    co_await impl->reset();
  }
}

}  // namespace httpclient
