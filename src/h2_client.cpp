#include "httpclient/h2_client.hpp"

#include <boost/asio/awaitable.hpp>
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
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
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
  std::transform(name.begin(), name.end(), name.begin(), [](unsigned char c) {
    return static_cast<char>(std::tolower(c));
  });
  return {name, value};
}

bool h2_skip_header(std::string_view name) {
  return name.empty() || name.front() == ':' || header_name_equals(name, "host") ||
         header_name_equals(name, "connection") ||
         header_name_equals(name, "keep-alive") ||
         header_name_equals(name, "proxy-connection") ||
         header_name_equals(name, "transfer-encoding") ||
         header_name_equals(name, "upgrade") ||
         header_name_equals(name, "content-length");
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

  explicit Impl(asio::io_context& io, Options options)
      : io_(io),
        strand_(asio::make_strand(io)),
        ssl_ctx_(ssl::context::tls_client),
        deadline_timer_(strand_),
        options_(options) {}

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
    std::atomic<std::uint64_t> stream_slot_waits{0};
    std::atomic<std::uint64_t> max_active_streams{0};
    std::atomic<std::uint64_t> max_pending_stream_waiters{0};
  };

  static void update_max(std::atomic<std::uint64_t>& target, std::uint64_t value) {
    auto current = target.load(std::memory_order_relaxed);
    while (current < value &&
           !target.compare_exchange_weak(current, value, std::memory_order_relaxed)) {}
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
    bool callback_mode = false;
    BodySource body_source;
    bool has_body_source = false;
    std::chrono::steady_clock::time_point deadline{};
    std::chrono::steady_clock::time_point start{};
    H2Client::ResponseHandler handler;
  };

  struct StreamEntry {
    int32_t id = 0;
    bool in_use = false;
    StreamState state;
  };

  struct DeadlineEntry {
    std::chrono::steady_clock::time_point deadline{};
    int32_t stream_id = 0;

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
      }
    }
    void dismiss() { armed = false; }
  };

  asio::awaitable<void> ensure_connected(const ParsedUrl& url, bool insecure) {
    if (connected_) {
      co_return;
    }
    if (connecting_) {
      asio::steady_timer timer(strand_);
      while (!connected_ && connecting_) {
        timer.expires_after(std::chrono::milliseconds(1));
        co_await timer.async_wait(asio::use_awaitable);
      }
      if (connected_) {
        co_return;
      }
    }
    connecting_ = true;
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
      streams_.reserve(std::max<std::size_t>(256, options_.max_concurrent_streams * 2));
      state_waiters_.reserve(std::max<std::size_t>(256, options_.max_concurrent_streams * 2));
    }

    static const unsigned char alpn[] = {2, 'h', '2'};
    SSL_CTX_set_alpn_protos(ssl_ctx_.native_handle(), alpn, sizeof(alpn));
    if (options_.verify_tls && !insecure) {
      ssl_ctx_.set_default_verify_paths();
    } else {
      ssl_ctx_.set_verify_mode(ssl::verify_none);
    }

    stream_ = std::make_unique<Stream>(strand_, ssl_ctx_);
    if (insecure || !options_.verify_tls) {
      stream_->set_verify_mode(ssl::verify_none);
    }
    if (!SSL_set_tlsext_host_name(stream_->native_handle(), url.host.c_str())) {
      throw std::runtime_error("SNI setup failed");
    }

    tcp::resolver resolver(strand_);
    auto endpoints = co_await resolver.async_resolve(url.host, url.port,
                                                     asio::use_awaitable);
    co_await asio::async_connect(stream_->next_layer(), endpoints, asio::use_awaitable);
    boost::system::error_code option_ec;
    stream_->next_layer().set_option(tcp::no_delay(true), option_ec);
    co_await stream_->async_handshake(ssl::stream_base::client, asio::use_awaitable);

    const unsigned char* selected = nullptr;
    unsigned int selected_len = 0;
    SSL_get0_alpn_selected(stream_->native_handle(), &selected, &selected_len);
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
    guard.dismiss();

    auto generation = io_generation_;
    auto self = shared_from_this();
    asio::co_spawn(
        strand_,
        [self, generation]() -> asio::awaitable<void> {
          co_await self->read_loop(generation);
        },
        asio::detached);
  }

  asio::awaitable<Response> get(std::string url_text, bool insecure) {
    Request request;
    request.url = std::move(url_text);
    request.method = "GET";
    co_return co_await async_request(std::move(request), insecure);
  }

  asio::awaitable<Response> async_request(Request request, bool insecure) {
    auto self = shared_from_this();
    co_return co_await asio::co_spawn(
        strand_, self->request_on_strand(std::move(request), insecure),
        asio::use_awaitable);
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
            co_await self->ensure_connected(url, insecure);
            auto slot = co_await self->acquire_stream_slot();
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

  asio::awaitable<Response> request_on_strand(Request request, bool insecure) {
    auto start = std::chrono::steady_clock::now();
    try {
      auto url = parse_url(request.url);
      co_await ensure_connected(url, insecure);
      auto slot = co_await acquire_stream_slot();
      auto stream_id = submit_stream(std::move(request), url, start);
      auto* state = find_stream(stream_id);
      if (state == nullptr) {
        throw std::runtime_error("h2 stream state missing");
      }

      if (!state->done) {
        auto waiter = std::make_shared<asio::steady_timer>(strand_);
        waiter->expires_at(asio::steady_timer::time_point::max());
        state_waiters_[stream_id] = waiter;
        boost::system::error_code ec;
        co_await waiter->async_wait(asio::redirect_error(asio::use_awaitable, ec));
        state_waiters_.erase(stream_id);
      }
      state = find_stream(stream_id);
      if (state == nullptr || !state->done) {
        nghttp2_submit_rst_stream(session_, NGHTTP2_FLAG_NONE, stream_id,
                                  NGHTTP2_CANCEL);
        pump_output();
        erase_stream(stream_id);
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
    std::string content_length;
    const std::string_view method =
        request.method.empty() ? std::string_view("GET")
                               : std::string_view(request.method);
    if (!request.body.empty()) {
      content_length = std::to_string(request.body.size());
    }

    std::vector<std::pair<std::string, std::string>> extra_headers;
    extra_headers.reserve(request.headers.size());
    bool has_accept = false;
    for (const auto& header : request.headers) {
      auto [name, value] = split_header(header);
      if (h2_skip_header(name)) {
        continue;
      }
      has_accept = has_accept || header_name_equals(name, "accept");
      extra_headers.emplace_back(std::move(name), std::move(value));
    }

    std::vector<nghttp2_nv> hdrs;
    hdrs.reserve(6 + extra_headers.size());
    hdrs.push_back(nv(":method", method));
    hdrs.push_back(nv(":scheme", url.scheme));
    hdrs.push_back(nv(":path", url.target));
    hdrs.push_back(nv(":authority", url.host));
    if (!has_accept) {
      hdrs.push_back(nv("accept", "*/*"));
    }
    if (!request.body.empty()) {
      hdrs.push_back(nv("content-length", content_length));
    }
    for (const auto& [name, value] : extra_headers) {
      hdrs.push_back(nghttp2_nv{
          reinterpret_cast<uint8_t*>(const_cast<char*>(name.data())),
          reinterpret_cast<uint8_t*>(const_cast<char*>(value.data())),
          name.size(),
          value.size(),
          NGHTTP2_NV_FLAG_NONE,
      });
    }

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
        nghttp2_submit_request(session_, nullptr, hdrs.data(), hdrs.size(), provider,
                               nullptr);
    if (stream_id < 0) {
      release_stream_entry(expected_stream_id);
      throw std::runtime_error(nghttp2_strerror(stream_id));
    }
    if (stream_id != expected_stream_id) {
      auto it = find_stream_it(expected_stream_id);
      if (it != streams_.end()) {
        it->id = stream_id;
        state = &it->state;
      } else {
        state = &acquire_stream_entry(stream_id).state;
      }
    }
    ++stats_.streams_submitted;
    state->store_body = request.store_response_body;
    state->store_headers = request.store_response_headers;
    state->deadline = std::chrono::steady_clock::now() +
                      std::chrono::milliseconds(request.timeout_ms);
    state->start = start;
    deadline_heap_.push_back(DeadlineEntry{state->deadline, stream_id});
    std::push_heap(deadline_heap_.begin(), deadline_heap_.end(),
                   std::greater<DeadlineEntry>{});
    pump_output();
    arm_deadline_timer();
    return stream_id;
  }

  void complete_stream(int32_t stream_id) {
    auto it = find_stream_it(stream_id);
    if (it == streams_.end()) {
      return;
    }
    it->state.done = true;
    if (!it->state.callback_mode) {
      auto waiter = state_waiters_.find(stream_id);
      if (waiter != state_waiters_.end() && waiter->second) {
        waiter->second->cancel();
      }
      return;
    }

    auto response = std::move(it->state.response);
    response.total_time_sec =
        std::chrono::duration<double>(std::chrono::steady_clock::now() -
                                      it->state.start)
            .count();
    response.http_version = 3;
    auto handler = std::move(it->state.handler);
    release_stream_entry(it);
    release_stream_slot();
    ++stats_.streams_completed;
    if (handler) {
      handler(std::move(response));
    }
  }

  std::size_t stream_limit() const {
    auto configured = std::max<std::size_t>(1, options_.max_concurrent_streams);
    auto peer = std::max<std::size_t>(1, peer_max_concurrent_streams_);
    return std::min(configured, peer);
  }

  asio::awaitable<StreamSlot> acquire_stream_slot() {
    for (;;) {
      if (stopping_) {
        throw std::runtime_error("h2 shutdown");
      }
      if (active_streams_ < stream_limit()) {
        ++active_streams_;
        update_max(stats_.max_active_streams, active_streams_);
        co_return StreamSlot{this, true};
      }
      ++stats_.stream_slot_waits;
      auto waiter = std::make_shared<asio::steady_timer>(strand_);
      waiter->expires_at(asio::steady_timer::time_point::max());
      stream_waiters_.push_back(waiter);
      update_max(stats_.max_pending_stream_waiters, stream_waiters_.size());
      boost::system::error_code ec;
      co_await waiter->async_wait(asio::redirect_error(asio::use_awaitable, ec));
    }
  }

  void release_stream_slot() {
    if (active_streams_ > 0) {
      --active_streams_;
    }
    wake_stream_waiters();
  }

  void wake_stream_waiters() {
    while (!stream_waiters_.empty() && active_streams_ < stream_limit()) {
      auto waiter = std::move(stream_waiters_.front());
      stream_waiters_.pop_front();
      if (waiter) {
        waiter->cancel();
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
      auto it = find_stream_it(stream_id);
      if (it == streams_.end() || it->state.done ||
          it->state.deadline != entry.deadline) {
        continue;
      }
      expired_any = true;
      it->state.response.error = "h2 stream timeout";
      it->state.done = true;
      ++stats_.streams_timed_out;
      nghttp2_submit_rst_stream(session_, NGHTTP2_FLAG_NONE, stream_id,
                                NGHTTP2_CANCEL);
      if (it->state.callback_mode) {
        auto response = std::move(it->state.response);
        response.total_time_sec =
            std::chrono::duration<double>(std::chrono::steady_clock::now() -
                                          it->state.start)
                .count();
        response.http_version = 3;
        auto handler = std::move(it->state.handler);
        release_stream_entry(it);
        release_stream_slot();
        if (handler) {
          handler(std::move(response));
        }
        continue;
      }
      auto waiter = state_waiters_.find(stream_id);
      if (waiter != state_waiters_.end() && waiter->second) {
        waiter->second->cancel();
      }
    }
    if (expired_any) {
      pump_output();
    }
  }

  void prune_deadline_heap() {
    while (!deadline_heap_.empty()) {
      auto& entry = deadline_heap_.front();
      auto it = find_stream_it(entry.stream_id);
      if (it != streams_.end() && !it->state.done &&
          it->state.deadline == entry.deadline) {
        return;
      }
      std::pop_heap(deadline_heap_.begin(), deadline_heap_.end(),
                    std::greater<DeadlineEntry>{});
      deadline_heap_.pop_back();
    }
  }

  void pump_output() {
    if (stopping_ || !stream_) {
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
      while (!stopping_ && generation == io_generation_ && stream_) {
        if (active_write_buffer_.empty()) {
          active_write_buffer_.swap(pending_write_buffer_);
        }
        if (active_write_buffer_.empty()) {
          writing_ = false;
          co_return;
        }
        boost::system::error_code ec;
        co_await asio::async_write(
            *stream_, asio::buffer(active_write_buffer_),
            asio::redirect_error(asio::use_awaitable, ec));
        if (ec) {
          if (generation != io_generation_ && ec == asio::error::operation_aborted) {
            writing_ = false;
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
  }

  asio::awaitable<void> read_loop(std::uint64_t generation) {
    std::array<uint8_t, 16384> buf{};
    try {
      for (;;) {
        if (stopping_ || generation != io_generation_ || !stream_) {
          break;
        }
        boost::system::error_code ec;
        std::size_t n = co_await stream_->async_read_some(
            asio::buffer(buf), asio::redirect_error(asio::use_awaitable, ec));
        if (ec) {
          if (generation != io_generation_ && ec == asio::error::operation_aborted) {
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
    if (stream != nullptr && stream->store_body) {
      stream->response.body.append(reinterpret_cast<const char*>(data), len);
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
          self->wake_stream_waiters();
        }
      }
    }
    if ((frame->hd.type == NGHTTP2_DATA || frame->hd.type == NGHTTP2_HEADERS) &&
        (frame->hd.flags & NGHTTP2_FLAG_END_STREAM)) {
      self->complete_stream(frame->hd.stream_id);
    }
    return 0;
  }

  void request_shutdown() {
    auto self = shared_from_this();
    asio::dispatch(strand_, [self] { self->shutdown_now(); });
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
    if (stopping_ && !stream_) {
      return;
    }
    stopping_ = true;
    if (stream_) {
      boost::system::error_code ec;
      stream_->lowest_layer().shutdown(tcp::socket::shutdown_both, ec);
      stream_->lowest_layer().close(ec);
    }
    active_write_buffer_.clear();
    pending_write_buffer_.clear();
    writing_ = false;
    connected_ = false;
    connecting_ = false;
    active_streams_ = 0;
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
    if (stream_) {
      boost::system::error_code ec;
      stream_->lowest_layer().shutdown(tcp::socket::shutdown_both, ec);
      stream_->lowest_layer().close(ec);
      stream_.reset();
    }
    active_write_buffer_.clear();
    pending_write_buffer_.clear();
    writing_ = false;
    connected_ = false;
    connecting_ = false;
    active_streams_ = 0;
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
  }

  void wake_all_stream_waiters() {
    while (!stream_waiters_.empty()) {
      auto waiter = std::move(stream_waiters_.front());
      stream_waiters_.pop_front();
      if (waiter) {
        waiter->cancel();
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
        waiter->second->cancel();
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

  std::vector<StreamEntry>::iterator find_stream_it(int32_t stream_id) {
    return std::find_if(streams_.begin(), streams_.end(),
                        [stream_id](const StreamEntry& entry) {
                          return entry.in_use && entry.id == stream_id;
                        });
  }

  StreamState* find_stream(int32_t stream_id) {
    auto it = find_stream_it(stream_id);
    if (it == streams_.end()) {
      return nullptr;
    }
    return &it->state;
  }

  void erase_stream(int32_t stream_id) {
    release_stream_entry(stream_id);
  }

  StreamEntry& acquire_stream_entry(int32_t stream_id) {
    for (auto& entry : streams_) {
      if (!entry.in_use) {
        entry.id = stream_id;
        entry.in_use = true;
        entry.state = StreamState{};
        return entry;
      }
    }
    streams_.push_back(StreamEntry{stream_id, true, StreamState{}});
    return streams_.back();
  }

  void release_stream_entry(int32_t stream_id) {
    auto it = find_stream_it(stream_id);
    if (it != streams_.end()) {
      release_stream_entry(it);
    }
  }

  void release_stream_entry(std::vector<StreamEntry>::iterator it) {
    if (it != streams_.end()) {
      it->state = StreamState{};
      it->id = 0;
      it->in_use = false;
    }
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
  std::uint64_t io_generation_ = 0;
  std::unique_ptr<Stream> stream_;
  nghttp2_session_callbacks* callbacks_ = nullptr;
  nghttp2_session* session_ = nullptr;
  std::vector<StreamEntry> streams_;
  std::unordered_map<int32_t, std::shared_ptr<asio::steady_timer>> state_waiters_;
  std::vector<DeadlineEntry> deadline_heap_;
  std::vector<uint8_t> active_write_buffer_;
  std::vector<uint8_t> pending_write_buffer_;
  std::deque<std::shared_ptr<asio::steady_timer>> stream_waiters_;
  std::size_t active_streams_ = 0;
  std::size_t peer_max_concurrent_streams_ = 100;
  bool deadline_timer_armed_ = false;
  bool writing_ = false;
  Counters stats_;
};

void H2Client::Impl::StreamSlot::release() {
  if (active && self) {
    active = false;
    self->release_stream_slot();
  }
}

H2Client::H2Client(asio::io_context& io) : H2Client(io, Options{}) {}

H2Client::H2Client(asio::io_context& io, Options options) {
  auto n = std::max<std::size_t>(1, options.sessions_per_origin);
  impls_.reserve(n);
  for (std::size_t i = 0; i < n; ++i) {
    impls_.push_back(std::make_shared<Impl>(io, options));
  }
}

H2Client::~H2Client() = default;

asio::awaitable<Response> H2Client::get(std::string url, bool insecure) {
  Request request;
  request.url = std::move(url);
  request.method = "GET";
  co_return co_await async_request(std::move(request), insecure);
}

asio::awaitable<Response> H2Client::async_request(Request request, bool insecure) {
  auto idx = next_impl_.fetch_add(1, std::memory_order_relaxed) % impls_.size();
  co_return co_await impls_[idx]->async_request(std::move(request), insecure);
}

void H2Client::async_request_callback(Request request, ResponseHandler handler,
                                      bool insecure) {
  auto idx = next_impl_.fetch_add(1, std::memory_order_relaxed) % impls_.size();
  impls_[idx]->async_request_callback(std::move(request), std::move(handler), insecure);
}

H2Client::Stats H2Client::stats() const {
  Stats out;
  for (const auto& impl : impls_) {
    out.streams_submitted += impl->stats_.streams_submitted.load();
    out.streams_completed += impl->stats_.streams_completed.load();
    out.streams_timed_out += impl->stats_.streams_timed_out.load();
    out.stream_slot_waits += impl->stats_.stream_slot_waits.load();
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
  return out;
}

void H2Client::shutdown() {
  for (auto& impl : impls_) {
    impl->request_shutdown();
  }
}

asio::awaitable<void> H2Client::reset_connections() {
  for (auto& impl : impls_) {
    co_await impl->reset();
  }
}

}  // namespace httpclient
