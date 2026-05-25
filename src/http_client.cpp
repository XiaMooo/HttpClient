#include "httpclient/http_client.hpp"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <algorithm>
#include <atomic>
#include <array>
#include <chrono>
#include <cstdint>
#include <deque>
#include <list>
#include <stdexcept>

namespace httpclient {
namespace asio = boost::asio;

namespace {

struct ParsedUrl {
  std::string scheme;
  std::string host;
  std::string port;
};

ParsedUrl parse_origin(const std::string& url) {
  auto scheme_end = url.find("://");
  if (scheme_end == std::string::npos) {
    throw std::invalid_argument("url must include scheme");
  }
  ParsedUrl out;
  out.scheme = url.substr(0, scheme_end);
  auto rest_start = scheme_end + 3;
  auto path_start = url.find('/', rest_start);
  auto authority = path_start == std::string::npos
                       ? url.substr(rest_start)
                       : url.substr(rest_start, path_start - rest_start);
  auto colon = authority.rfind(':');
  if (colon == std::string::npos) {
    out.host = authority;
    out.port = out.scheme == "https" ? "443" : "80";
  } else {
    out.host = authority.substr(0, colon);
    out.port = authority.substr(colon + 1);
  }
  return out;
}

std::string origin_key(const Request& request) {
  auto parsed = parse_origin(request.url);
  return parsed.scheme + "://" + parsed.host + ":" + parsed.port;
}

enum class CachedRouteProtocol : unsigned char {
  Unknown,
  H1Only,
  H2Available,
};

struct ThreadLocalRouteEntry {
  const void* owner = nullptr;
  std::string url;
  std::string origin;
  CachedRouteProtocol protocol = CachedRouteProtocol::Unknown;
  std::chrono::steady_clock::time_point last_used{};
  std::uint64_t generation = 0;
};

thread_local std::array<ThreadLocalRouteEntry, 64> tls_route_cache{};
thread_local std::size_t tls_route_cursor = 0;

bool lookup_tls_route(const void* owner, const std::string& url,
                      std::chrono::seconds ttl, std::uint64_t generation,
                      std::string& origin, CachedRouteProtocol& protocol) {
  const auto now = std::chrono::steady_clock::now();
  for (auto& entry : tls_route_cache) {
    if (entry.owner == owner && !entry.url.empty() && entry.url == url) {
      const bool expired =
          ttl.count() > 0 && now - entry.last_used >= ttl;
      if (entry.generation != generation || expired) {
        entry = ThreadLocalRouteEntry{};
        return false;
      }
      entry.last_used = now;
      origin = entry.origin;
      protocol = entry.protocol;
      return true;
    }
  }
  return false;
}

void store_tls_route(const void* owner, std::string url, std::string origin,
                     CachedRouteProtocol protocol, std::uint64_t generation) {
  auto& entry = tls_route_cache[tls_route_cursor++ % tls_route_cache.size()];
  entry.owner = owner;
  entry.url = std::move(url);
  entry.origin = std::move(origin);
  entry.protocol = protocol;
  entry.last_used = std::chrono::steady_clock::now();
  entry.generation = generation;
}

}  // namespace

struct HttpClient::Impl : std::enable_shared_from_this<Impl> {
  enum class ProtocolState {
    Unknown,
    Detecting,
    H2Available,
    H1Only,
    H2FailedRecently,
  };

  enum class GateAction {
    UseH1,
    UseH2,
    Detect,
    Wait,
  };

  struct Waiter {
    std::shared_ptr<asio::steady_timer> wake;
    bool done = false;
  };

  struct Counters {
    std::atomic<std::uint64_t> probe_h1_adopted{0};
    std::atomic<std::uint64_t> probe_h2_marked{0};
    std::atomic<std::uint64_t> probe_reconnect{0};
    std::atomic<std::uint64_t> overflow_fallback{0};
    std::atomic<std::uint64_t> overflow_fallback_on_h2_origin{0};
    std::atomic<std::uint64_t> url_route_cache_hits{0};
    std::atomic<std::uint64_t> url_route_cache_misses{0};
    std::atomic<std::uint64_t> h1_cached_routes{0};
    std::atomic<std::uint64_t> h2_cached_routes{0};
    std::atomic<std::uint64_t> detect_waiters{0};
    std::atomic<std::uint64_t> detect_queue_overflow{0};
    std::atomic<std::uint64_t> detect_overflow_to_h1{0};
    std::atomic<std::uint64_t> detect_overflow_to_h1_later_h2{0};
  };

  struct OriginState {
    ProtocolState protocol = ProtocolState::Unknown;
    std::chrono::steady_clock::time_point retry_after{};
    std::deque<std::shared_ptr<Waiter>> waiters;
    bool saw_h2_origin = false;
  };

  struct GateDecision {
    GateAction action = GateAction::UseH1;
    std::shared_ptr<asio::steady_timer> waiter;
    std::shared_ptr<Waiter> waiter_state;
    bool overflow_fallback = false;
  };

  struct OriginGate : std::enable_shared_from_this<OriginGate> {
    OriginGate(asio::io_context& io, Options& options)
        : io(io), options(options), strand(asio::make_strand(io)) {}

    asio::awaitable<GateDecision> enter() {
      co_return co_await asio::co_spawn(
          strand,
          [self = shared_from_this()]() -> asio::awaitable<GateDecision> {
            auto now = std::chrono::steady_clock::now();
            switch (self->state.protocol) {
              case ProtocolState::H2Available:
                co_return GateDecision{GateAction::UseH2, {}, {}, false};
              case ProtocolState::H1Only:
                co_return GateDecision{GateAction::UseH1, {}, {}, false};
              case ProtocolState::H2FailedRecently:
                if (now < self->state.retry_after) {
                  co_return GateDecision{GateAction::UseH1, {}, {}, false};
                }
                self->state.protocol = ProtocolState::Detecting;
                self->eviction_blocked.store(true, std::memory_order_release);
                co_return GateDecision{GateAction::Detect, {}, {}, false};
              case ProtocolState::Detecting:
                if (self->state.waiters.size() >= self->options.origin_waiter_limit) {
                  if (self->options.detection_overflow_policy ==
                      HttpClient::DetectionOverflowPolicy::WaitForDetection) {
                    break;
                  }
                  co_return GateDecision{GateAction::UseH1, {}, {}, true};
                }
                break;
              case ProtocolState::Unknown:
                self->state.protocol = ProtocolState::Detecting;
                self->eviction_blocked.store(true, std::memory_order_release);
                co_return GateDecision{GateAction::Detect, {}, {}, false};
            }

            auto waiter = std::make_shared<asio::steady_timer>(self->io);
            waiter->expires_at(asio::steady_timer::time_point::max());
            auto waiter_state = std::make_shared<Waiter>(Waiter{waiter});
            self->state.waiters.push_back(waiter_state);
            co_return GateDecision{GateAction::Wait, waiter, waiter_state, false};
          },
          asio::use_awaitable);
    }

    asio::awaitable<void> set_protocol(ProtocolState protocol) {
      co_await asio::co_spawn(
          strand,
          [self = shared_from_this(), protocol]() -> asio::awaitable<void> {
            self->state.protocol = protocol;
            self->state.retry_after = {};
            self->notify_waiters();
            self->eviction_blocked.store(false, std::memory_order_release);
            co_return;
          },
          asio::use_awaitable);
    }

    asio::awaitable<void> mark_h2_failed() {
      co_await asio::co_spawn(
          strand,
          [self = shared_from_this()]() -> asio::awaitable<void> {
            self->state.protocol = ProtocolState::H2FailedRecently;
            self->state.retry_after =
                std::chrono::steady_clock::now() + self->options.h2_failure_ttl;
            self->notify_waiters();
            self->eviction_blocked.store(false, std::memory_order_release);
            co_return;
          },
          asio::use_awaitable);
    }

    asio::awaitable<void> record_h2_origin() {
      co_await asio::co_spawn(
          strand,
          [self = shared_from_this()]() -> asio::awaitable<void> {
            self->state.saw_h2_origin = true;
            co_return;
          },
          asio::use_awaitable);
    }

    asio::awaitable<bool> has_h2_origin() {
      co_return co_await asio::co_spawn(
          strand,
          [self = shared_from_this()]() -> asio::awaitable<bool> {
            co_return self->state.saw_h2_origin;
          },
          asio::use_awaitable);
    }

    void notify_waiters() {
      for (auto& waiter : state.waiters) {
        if (waiter) {
          waiter->done = true;
        }
        if (waiter && waiter->wake) {
          waiter->wake->cancel();
        }
      }
      state.waiters.clear();
    }

    asio::io_context& io;
    Options& options;
    asio::strand<asio::io_context::executor_type> strand;
    OriginState state;
    std::atomic<bool> eviction_blocked{false};
  };

  struct CachedOrigin {
    std::shared_ptr<OriginGate> gate;
    ProtocolState protocol = ProtocolState::Unknown;
    std::chrono::steady_clock::time_point last_used{};
    std::list<std::string>::iterator lru_it;
    bool lru_linked = false;
  };

  Impl(asio::io_context& io, Options options)
      : io_(io),
        options_(std::move(options)),
        h1_(options_.h1),
        h2_(io, options_.h2) {}

  asio::awaitable<Response> async_request(Request request) {
    try {
      switch (request.protocol_policy) {
        case ProtocolPolicy::ForceH1:
          co_return co_await h1_.async_request(std::move(request));
        case ProtocolPolicy::ForceH2:
          co_return co_await request_h2(std::move(request), true);
        case ProtocolPolicy::PreferH1:
          co_return co_await request_prefer_h1(std::move(request));
        case ProtocolPolicy::PreferH2:
          co_return co_await request_prefer_h2(std::move(request));
        case ProtocolPolicy::Auto:
          break;
      }
      co_return co_await request_auto(std::move(request));
    } catch (const std::exception& e) {
      Response response;
      response.error = e.what();
      co_return response;
    }
  }

  asio::awaitable<Response> request_auto(Request request) {
    std::string key;
    CachedRouteProtocol cached = CachedRouteProtocol::Unknown;
    if (lookup_tls_route(this, request.url, options_.origin_cache_ttl,
                         cache_generation_.load(std::memory_order_acquire), key,
                         cached)) {
      ++stats_.url_route_cache_hits;
      if (cached == CachedRouteProtocol::H1Only) {
        ++stats_.h1_cached_routes;
        co_return co_await h1_.async_request(std::move(request));
      }
      if (cached == CachedRouteProtocol::H2Available) {
        ++stats_.h2_cached_routes;
        co_return co_await request_h2(std::move(request), false);
      }
    } else {
      ++stats_.url_route_cache_misses;
      key = origin_key(request);
    }
    auto fast = remembered_route(key);
    if (fast == ProtocolState::H1Only) {
      ++stats_.h1_cached_routes;
      store_tls_route(this, request.url, std::move(key),
                      CachedRouteProtocol::H1Only,
                      cache_generation_.load(std::memory_order_relaxed));
      co_return co_await h1_.async_request(std::move(request));
    }
    if (fast == ProtocolState::H2Available) {
      ++stats_.h2_cached_routes;
      store_tls_route(this, request.url, std::move(key),
                      CachedRouteProtocol::H2Available,
                      cache_generation_.load(std::memory_order_relaxed));
      co_return co_await request_h2(std::move(request), false);
    }

    auto gate = get_gate(key);
    auto decision = co_await gate->enter();

    switch (decision.action) {
      case GateAction::UseH2:
        ++stats_.h2_cached_routes;
        store_tls_route(this, request.url, std::move(key),
                        CachedRouteProtocol::H2Available,
                        cache_generation_.load(std::memory_order_relaxed));
        co_return co_await request_h2(std::move(request), false);
      case GateAction::UseH1:
        if (decision.overflow_fallback) {
          ++stats_.detect_overflow_to_h1;
          ++stats_.overflow_fallback;
          if (co_await gate->has_h2_origin()) {
            ++stats_.detect_overflow_to_h1_later_h2;
            ++stats_.overflow_fallback_on_h2_origin;
          }
          co_return co_await h1_.async_request(std::move(request));
        } else {
          ++stats_.h1_cached_routes;
          store_tls_route(this, request.url, std::move(key),
                          CachedRouteProtocol::H1Only,
                          cache_generation_.load(std::memory_order_relaxed));
          co_return co_await h1_.async_request(std::move(request));
        }
      case GateAction::Wait:
        ++stats_.detect_waiters;
        co_await wait_for_detection(decision.waiter);
        if (decision.waiter_state && !decision.waiter_state->done) {
          ++stats_.detect_queue_overflow;
          ++stats_.detect_overflow_to_h1;
          ++stats_.overflow_fallback;
          if (co_await gate->has_h2_origin()) {
            ++stats_.detect_overflow_to_h1_later_h2;
            ++stats_.overflow_fallback_on_h2_origin;
          }
          co_return co_await h1_.async_request(std::move(request));
        }
        co_return co_await request_auto(std::move(request));
      case GateAction::Detect:
        break;
    }

    auto response = co_await request_h2(request, false);
    if (response.error.empty()) {
      ++stats_.probe_h2_marked;
      co_await gate->record_h2_origin();
      remember_route(key, ProtocolState::H2Available);
      store_tls_route(this, request.url, std::move(key),
                      CachedRouteProtocol::H2Available,
                      cache_generation_.load(std::memory_order_relaxed));
      co_await gate->set_protocol(ProtocolState::H2Available);
      co_return response;
    }

    co_await gate->set_protocol(ProtocolState::H1Only);
    auto probe = co_await h1_.async_probe(std::move(request));
    if (probe.protocol == AsioHttpClient::ProbeProtocol::Http11 &&
        probe.response.error.empty()) {
      ++stats_.probe_h1_adopted;
      remember_route(key, ProtocolState::H1Only);
      store_tls_route(this, request.url, std::move(key),
                      CachedRouteProtocol::H1Only,
                      cache_generation_.load(std::memory_order_relaxed));
      co_return probe.response;
    }
    if (probe.protocol == AsioHttpClient::ProbeProtocol::H2) {
      ++stats_.probe_h2_marked;
      co_await gate->record_h2_origin();
      remember_route(key, ProtocolState::H2Available);
      store_tls_route(this, request.url, std::move(key),
                      CachedRouteProtocol::H2Available,
                      cache_generation_.load(std::memory_order_relaxed));
      co_await gate->set_protocol(ProtocolState::H2Available);
      co_return co_await request_h2(std::move(request), false);
    }
    ++stats_.probe_reconnect;
    co_return co_await h1_.async_request(std::move(request));
  }

  asio::awaitable<Response> request_prefer_h2(Request request) {
    auto key = origin_key(request);
    auto gate = get_gate(key);
    auto response = co_await request_h2(request, false);
    if (response.error.empty()) {
      ++stats_.probe_h2_marked;
      co_await gate->record_h2_origin();
      remember_route(key, ProtocolState::H2Available);
      co_await gate->set_protocol(ProtocolState::H2Available);
      co_return response;
    }
    co_await gate->mark_h2_failed();
    auto probe = co_await h1_.async_probe(std::move(request));
    if (probe.protocol == AsioHttpClient::ProbeProtocol::Http11 &&
        probe.response.error.empty()) {
      ++stats_.probe_h1_adopted;
      remember_route(key, ProtocolState::H1Only);
      co_return probe.response;
    }
    if (probe.protocol == AsioHttpClient::ProbeProtocol::H2) {
      ++stats_.probe_h2_marked;
      co_await gate->record_h2_origin();
      remember_route(key, ProtocolState::H2Available);
      co_await gate->set_protocol(ProtocolState::H2Available);
      co_return co_await request_h2(std::move(request), false);
    }
    ++stats_.probe_reconnect;
    co_return co_await h1_.async_request(std::move(request));
  }

  asio::awaitable<Response> request_prefer_h1(Request request) {
    auto key = origin_key(request);
    auto response = co_await h1_.async_request(request);
    if (response.error.empty()) {
      remember_route(key, ProtocolState::H1Only);
      store_tls_route(this, request.url, std::move(key),
                      CachedRouteProtocol::H1Only,
                      cache_generation_.load(std::memory_order_relaxed));
      co_await get_gate(origin_key(request))->set_protocol(ProtocolState::H1Only);
      co_return response;
    }
    co_return co_await request_h2(std::move(request), false);
  }

  asio::awaitable<Response> request_h2(Request request, bool force) {
    auto key = origin_key(request);
    auto response = co_await h2_.async_request(std::move(request),
                                               !options_.h2.verify_tls);
    if (!response.error.empty()) {
      if (force) {
        co_return response;
      }
      ++stats_.probe_reconnect;
      co_await get_gate(key)->mark_h2_failed();
    }
    co_return response;
  }

  void submit(Request request, HttpClient::ResponseHandler handler) {
    try {
      switch (request.protocol_policy) {
        case ProtocolPolicy::ForceH1:
          h1_.async_request_callback(std::move(request), std::move(handler));
          return;
        case ProtocolPolicy::Auto: {
          std::string key;
          CachedRouteProtocol cached = CachedRouteProtocol::Unknown;
          if (lookup_tls_route(this, request.url, options_.origin_cache_ttl,
                               cache_generation_.load(std::memory_order_acquire), key,
                               cached)) {
            ++stats_.url_route_cache_hits;
            if (cached == CachedRouteProtocol::H1Only) {
              ++stats_.h1_cached_routes;
              h1_.async_request_callback(std::move(request), std::move(handler));
              return;
            }
            if (cached == CachedRouteProtocol::H2Available) {
              ++stats_.h2_cached_routes;
              h2_.async_request_callback(std::move(request), std::move(handler), false);
              return;
            }
          } else {
            ++stats_.url_route_cache_misses;
            key = origin_key(request);
          }
          auto route = remembered_route(key);
          if (route == ProtocolState::H1Only || cached == CachedRouteProtocol::H1Only) {
            ++stats_.h1_cached_routes;
            store_tls_route(this, request.url, std::move(key),
                            CachedRouteProtocol::H1Only,
                            cache_generation_.load(std::memory_order_relaxed));
            h1_.async_request_callback(std::move(request), std::move(handler));
            return;
          }
          if (route == ProtocolState::H2Available ||
              cached == CachedRouteProtocol::H2Available) {
            ++stats_.h2_cached_routes;
            store_tls_route(this, request.url, std::move(key),
                            CachedRouteProtocol::H2Available,
                            cache_generation_.load(std::memory_order_relaxed));
            h2_.async_request_callback(std::move(request), std::move(handler), false);
            return;
          }
          break;
        }
        case ProtocolPolicy::ForceH2:
        case ProtocolPolicy::PreferH1:
        case ProtocolPolicy::PreferH2:
          break;
      }

      auto self = shared_from_this();
      asio::co_spawn(
          io_,
          [self, request = std::move(request),
           handler = std::move(handler)]() mutable -> asio::awaitable<void> {
            auto response = co_await self->async_request(std::move(request));
            handler(std::move(response));
          },
          asio::detached);
    } catch (const std::exception& e) {
      Response response;
      response.error = e.what();
      handler(std::move(response));
    }
  }

  std::shared_ptr<OriginGate> get_gate(const std::string& key) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = origins_.find(key);
    if (it != origins_.end()) {
      touch_cached_origin_locked(key, it->second);
      return it->second.gate;
    }
    evict_cached_origins_locked();
    auto gate = std::make_shared<OriginGate>(io_, options_);
    auto inserted = origins_.emplace(key, CachedOrigin{});
    auto& entry = inserted.first->second;
    entry.gate = gate;
    touch_cached_origin_locked(inserted.first->first, entry);
    return gate;
  }

  ProtocolState remembered_route(const std::string& key) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = origins_.find(key);
    if (it == origins_.end()) {
      return ProtocolState::Unknown;
    }
    touch_cached_origin_locked(key, it->second);
    return it->second.protocol;
  }

  void remember_route(const std::string& key, ProtocolState protocol) {
    if (protocol != ProtocolState::H1Only &&
        protocol != ProtocolState::H2Available) {
      return;
    }
    std::lock_guard<std::mutex> lk(mu_);
    evict_cached_origins_locked();
    auto inserted = origins_.try_emplace(key);
    auto& entry = inserted.first->second;
    if (!entry.gate) {
      entry.gate = std::make_shared<OriginGate>(io_, options_);
    }
    entry.protocol = protocol;
    touch_cached_origin_locked(inserted.first->first, entry);
  }

  void touch_cached_origin_locked(const std::string& key, CachedOrigin& entry) {
    entry.last_used = std::chrono::steady_clock::now();
    if (!entry.lru_linked) {
      origin_lru_.push_front(key);
      entry.lru_it = origin_lru_.begin();
      entry.lru_linked = true;
      return;
    }
    origin_lru_.splice(origin_lru_.begin(), origin_lru_, entry.lru_it);
  }

  bool cached_origin_idle(const CachedOrigin& entry) const {
    if (!entry.gate) {
      return true;
    }
    return !entry.gate->eviction_blocked.load(std::memory_order_acquire);
  }

  void evict_cached_origins_locked() {
    const auto max_origins = options_.max_cached_origins;
    if (max_origins == 0) {
      return;
    }
    const auto now = std::chrono::steady_clock::now();
    const auto ttl = options_.origin_cache_ttl;
    auto expired = [&](const CachedOrigin& entry) {
      return ttl.count() > 0 && now - entry.last_used >= ttl;
    };

    for (auto it = origin_lru_.rbegin(); it != origin_lru_.rend();) {
      auto map_it = origins_.find(*it);
      if (map_it == origins_.end()) {
        auto erase_it = std::next(it).base();
        it = std::make_reverse_iterator(origin_lru_.erase(erase_it));
        continue;
      }
      if (cached_origin_idle(map_it->second) &&
          (expired(map_it->second) || origins_.size() > max_origins)) {
        map_it->second.lru_linked = false;
        auto erase_it = std::next(it).base();
        it = std::make_reverse_iterator(origin_lru_.erase(erase_it));
        origins_.erase(map_it);
        cache_generation_.fetch_add(1, std::memory_order_acq_rel);
        continue;
      }
      if (origins_.size() <= max_origins) {
        break;
      }
      ++it;
    }
  }

  asio::awaitable<void> wait_for_detection(
      std::shared_ptr<asio::steady_timer> waiter) {
    boost::system::error_code ec;
    co_await waiter->async_wait(asio::redirect_error(asio::use_awaitable, ec));
  }

  void shutdown() {
    h2_.shutdown();
  }

  asio::awaitable<void> reset_connections() {
    co_await h1_.reset_connections();
    co_await h2_.reset_connections();
  }

  HttpClient::Stats stats() const {
    return HttpClient::Stats{
        stats_.probe_h1_adopted.load(),
        stats_.probe_h2_marked.load(),
        stats_.probe_reconnect.load(),
        stats_.overflow_fallback.load(),
        stats_.overflow_fallback_on_h2_origin.load(),
        stats_.url_route_cache_hits.load(),
        stats_.url_route_cache_misses.load(),
        stats_.h1_cached_routes.load(),
        stats_.h2_cached_routes.load(),
        stats_.detect_waiters.load(),
        stats_.detect_queue_overflow.load(),
        stats_.detect_overflow_to_h1.load(),
        stats_.detect_overflow_to_h1_later_h2.load(),
        h1_.stats(),
        h2_.stats(),
    };
  }

  asio::io_context& io_;
  Options options_;
  AsioHttpClient h1_;
  H2Client h2_;
  Counters stats_;
  std::mutex mu_;
  std::unordered_map<std::string, CachedOrigin> origins_;
  std::list<std::string> origin_lru_;
  std::atomic<std::uint64_t> cache_generation_{1};
};

HttpClient::HttpClient(asio::io_context& io)
    : HttpClient(io, Options{}) {}

HttpClient::HttpClient(asio::io_context& io, Options options)
    : impl_(std::make_shared<Impl>(io, std::move(options))) {}

HttpClient::~HttpClient() = default;

asio::awaitable<Response> HttpClient::async_request(Request request) {
  co_return co_await impl_->async_request(std::move(request));
}

void HttpClient::async_request_callback(Request request,
                                                 ResponseHandler handler) {
  impl_->submit(std::move(request), std::move(handler));
}

void HttpClient::shutdown() {
  if (impl_) {
    impl_->shutdown();
  }
}

HttpClient::Stats HttpClient::stats() const {
  return impl_->stats();
}

asio::awaitable<void> HttpClient::reset_connections() {
  co_return co_await impl_->reset_connections();
}

}  // namespace httpclient
