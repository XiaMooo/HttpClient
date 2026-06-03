#include "httpclient/http_client.hpp"

#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/post.hpp>
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
#include <future>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <cstdlib>

#include <zlib.h>

namespace httpclient {
namespace asio = boost::asio;

namespace {

struct ParsedUrl {
  std::string scheme;
  std::string host;
  std::string port;
  std::string path;
};

ParsedUrl parse_url(const std::string& url) {
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
  out.path = path_start == std::string::npos ? "/" : url.substr(path_start);
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

ParsedUrl parse_origin(const std::string& url) {
  return parse_url(url);
}

std::string origin_key(const Request& request) {
  auto parsed = parse_origin(request.url);
  return parsed.scheme + "://" + parsed.host + ":" + parsed.port;
}

std::string lower_copy(std::string_view value) {
  std::string out;
  out.reserve(value.size());
  for (auto ch : value) {
    out.push_back(static_cast<char>(
        std::tolower(static_cast<unsigned char>(ch))));
  }
  return out;
}

std::string trim_copy(std::string_view value) {
  while (!value.empty() &&
         std::isspace(static_cast<unsigned char>(value.front()))) {
    value.remove_prefix(1);
  }
  while (!value.empty() &&
         std::isspace(static_cast<unsigned char>(value.back()))) {
    value.remove_suffix(1);
  }
  return std::string(value);
}

std::vector<std::string_view> split_semicolon(std::string_view value) {
  std::vector<std::string_view> out;
  while (!value.empty()) {
    auto pos = value.find(';');
    auto part = pos == std::string_view::npos ? value : value.substr(0, pos);
    out.push_back(part);
    if (pos == std::string_view::npos) {
      break;
    }
    value.remove_prefix(pos + 1);
  }
  return out;
}

std::pair<std::string, std::string> split_name_value(std::string_view value) {
  auto eq = value.find('=');
  if (eq == std::string_view::npos) {
    return {trim_copy(value), {}};
  }
  return {trim_copy(value.substr(0, eq)), trim_copy(value.substr(eq + 1))};
}

std::string default_cookie_path(std::string_view path) {
  if (path.empty() || path.front() != '/') {
    return "/";
  }
  auto slash = path.rfind('/');
  if (slash == 0 || slash == std::string_view::npos) {
    return "/";
  }
  return std::string(path.substr(0, slash));
}

bool domain_match(std::string_view host, std::string_view domain) {
  auto h = lower_copy(host);
  auto d = lower_copy(domain);
  if (!d.empty() && d.front() == '.') {
    d.erase(d.begin());
  }
  return h == d ||
         (h.size() > d.size() && h.compare(h.size() - d.size(), d.size(), d) == 0 &&
          h[h.size() - d.size() - 1] == '.');
}

bool path_match(std::string_view request_path, std::string_view cookie_path) {
  if (cookie_path.empty()) {
    return true;
  }
  if (request_path.rfind(cookie_path, 0) != 0) {
    return false;
  }
  return request_path.size() == cookie_path.size() ||
         cookie_path.back() == '/' || request_path[cookie_path.size()] == '/';
}

std::string resolve_redirect_url(const std::string& base,
                                 std::string_view location) {
  auto target = trim_copy(location);
  if (target.find("://") != std::string::npos) {
    return target;
  }
  auto parsed = parse_url(base);
  const auto origin = parsed.scheme + "://" + parsed.host +
                      ((parsed.scheme == "https" && parsed.port == "443") ||
                               (parsed.scheme == "http" && parsed.port == "80")
                           ? ""
                           : ":" + parsed.port);
  if (target.rfind("//", 0) == 0) {
    return parsed.scheme + ":" + target;
  }
  if (!target.empty() && target.front() == '/') {
    return origin + target;
  }
  auto path = parsed.path;
  auto query = path.find('?');
  if (query != std::string::npos) {
    path.resize(query);
  }
  auto slash = path.rfind('/');
  auto dir = slash == std::string::npos ? "/" : path.substr(0, slash + 1);
  return origin + dir + target;
}

bool has_url_scheme(std::string_view url) {
  return url.find("://") != std::string_view::npos;
}

bool host_matches_no_proxy(std::string_view host,
                           const std::vector<std::string>& no_proxy) {
  auto h = lower_copy(host);
  for (const auto& item : no_proxy) {
    auto n = lower_copy(trim_copy(item));
    if (n.empty()) {
      continue;
    }
    if (n == "*") {
      return true;
    }
    if (!n.empty() && n.front() == '.') {
      n.erase(n.begin());
    }
    if (h == n ||
        (h.size() > n.size() &&
         h.compare(h.size() - n.size(), n.size(), n) == 0 &&
         h[h.size() - n.size() - 1] == '.')) {
      return true;
    }
  }
  return false;
}

void append_no_proxy_items(std::vector<std::string>& out, const char* value) {
  if (value == nullptr || *value == '\0') {
    return;
  }
  std::string_view text(value);
  while (!text.empty()) {
    auto comma = text.find(',');
    auto item = comma == std::string_view::npos ? text : text.substr(0, comma);
    auto trimmed = trim_copy(item);
    if (!trimmed.empty()) {
      out.push_back(std::move(trimmed));
    }
    if (comma == std::string_view::npos) {
      break;
    }
    text.remove_prefix(comma + 1);
  }
}

std::vector<std::string> effective_no_proxy(
    const std::vector<std::string>& configured) {
  auto out = configured;
  append_no_proxy_items(out, std::getenv("NO_PROXY"));
  append_no_proxy_items(out, std::getenv("no_proxy"));
  return out;
}

std::optional<ProxyConfig> env_proxy_for(const Request& request) {
  auto parsed = parse_url(request.url);
  const char* value = nullptr;
  if (parsed.scheme == "https") {
    value = std::getenv("HTTPS_PROXY");
    if (value == nullptr || *value == '\0') {
      value = std::getenv("https_proxy");
    }
  } else {
    value = std::getenv("HTTP_PROXY");
    if (value == nullptr || *value == '\0') {
      value = std::getenv("http_proxy");
    }
  }
  if (value == nullptr || *value == '\0') {
    value = std::getenv("ALL_PROXY");
  }
  if (value == nullptr || *value == '\0') {
    value = std::getenv("all_proxy");
  }
  if (value == nullptr || *value == '\0') {
    return std::nullopt;
  }
  return ProxyConfig{value};
}

std::string resolve_base_url(const std::string& base, const std::string& url) {
  if (base.empty() || has_url_scheme(url)) {
    return url;
  }
  if (url.empty()) {
    return base;
  }
  return resolve_redirect_url(base, url);
}

bool redirect_changes_to_get(long status, std::string_view method) {
  auto lower = lower_copy(method);
  return (status == 301 || status == 302 || status == 303) &&
         lower != "get" && lower != "head";
}

bool is_retry_status(long status, const std::vector<int>& statuses) {
  return std::find(statuses.begin(), statuses.end(), static_cast<int>(status)) !=
         statuses.end();
}

std::string zlib_decode(std::string_view input, int window_bits) {
  z_stream stream{};
  if (inflateInit2(&stream, window_bits) != Z_OK) {
    throw std::runtime_error("inflate init failed");
  }

  std::string out;
  out.reserve(input.size() * 2);
  std::array<char, 32768> buffer{};
  stream.next_in =
      reinterpret_cast<Bytef*>(const_cast<char*>(input.data()));
  stream.avail_in = static_cast<uInt>(input.size());

  int ret = Z_OK;
  while (ret == Z_OK) {
    stream.next_out = reinterpret_cast<Bytef*>(buffer.data());
    stream.avail_out = static_cast<uInt>(buffer.size());
    ret = inflate(&stream, Z_NO_FLUSH);
    if (ret != Z_OK && ret != Z_STREAM_END) {
      inflateEnd(&stream);
      throw std::runtime_error("inflate failed");
    }
    out.append(buffer.data(), buffer.size() - stream.avail_out);
  }
  inflateEnd(&stream);
  return out;
}

bool remove_header_case_insensitive(std::vector<std::string>& headers,
                                    std::string_view name) {
  auto old = headers.size();
  headers.erase(std::remove_if(headers.begin(), headers.end(),
                               [&](const std::string& line) {
                                 auto colon = line.find(':');
                                 auto header_name =
                                     colon == std::string::npos
                                         ? std::string_view(line)
                                         : std::string_view(line).substr(0, colon);
                                 return lower_copy(header_name) == lower_copy(name);
                               }),
                headers.end());
  return headers.size() != old;
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

  struct CookieJar {
    struct Cookie {
      std::string name;
      std::string value;
      std::string domain;
      std::string path = "/";
      bool host_only = true;
      std::chrono::steady_clock::time_point expires{};
      std::chrono::steady_clock::time_point last_used{};
    };

    explicit CookieJar(const Options& options) : options(options) {}

    void add_from_response(const Request& request, const Response& response) {
      auto parsed = parse_url(request.url);
      std::lock_guard<std::mutex> lock(mu);
      const auto now = std::chrono::steady_clock::now();
      prune_expired_locked(now);
      for (const auto& line : response.headers) {
        auto colon = line.find(':');
        if (colon == std::string::npos) {
          continue;
        }
        auto name = trim_copy(std::string_view(line).substr(0, colon));
        if (lower_copy(name) != "set-cookie") {
          continue;
        }
        auto value = trim_copy(std::string_view(line).substr(colon + 1));
        auto parts = split_semicolon(value);
        if (parts.empty()) {
          continue;
        }
        auto [cookie_name, cookie_value] = split_name_value(parts.front());
        if (cookie_name.empty()) {
          continue;
        }

        Cookie cookie;
        cookie.name = std::move(cookie_name);
        cookie.value = std::move(cookie_value);
        cookie.domain = lower_copy(parsed.host);
        cookie.path = default_cookie_path(parsed.path);
        cookie.last_used = now;

        bool delete_cookie = false;
        for (std::size_t i = 1; i < parts.size(); ++i) {
          auto [attr_name, attr_value] = split_name_value(parts[i]);
          auto attr = lower_copy(attr_name);
          if (attr == "domain" && !attr_value.empty() &&
              domain_match(parsed.host, attr_value)) {
            cookie.domain = lower_copy(attr_value);
            if (!cookie.domain.empty() && cookie.domain.front() == '.') {
              cookie.domain.erase(cookie.domain.begin());
            }
            cookie.host_only = false;
          } else if (attr == "path" && !attr_value.empty() &&
                     attr_value.front() == '/') {
            cookie.path = std::move(attr_value);
          } else if (attr == "max-age") {
            try {
              auto seconds = std::stoll(attr_value);
              if (seconds <= 0) {
                delete_cookie = true;
              } else {
                cookie.expires = now + std::chrono::seconds(seconds);
              }
            } catch (...) {
            }
          }
        }

        auto& jar = cookies[cookie.domain];
        jar.erase(std::remove_if(jar.begin(), jar.end(),
                                 [&](const Cookie& existing) {
                                   return existing.name == cookie.name &&
                                          existing.path == cookie.path;
                                 }),
                  jar.end());
        if (!delete_cookie) {
          jar.push_back(std::move(cookie));
          enforce_domain_limit_locked(jar);
        }
      }
      enforce_domain_count_locked();
    }

    std::string cookie_header(const Request& request) {
      auto parsed = parse_url(request.url);
      std::lock_guard<std::mutex> lock(mu);
      auto now = std::chrono::steady_clock::now();
      prune_expired_locked(now);
      std::string out;
      for (auto& [domain, jar] : cookies) {
        if (!domain_match(parsed.host, domain)) {
          continue;
        }
        for (auto& cookie : jar) {
          if (cookie.host_only && lower_copy(parsed.host) != cookie.domain) {
            continue;
          }
          if (cookie.expires != std::chrono::steady_clock::time_point{} &&
              now >= cookie.expires) {
            continue;
          }
          if (!path_match(parsed.path, cookie.path)) {
            continue;
          }
          cookie.last_used = now;
          if (!out.empty()) {
            out.append("; ");
          }
          out.append(cookie.name);
          out.push_back('=');
          out.append(cookie.value);
        }
      }
      return out;
    }

    void clear() {
      std::lock_guard<std::mutex> lock(mu);
      cookies.clear();
    }

    void prune_expired_locked(std::chrono::steady_clock::time_point now) {
      for (auto it = cookies.begin(); it != cookies.end();) {
        auto& jar = it->second;
        jar.erase(std::remove_if(jar.begin(), jar.end(),
                                 [&](const Cookie& cookie) {
                                   return cookie.expires !=
                                              std::chrono::steady_clock::time_point{} &&
                                          now >= cookie.expires;
                                 }),
                  jar.end());
        if (jar.empty()) {
          it = cookies.erase(it);
        } else {
          ++it;
        }
      }
    }

    void enforce_domain_limit_locked(std::vector<Cookie>& jar) {
      auto limit = options.max_cookies_per_domain;
      if (limit == 0) {
        jar.clear();
        return;
      }
      while (jar.size() > limit) {
        auto oldest = std::min_element(
            jar.begin(), jar.end(), [](const Cookie& a, const Cookie& b) {
              return a.last_used < b.last_used;
            });
        if (oldest == jar.end()) {
          break;
        }
        jar.erase(oldest);
      }
    }

    void enforce_domain_count_locked() {
      auto limit = options.max_cookie_domains;
      if (limit == 0) {
        cookies.clear();
        return;
      }
      while (cookies.size() > limit) {
        auto oldest = cookies.end();
        auto oldest_time = std::chrono::steady_clock::time_point::max();
        for (auto it = cookies.begin(); it != cookies.end(); ++it) {
          auto domain_time = std::chrono::steady_clock::time_point::max();
          for (const auto& cookie : it->second) {
            domain_time = std::min(domain_time, cookie.last_used);
          }
          if (domain_time < oldest_time) {
            oldest_time = domain_time;
            oldest = it;
          }
        }
        if (oldest == cookies.end()) {
          break;
        }
        cookies.erase(oldest);
      }
    }

    const Options& options;
    mutable std::mutex mu;
    std::unordered_map<std::string, std::vector<Cookie>> cookies;
  };

  static Options normalize_options(Options options) {
    auto hw = std::max<std::size_t>(1, std::thread::hardware_concurrency());
    switch (options.runtime_profile) {
      case HttpClient::RuntimeProfile::Auto:
        options.h1.auto_shards = true;
        options.h1.stripe_origins_across_shards = true;
        if (options.h1.shard_count == 0) {
          options.h1.shard_count = std::min<std::size_t>(16, hw);
        }
        options.h1.max_connections_per_origin =
            std::min<std::size_t>(options.h1.max_connections_per_origin, 32);
        options.h2.auto_shards = true;
        if (options.h2.shard_count == 0) {
          options.h2.shard_count = std::min<std::size_t>(4, hw);
        }
        options.h2.sessions_per_origin =
            std::max<std::size_t>(options.h2.sessions_per_origin, 4);
        break;
      case HttpClient::RuntimeProfile::Throughput:
        options.h1.auto_shards = false;
        options.h1.stripe_origins_across_shards = true;
        if (options.h1.shard_count == 0) {
          options.h1.shard_count = std::min<std::size_t>(16, hw);
        }
        options.h1.max_connections_per_origin =
            std::max<std::size_t>(options.h1.max_connections_per_origin, 512);
        options.h2.auto_shards = false;
        if (options.h2.shard_count == 0) {
          options.h2.shard_count = std::min<std::size_t>(4, hw);
        }
        options.h2.sessions_per_origin =
            std::max<std::size_t>(options.h2.sessions_per_origin, 4);
        break;
      case HttpClient::RuntimeProfile::Balanced:
        options.h1.auto_shards = false;
        options.h2.auto_shards = false;
        break;
    }
    return options;
  }

  Impl(Options options)
      : owned_io_(std::make_unique<asio::io_context>()),
        owned_work_(std::make_unique<OwnedWork>(
            asio::make_work_guard(*owned_io_))),
        owned_thread_([this] { owned_io_->run(); }),
        io_(*owned_io_),
        options_(normalize_options(std::move(options))),
        h1_(options_.h1),
        h2_(io_, options_.h2),
        cookie_jar_(options_) {}

  Impl(asio::io_context& io, Options options)
      : io_(io),
        options_(normalize_options(std::move(options))),
        h1_(options_.h1),
        h2_(io, options_.h2),
        cookie_jar_(options_) {}

  ~Impl() { shutdown(); }

  asio::awaitable<Response> async_request(Request request) {
    co_return co_await request_with_httpx_policy(std::move(request));
  }

  asio::awaitable<Response> raw_request(Request request) {
    try {
      if (request.proxy.has_value()) {
        if (request.protocol_policy != ProtocolPolicy::ForceH2 &&
            request.protocol_policy != ProtocolPolicy::PreferH2) {
          request.protocol_policy = ProtocolPolicy::ForceH1;
        }
      }
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

  asio::awaitable<Response> request_with_httpx_policy(Request request) {
    apply_defaults(request);
    const auto follow_redirects =
        request.follow_redirects.value_or(options_.follow_redirects);
    const auto max_redirects =
        request.max_redirects >= 0 ? request.max_redirects : options_.max_redirects;
    const auto max_retries =
        request.max_retries >= 0 ? request.max_retries : options_.max_retries;
    const auto retry_backoff =
        request.retry_backoff_ms >= 0
            ? std::chrono::milliseconds(request.retry_backoff_ms)
            : options_.retry_backoff;

    Response last;
    for (int attempt = 0; attempt <= max_retries; ++attempt) {
      auto current = request;
      int redirects = 0;
      for (;;) {
        if (options_.enable_cookie_jar && current.use_cookie_jar &&
            !current.header("Cookie").has_value()) {
          auto cookie = cookie_jar_.cookie_header(current);
          if (!cookie.empty()) {
            current.set_header("Cookie", std::move(cookie));
          }
        }

        auto response = co_await raw_request(current);
        response.final_url = current.url;
        response.redirect_count = redirects;

        if (current.auto_decompress && current.store_response_body &&
            response.error.empty() && !response.body.empty()) {
          decode_response_body(response);
        }

        if (options_.enable_cookie_jar && current.use_cookie_jar) {
          cookie_jar_.add_from_response(current, response);
        }
        for (auto& hook : options_.response_hooks) {
          hook(response);
        }

        if (!follow_redirects || !response.error.empty() ||
            !response.is_redirect()) {
          last = std::move(response);
          break;
        }

        auto location = response.header("Location");
        if (!location.has_value()) {
          last = std::move(response);
          break;
        }
        if (redirects >= max_redirects) {
          response.error = "too many redirects";
          last = std::move(response);
          break;
        }

        auto next = current;
        next.url = resolve_redirect_url(current.url, *location);
        if (redirect_changes_to_get(response.status, current.method)) {
          next.method = "GET";
          next.body.clear();
          next.remove_header("Content-Type");
          next.remove_header("Content-Length");
        }
        current = std::move(next);
        ++redirects;
      }

      if (attempt >= max_retries ||
          (last.error.empty() &&
           !is_retry_status(last.status, options_.retry_statuses))) {
        co_return last;
      }
      if (retry_backoff.count() > 0) {
        asio::steady_timer timer(co_await asio::this_coro::executor);
        timer.expires_after(retry_backoff);
        boost::system::error_code ec;
        co_await timer.async_wait(asio::redirect_error(asio::use_awaitable, ec));
      }
    }
    co_return last;
  }

  void apply_defaults(Request& request) {
    request.url = resolve_base_url(options_.base_url, request.url);
    if (request.timeout.total_ms < 0 && options_.timeout.total_ms >= 0) {
      request.timeout = options_.timeout;
      request.timeout_ms = options_.timeout.total_ms;
    } else if (request.timeout.total_ms >= 0) {
      request.timeout_ms = request.timeout.total_ms;
    }
    if (options_.auto_decompress) {
      request.auto_decompress = true;
    }
    for (const auto& header : options_.default_headers) {
      auto colon = header.find(':');
      if (colon == std::string::npos) {
        request.add_header_line(header);
        continue;
      }
      auto name = trim_copy(std::string_view(header).substr(0, colon));
      if (!request.header(name).has_value()) {
        request.add_header_line(header);
      }
    }
    if (!options_.default_query_params.empty()) {
      request.url = append_query_params(std::move(request.url),
                                        options_.default_query_params);
    }
    if (request.auto_decompress &&
        !request.header("Accept-Encoding").has_value()) {
      request.set_header("Accept-Encoding", "gzip, deflate");
    }
    if (!request.disable_proxy && !request.proxy.has_value()) {
      auto parsed = parse_url(request.url);
      auto no_proxy = options_.trust_env_proxy
                          ? effective_no_proxy(options_.no_proxy)
                          : options_.no_proxy;
      if (!host_matches_no_proxy(parsed.host, no_proxy)) {
        if (options_.proxy.has_value()) {
          request.proxy = options_.proxy;
          request.proxy_override = false;
        } else if (options_.trust_env_proxy) {
          request.proxy = env_proxy_for(request);
          request.proxy_override = false;
        }
      }
    }
    for (auto& hook : options_.request_hooks) {
      hook(request);
    }
  }

  static void decode_response_body(Response& response) {
    auto encoding = response.header("Content-Encoding");
    if (!encoding.has_value()) {
      return;
    }
    auto enc = lower_copy(trim_copy(*encoding));
    try {
      if (enc == "gzip" || enc == "x-gzip") {
        response.body = zlib_decode(response.body, 16 + MAX_WBITS);
      } else if (enc == "deflate") {
        try {
          response.body = zlib_decode(response.body, MAX_WBITS);
        } catch (...) {
          response.body = zlib_decode(response.body, -MAX_WBITS);
        }
      } else {
        return;
      }
      remove_header_case_insensitive(response.headers, "Content-Encoding");
      remove_header_case_insensitive(response.headers, "Content-Length");
    } catch (const std::exception& e) {
      response.error = e.what();
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

  bool needs_httpx_policy(const Request& request) const {
    const auto follow_redirects =
        request.follow_redirects.value_or(options_.follow_redirects);
    const auto max_retries =
        request.max_retries >= 0 ? request.max_retries : options_.max_retries;
    return follow_redirects || max_retries > 0 ||
           (options_.enable_cookie_jar && request.use_cookie_jar) ||
           request.proxy.has_value() || options_.proxy.has_value() ||
           options_.trust_env_proxy ||
           !options_.default_headers.empty() ||
           !options_.default_query_params.empty() ||
           !options_.base_url.empty() ||
           options_.auto_decompress || request.auto_decompress ||
           options_.timeout.total_ms >= 0 ||
           !options_.request_hooks.empty() ||
           !options_.response_hooks.empty();
  }

  void submit(Request request, HttpClient::ResponseHandler handler) {
    try {
      if (needs_httpx_policy(request)) {
        auto self = shared_from_this();
        asio::co_spawn(
            io_,
            [self, request = std::move(request),
             handler = std::move(handler)]() mutable -> asio::awaitable<void> {
              auto response = co_await self->async_request(std::move(request));
              handler(std::move(response));
            },
            asio::detached);
        return;
      }

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
    bool expected = false;
    if (!stopped_.compare_exchange_strong(expected, true)) {
      return;
    }
    h2_.shutdown();
    if (owned_work_) {
      owned_work_.reset();
    }
    if (owned_io_) {
      asio::post(*owned_io_, [] {});
      owned_io_->stop();
    }
    if (owned_thread_.joinable() &&
        owned_thread_.get_id() != std::this_thread::get_id()) {
      owned_thread_.join();
    }
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

  using OwnedWork =
      asio::executor_work_guard<asio::io_context::executor_type>;
  std::unique_ptr<asio::io_context> owned_io_;
  std::unique_ptr<OwnedWork> owned_work_;
  std::jthread owned_thread_;
  asio::io_context& io_;
  Options options_;
  AsioHttpClient h1_;
  H2Client h2_;
  Counters stats_;
  CookieJar cookie_jar_;
  std::atomic<bool> stopped_{false};
  std::mutex mu_;
  std::unordered_map<std::string, CachedOrigin> origins_;
  std::list<std::string> origin_lru_;
  std::atomic<std::uint64_t> cache_generation_{1};
};

HttpClient::HttpClient() : HttpClient(Options{}) {}

HttpClient::HttpClient(Options options)
    : impl_(std::make_shared<Impl>(std::move(options))) {}

HttpClient::HttpClient(asio::io_context& io)
    : HttpClient(io, Options{}) {}

HttpClient::HttpClient(asio::io_context& io, Options options)
    : impl_(std::make_shared<Impl>(io, std::move(options))) {}

HttpClient::~HttpClient() = default;

asio::awaitable<Response> HttpClient::async_request(Request request) {
  co_return co_await impl_->async_request(std::move(request));
}

asio::awaitable<Response> HttpClient::async_get(std::string url) {
  co_return co_await async_request(RequestBuilder::get(std::move(url)).build());
}

asio::awaitable<Response> HttpClient::async_post(std::string url,
                                                 std::string body,
                                                 std::string content_type) {
  co_return co_await async_request(RequestBuilder::post(std::move(url))
                                       .body(std::move(body),
                                             std::move(content_type))
                                       .build());
}

asio::awaitable<Response> HttpClient::async_put(std::string url,
                                                std::string body,
                                                std::string content_type) {
  co_return co_await async_request(RequestBuilder::put(std::move(url))
                                       .body(std::move(body),
                                             std::move(content_type))
                                       .build());
}

asio::awaitable<Response> HttpClient::async_del(std::string url) {
  co_return co_await async_request(RequestBuilder::del(std::move(url)).build());
}

void HttpClient::async_request_callback(Request request,
                                                 ResponseHandler handler) {
  impl_->submit(std::move(request), std::move(handler));
}

std::future<Response> HttpClient::request_async(Request request) {
  auto promise = std::make_shared<std::promise<Response>>();
  auto future = promise->get_future();
  async_request_callback(std::move(request),
                         [promise](Response response) mutable {
                           promise->set_value(std::move(response));
                         });
  return future;
}

Response HttpClient::request(Request request) {
  return request_async(std::move(request)).get();
}

Response HttpClient::get(std::string url) {
  return request(RequestBuilder::get(std::move(url)).build());
}

Response HttpClient::post(std::string url, std::string body,
                          std::string content_type) {
  return request(RequestBuilder::post(std::move(url))
                     .body(std::move(body), std::move(content_type))
                     .build());
}

Response HttpClient::put(std::string url, std::string body,
                         std::string content_type) {
  return request(RequestBuilder::put(std::move(url))
                     .body(std::move(body), std::move(content_type))
                     .build());
}

Response HttpClient::del(std::string url) {
  return request(RequestBuilder::del(std::move(url)).build());
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
