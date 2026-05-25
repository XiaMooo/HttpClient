#include "httpclient/curl_http_client.hpp"

#include <curl/curl.h>

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/timerfd.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cerrno>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <optional>
#include <queue>
#include <sstream>
#include <stdexcept>
#include <unordered_map>

namespace httpclient {
namespace {

struct CurlGlobal {
  CurlGlobal() { curl_global_init(CURL_GLOBAL_DEFAULT); }
  ~CurlGlobal() { curl_global_cleanup(); }
};

CurlGlobal& global_curl() {
  static CurlGlobal g;
  return g;
}

std::once_flag& curl_once_flag() {
  static std::once_flag flag;
  return flag;
}

size_t write_body(char* ptr, size_t size, size_t nmemb, void* userdata) {
  auto* out = static_cast<std::string*>(userdata);
  out->append(ptr, size * nmemb);
  return size * nmemb;
}

size_t discard_body(char*, size_t size, size_t nmemb, void*) {
  return size * nmemb;
}

size_t write_header(char* ptr, size_t size, size_t nmemb, void* userdata) {
  auto* out = static_cast<std::vector<std::string>*>(userdata);
  out->emplace_back(ptr, size * nmemb);
  return size * nmemb;
}

size_t discard_header(char*, size_t size, size_t nmemb, void*) {
  return size * nmemb;
}

struct CompletedItem {
  std::promise<Response> promise;
  Request request;
};

int socket_action_from_curl(int what) {
  int events = 0;
  if (what & CURL_POLL_IN) {
    events |= EPOLLIN;
  }
  if (what & CURL_POLL_OUT) {
    events |= EPOLLOUT;
  }
  return events;
}

int action_from_epoll(uint32_t events) {
  int action = 0;
  if (events & EPOLLIN) {
    action |= CURL_CSELECT_IN;
  }
  if (events & EPOLLOUT) {
    action |= CURL_CSELECT_OUT;
  }
  if (events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
    action |= CURL_CSELECT_ERR;
  }
  return action;
}

void close_fd(int fd) {
  if (fd >= 0) {
    close(fd);
  }
}

}  // namespace

struct CurlHttpClient::Impl {
  explicit Impl(Options options)
      : options_(options), stop_(false) {
    std::call_once(curl_once_flag(), [] { (void)global_curl(); });
    worker_ = std::thread([this] { run(); });
  }

  ~Impl() { shutdown(); }

  std::future<Response> submit(Request request) {
    std::promise<Response> promise;
    auto fut = promise.get_future();
    {
      std::lock_guard<std::mutex> lk(mu_);
      queue_.push(Pending{std::move(request), std::move(promise)});
      wake_worker_locked();
    }
    cv_.notify_one();
    return fut;
  }

  Response request(Request request) {
    return submit(std::move(request)).get();
  }

  void shutdown() {
    bool expected = false;
    if (!stop_.compare_exchange_strong(expected, true)) {
      return;
    }
    {
      std::lock_guard<std::mutex> lk(mu_);
      wake_worker_locked();
    }
    if (worker_.joinable()) {
      worker_.join();
    }
  }

  struct Pending {
    Request request;
    std::promise<Response> promise;
  };

  struct SockInfo {
    curl_socket_t sock = CURL_SOCKET_BAD;
    int events = 0;
  };

  enum class EventKind : uint64_t {
    Wake = 1,
    Timer = 2,
    CurlSocket = 3,
  };

  struct LoopEvent {
    EventKind kind = EventKind::CurlSocket;
    curl_socket_t sock = CURL_SOCKET_BAD;
  };

  void run() {
    CURLM* multi = curl_multi_init();
    if (!multi) {
      fail_all("curl_multi_init failed");
      return;
    }

    epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
    wake_fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    timer_fd_ = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    if (epoll_fd_ < 0 || wake_fd_ < 0 || timer_fd_ < 0) {
      fail_all(std::strerror(errno));
      cleanup_loop_fds();
      curl_multi_cleanup(multi);
      return;
    }

    {
      std::lock_guard<std::mutex> lk(mu_);
      multi_ = multi;
    }

    add_loop_fd(wake_fd_, EventKind::Wake, EPOLLIN);
    add_loop_fd(timer_fd_, EventKind::Timer, EPOLLIN);

    curl_multi_setopt(multi, CURLMOPT_MAX_TOTAL_CONNECTIONS,
                      static_cast<long>(options_.max_total_connections));
    curl_multi_setopt(multi, CURLMOPT_MAX_HOST_CONNECTIONS,
                      static_cast<long>(options_.max_host_connections));
    curl_multi_setopt(multi, CURLMOPT_PIPELINING, CURLPIPE_MULTIPLEX);
    curl_multi_setopt(multi, CURLMOPT_MAX_CONCURRENT_STREAMS, 100L);
    curl_multi_setopt(multi, CURLMOPT_SOCKETFUNCTION, &Impl::socket_callback);
    curl_multi_setopt(multi, CURLMOPT_SOCKETDATA, this);
    curl_multi_setopt(multi, CURLMOPT_TIMERFUNCTION, &Impl::timer_callback);
    curl_multi_setopt(multi, CURLMOPT_TIMERDATA, this);

    int still_running = 0;
    CURLMcode mc = curl_multi_socket_action(multi, CURL_SOCKET_TIMEOUT, 0,
                                            &still_running);
    if (mc != CURLM_OK) {
      fail_all(curl_multi_strerror(mc));
    }

    while (true) {
      drain_queue(multi);
      read_completed(multi);

      if (stop_.load()) {
        std::lock_guard<std::mutex> lk(mu_);
        if (queue_.empty() && active_.empty()) {
          break;
        }
      }

      epoll_event events[256];
      int n = epoll_wait(epoll_fd_, events, 256, -1);
      if (n < 0) {
        if (errno == EINTR) {
          continue;
        }
        fail_all(std::strerror(errno));
        break;
      }

      for (int i = 0; i < n; ++i) {
        auto* loop_event = static_cast<LoopEvent*>(events[i].data.ptr);
        if (loop_event == nullptr) {
          continue;
        }
        if (loop_event->kind == EventKind::Wake) {
          drain_eventfd();
          drain_queue(multi);
          mc = curl_multi_socket_action(multi, CURL_SOCKET_TIMEOUT, 0,
                                        &still_running);
        } else if (loop_event->kind == EventKind::Timer) {
          drain_timerfd();
          mc = curl_multi_socket_action(multi, CURL_SOCKET_TIMEOUT, 0,
                                        &still_running);
        } else {
          mc = curl_multi_socket_action(
              multi, loop_event->sock, action_from_epoll(events[i].events),
              &still_running);
        }

        if (mc != CURLM_OK) {
          fail_all(curl_multi_strerror(mc));
          break;
        }
        read_completed(multi);
      }
    }

    {
      std::lock_guard<std::mutex> lk(mu_);
      multi_ = nullptr;
    }
    for (const auto& [sock, _] : sockets_) {
      epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, sock, nullptr);
    }
    sockets_.clear();
    cleanup_loop_fds();
    curl_multi_cleanup(multi);
  }

  static int socket_callback(CURL* easy, curl_socket_t sock, int what,
                             void* userp, void* socketp) {
    (void)easy;
    (void)socketp;
    auto* self = static_cast<Impl*>(userp);
    return self->update_socket(sock, what);
  }

  static int timer_callback(CURLM* multi, long timeout_ms, void* userp) {
    (void)multi;
    auto* self = static_cast<Impl*>(userp);
    return self->update_timer(timeout_ms);
  }

  int update_socket(curl_socket_t sock, int what) {
    if (what == CURL_POLL_REMOVE) {
      auto it = sockets_.find(sock);
      if (it != sockets_.end()) {
        epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, sock, nullptr);
        sockets_.erase(it);
      }
      return 0;
    }

    int events = socket_action_from_curl(what);
    if (events == 0) {
      return 0;
    }
    events |= EPOLLERR | EPOLLHUP | EPOLLRDHUP;

    epoll_event ev{};
    ev.events = static_cast<uint32_t>(events);
    auto [it, inserted] = sockets_.emplace(sock, SockInfo{sock, events});
    it->second.sock = sock;
    it->second.events = events;
    auto& loop_event = socket_events_[sock];
    loop_event.kind = EventKind::CurlSocket;
    loop_event.sock = sock;
    ev.data.ptr = &loop_event;

    int op = inserted ? EPOLL_CTL_ADD : EPOLL_CTL_MOD;
    if (epoll_ctl(epoll_fd_, op, sock, &ev) < 0) {
      if (op == EPOLL_CTL_MOD && errno == ENOENT) {
        epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, sock, &ev);
      }
    }
    return 0;
  }

  int update_timer(long timeout_ms) {
    itimerspec spec{};
    if (timeout_ms < 0) {
      timerfd_settime(timer_fd_, 0, &spec, nullptr);
      return 0;
    }
    if (timeout_ms == 0) {
      timeout_ms = 1;
    }

    spec.it_value.tv_sec = timeout_ms / 1000;
    spec.it_value.tv_nsec = (timeout_ms % 1000) * 1000000L;
    timerfd_settime(timer_fd_, 0, &spec, nullptr);
    return 0;
  }

  void add_loop_fd(int fd, EventKind kind, uint32_t events) {
    epoll_event ev{};
    ev.events = events;
    if (kind == EventKind::Wake) {
      wake_event_.kind = EventKind::Wake;
      ev.data.ptr = &wake_event_;
    } else {
      timer_event_.kind = EventKind::Timer;
      ev.data.ptr = &timer_event_;
    }
    epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev);
  }

  void drain_eventfd() {
    uint64_t value = 0;
    while (read(wake_fd_, &value, sizeof(value)) == sizeof(value)) {
    }
  }

  void drain_timerfd() {
    uint64_t value = 0;
    while (read(timer_fd_, &value, sizeof(value)) == sizeof(value)) {
    }
  }

  void wake_worker_locked() {
    if (wake_fd_ >= 0) {
      uint64_t one = 1;
      (void)write(wake_fd_, &one, sizeof(one));
    } else if (multi_ != nullptr) {
      curl_multi_wakeup(multi_);
    }
  }

  void cleanup_loop_fds() {
    close_fd(wake_fd_);
    close_fd(timer_fd_);
    close_fd(epoll_fd_);
    wake_fd_ = -1;
    timer_fd_ = -1;
    epoll_fd_ = -1;
  }

  void drain_queue(CURLM* multi) {
    for (;;) {
      Pending pending;
      {
        std::lock_guard<std::mutex> lk(mu_);
        if (queue_.empty()) {
          return;
        }
        pending = std::move(queue_.front());
        queue_.pop();
      }
      add_easy(multi, std::move(pending));
    }
  }

  void add_easy(CURLM* multi, Pending pending) {
    CURL* easy = curl_easy_init();
    if (!easy) {
      Response resp;
      resp.error = "curl_easy_init failed";
      pending.promise.set_value(std::move(resp));
      return;
    }

    auto* ctx = new EasyContext;
    ctx->pending = std::move(pending);

    const auto& req = ctx->pending.request;
    curl_easy_setopt(easy, CURLOPT_URL, req.url.c_str());
    curl_easy_setopt(easy, CURLOPT_CUSTOMREQUEST, req.method.c_str());
    if (req.store_response_body) {
      curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION, &write_body);
      curl_easy_setopt(easy, CURLOPT_WRITEDATA, &ctx->response.body);
    } else {
      curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION, &discard_body);
    }
    if (req.store_response_headers) {
      curl_easy_setopt(easy, CURLOPT_HEADERFUNCTION, &write_header);
      curl_easy_setopt(easy, CURLOPT_HEADERDATA, &ctx->response.headers);
    } else {
      curl_easy_setopt(easy, CURLOPT_HEADERFUNCTION, &discard_header);
    }
    curl_easy_setopt(easy, CURLOPT_PRIVATE, ctx);
    curl_easy_setopt(easy, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(easy, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(easy, CURLOPT_TIMEOUT_MS, req.timeout_ms);
    curl_easy_setopt(easy, CURLOPT_SSL_VERIFYPEER, req.verify_peer ? 1L : 0L);
    curl_easy_setopt(easy, CURLOPT_SSL_VERIFYHOST, req.verify_host ? 2L : 0L);
    if (req.fresh_connect) {
      curl_easy_setopt(easy, CURLOPT_FRESH_CONNECT, 1L);
    }
    if (req.forbid_reuse) {
      curl_easy_setopt(easy, CURLOPT_FORBID_REUSE, 1L);
    }
    if (req.disable_proxy) {
      curl_easy_setopt(easy, CURLOPT_NOPROXY, "*");
      curl_easy_setopt(easy, CURLOPT_PROXY, "");
    }

    if (!req.body.empty()) {
      curl_easy_setopt(easy, CURLOPT_POSTFIELDS, req.body.data());
      curl_easy_setopt(easy, CURLOPT_POSTFIELDSIZE,
                       static_cast<long>(req.body.size()));
    }

    if (options_.enable_http2) {
      curl_easy_setopt(easy, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2TLS);
    }

    struct curl_slist* header_list = nullptr;
    for (const auto& header : req.headers) {
      header_list = curl_slist_append(header_list, header.c_str());
    }
    if (header_list) {
      curl_easy_setopt(easy, CURLOPT_HTTPHEADER, header_list);
      ctx->header_list = header_list;
    }

    CURLMcode mc = curl_multi_add_handle(multi, easy);
    if (mc != CURLM_OK) {
      ctx->response.error = curl_multi_strerror(mc);
      ctx->pending.promise.set_value(std::move(ctx->response));
      free_easy(easy, ctx);
      return;
    }

    {
      std::lock_guard<std::mutex> lk(mu_);
      active_.emplace(easy, ctx);
    }
  }

  struct EasyContext {
    Pending pending;
    Response response;
    struct curl_slist* header_list = nullptr;
  };

  void read_completed(CURLM* multi) {
    int msgs_left = 0;
    while (CURLMsg* msg = curl_multi_info_read(multi, &msgs_left)) {
      if (msg->msg != CURLMSG_DONE) {
        continue;
      }
      CURL* easy = msg->easy_handle;
      EasyContext* ctx = nullptr;
      curl_easy_getinfo(easy, CURLINFO_PRIVATE, &ctx);
      if (!ctx) {
        curl_multi_remove_handle(multi, easy);
        curl_easy_cleanup(easy);
        continue;
      }

      curl_easy_getinfo(easy, CURLINFO_RESPONSE_CODE, &ctx->response.status);
      curl_easy_getinfo(easy, CURLINFO_TOTAL_TIME, &ctx->response.total_time_sec);
      curl_easy_getinfo(easy, CURLINFO_NAMELOOKUP_TIME,
                        &ctx->response.namelookup_time_sec);
      curl_easy_getinfo(easy, CURLINFO_CONNECT_TIME,
                        &ctx->response.connect_time_sec);
      curl_easy_getinfo(easy, CURLINFO_APPCONNECT_TIME,
                        &ctx->response.appconnect_time_sec);
      curl_easy_getinfo(easy, CURLINFO_PRETRANSFER_TIME,
                        &ctx->response.pretransfer_time_sec);
      curl_easy_getinfo(easy, CURLINFO_STARTTRANSFER_TIME,
                        &ctx->response.starttransfer_time_sec);
      curl_easy_getinfo(easy, CURLINFO_NUM_CONNECTS, &ctx->response.num_connects);
      curl_easy_getinfo(easy, CURLINFO_HTTP_VERSION, &ctx->response.http_version);

      char* ip = nullptr;
      if (curl_easy_getinfo(easy, CURLINFO_PRIMARY_IP, &ip) == CURLE_OK &&
          ip != nullptr) {
        ctx->response.primary_ip = ip;
      }

      if (msg->data.result != CURLE_OK) {
        ctx->response.error = curl_easy_strerror(msg->data.result);
      }

      ctx->pending.promise.set_value(std::move(ctx->response));

      {
        std::lock_guard<std::mutex> lk(mu_);
        active_.erase(easy);
      }

      curl_multi_remove_handle(multi, easy);
      free_easy(easy, ctx);
    }
  }

  void free_easy(CURL* easy, EasyContext* ctx) {
    if (ctx->header_list) {
      curl_slist_free_all(ctx->header_list);
    }
    curl_easy_cleanup(easy);
    delete ctx;
  }

  void fail_all(std::string error) {
    std::queue<Pending> pending;
    {
      std::lock_guard<std::mutex> lk(mu_);
      std::swap(pending, queue_);
    }
    while (!pending.empty()) {
      auto item = std::move(pending.front());
      pending.pop();
      Response resp;
      resp.error = error;
      item.promise.set_value(std::move(resp));
    }
  }

  Options options_;
  std::atomic<bool> stop_;
  std::thread worker_;
  std::mutex mu_;
  std::condition_variable cv_;
  std::queue<Pending> queue_;
  std::unordered_map<CURL*, EasyContext*> active_;
  std::unordered_map<curl_socket_t, SockInfo> sockets_;
  std::unordered_map<curl_socket_t, LoopEvent> socket_events_;
  LoopEvent wake_event_{EventKind::Wake, CURL_SOCKET_BAD};
  LoopEvent timer_event_{EventKind::Timer, CURL_SOCKET_BAD};
  CURLM* multi_ = nullptr;
  int epoll_fd_ = -1;
  int wake_fd_ = -1;
  int timer_fd_ = -1;
};

CurlHttpClient::CurlHttpClient() : CurlHttpClient(Options{}) {}

CurlHttpClient::CurlHttpClient(Options options) : impl_(std::make_unique<Impl>(options)) {}

CurlHttpClient::~CurlHttpClient() { shutdown(); }

std::future<Response> CurlHttpClient::async_request(Request request) {
  return impl_->submit(std::move(request));
}

Response CurlHttpClient::request(Request request) {
  return impl_->request(std::move(request));
}

void CurlHttpClient::shutdown() {
  if (impl_) {
    impl_->shutdown();
  }
}

}  // namespace httpclient
