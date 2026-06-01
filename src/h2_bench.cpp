#include "httpclient/h2_client.hpp"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstdio>
#include <functional>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>

namespace asio = boost::asio;

namespace {

struct ProcessMemoryStats {
  std::uint64_t rss_kb = 0;
  std::uint64_t peak_rss_kb = 0;
};

ProcessMemoryStats read_process_memory_stats() {
  ProcessMemoryStats stats;
#if defined(__linux__)
  FILE* file = std::fopen("/proc/self/status", "r");
  if (!file) {
    return stats;
  }
  char line[256];
  while (std::fgets(line, sizeof(line), file)) {
    std::string_view text(line);
    auto colon = text.find(':');
    if (colon == std::string_view::npos) {
      continue;
    }
    auto key = text.substr(0, colon);
    std::uint64_t value = 0;
    std::istringstream input(std::string(text.substr(colon + 1)));
    input >> value;
    if (key == "VmRSS") {
      stats.rss_kb = value;
    } else if (key == "VmHWM") {
      stats.peak_rss_kb = value;
    }
  }
  std::fclose(file);
#endif
  return stats;
}

struct Args {
  std::string url = "https://127.0.0.1:8443/ping";
  int requests = 16;
  int concurrency = 4;
  int sessions = 1;
  int max_streams = 128;
  int body_bytes = 0;
  int warmup = 0;
  int timeout_ms = 5000;
  bool insecure = false;
  bool callback_mode = false;
  bool discard_response = false;
};

Args parse_args(int argc, char** argv) {
  Args args;
  for (int i = 1; i < argc; ++i) {
    std::string_view s(argv[i]);
    auto next = [&](std::string_view key) { return i + 1 < argc && s == key; };
    if (next("--url")) {
      args.url = argv[++i];
    } else if (next("--requests")) {
      args.requests = std::atoi(argv[++i]);
    } else if (next("--concurrency")) {
      args.concurrency = std::atoi(argv[++i]);
    } else if (next("--sessions")) {
      args.sessions = std::atoi(argv[++i]);
    } else if (next("--max-streams")) {
      args.max_streams = std::atoi(argv[++i]);
    } else if (next("--body-bytes")) {
      args.body_bytes = std::atoi(argv[++i]);
    } else if (next("--warmup")) {
      args.warmup = std::atoi(argv[++i]);
    } else if (next("--timeout-ms")) {
      args.timeout_ms = std::atoi(argv[++i]);
    } else if (s == "--insecure") {
      args.insecure = true;
    } else if (s == "--callback") {
      args.callback_mode = true;
    } else if (s == "--discard-response") {
      args.discard_response = true;
    }
  }
  return args;
}

asio::awaitable<void> run(Args args, httpclient::H2Client& client) {
  auto make_request = [&](const std::shared_ptr<std::string>& payload) {
    httpclient::Request req;
    req.url = args.url;
    req.method = payload->empty() ? "GET" : "POST";
    req.body = *payload;
    req.timeout_ms = args.timeout_ms;
    req.verify_peer = !args.insecure;
    req.verify_host = !args.insecure;
    req.disable_proxy = true;
    req.store_response_body = !args.discard_response;
    req.store_response_headers = !args.discard_response;
    return req;
  };

  auto payload = std::make_shared<std::string>(
      static_cast<std::size_t>(std::max(0, args.body_bytes)), 'x');

  for (int i = 0; i < args.warmup; ++i) {
    auto resp = co_await client.async_request(make_request(payload), args.insecure);
    if (!resp.error.empty()) {
      std::cerr << "warmup_error=" << resp.error << "\n";
      break;
    }
  }

  auto start = std::chrono::steady_clock::now();
  if (args.requests == 1 && args.concurrency == 1) {
    auto resp = co_await client.async_request(make_request(payload), args.insecure);
    auto wall_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - start)
                       .count();
    if (!resp.error.empty()) {
      std::cerr << "error=" << resp.error << "\n";
    }
    std::cout << "requests=1\n";
    std::cout << "ok=" << (resp.error.empty() && resp.status >= 200 && resp.status < 500)
              << " fail=" << (!resp.error.empty() || resp.status < 200 || resp.status >= 500)
              << "\n";
    std::cout << "status=" << resp.status << "\n";
    std::cout << "wall_ms=" << wall_ms << "\n";
    client.shutdown();
    co_return;
  }

  struct State {
    std::atomic<int> issued = 0;
    std::atomic<int> ok = 0;
    std::atomic<int> fail = 0;
    std::atomic<int> done = 0;
  };
  auto state = std::make_shared<State>();
  auto ex = co_await asio::this_coro::executor;
  auto done_timer = std::make_shared<asio::steady_timer>(ex);
  done_timer->expires_at(asio::steady_timer::time_point::max());

  auto handle_response = [&, done_timer](httpclient::Response resp) {
    if (resp.error.empty() && resp.status >= 200 && resp.status < 500) {
      state->ok.fetch_add(1);
    } else {
      if (state->fail.load() < 3 && !resp.error.empty()) {
        std::cerr << "error=" << resp.error << "\n";
      }
      state->fail.fetch_add(1);
    }
  };

  if (args.callback_mode) {
    auto issue_one = std::make_shared<std::function<void()>>();
    *issue_one = [&, payload, issue_one, handle_response]() {
      int id = state->issued.fetch_add(1);
      if (id >= args.requests) {
        if (state->done.fetch_add(1) + 1 == args.concurrency) {
          done_timer->cancel();
        }
        return;
      }
      httpclient::Request req = make_request(payload);
      client.async_request_callback(
          std::move(req),
          [&, issue_one, handle_response](httpclient::Response resp) mutable {
            handle_response(std::move(resp));
            (*issue_one)();
          },
          args.insecure);
    };
    for (int i = 0; i < args.concurrency; ++i) {
      (*issue_one)();
    }
  } else {
    auto worker = [&, payload, done_timer, handle_response]() -> asio::awaitable<void> {
    for (;;) {
      int id = state->issued.fetch_add(1);
      if (id >= args.requests) break;
      httpclient::Request req = make_request(payload);
      auto resp = co_await client.async_request(std::move(req), args.insecure);
      handle_response(std::move(resp));
    }
    if (state->done.fetch_add(1) + 1 == args.concurrency) {
      done_timer->cancel();
    }
    };

    for (int i = 0; i < args.concurrency; ++i) {
      asio::co_spawn(ex, worker(), asio::detached);
    }
  }

  if (state->done.load() < args.concurrency) {
    boost::system::error_code ec;
    co_await done_timer->async_wait(asio::redirect_error(asio::use_awaitable, ec));
  }

  auto wall_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - start)
                     .count();
  auto stats = client.stats();
  auto mem = read_process_memory_stats();
  std::cout << "requests=" << args.requests << "\n";
  std::cout << "ok=" << state->ok.load() << " fail=" << state->fail.load() << "\n";
  std::cout << "wall_ms=" << wall_ms << "\n";
  std::cout << "rss_kb=" << mem.rss_kb << " peak_rss_kb=" << mem.peak_rss_kb
            << "\n";
  std::cout << "h2_streams_submitted=" << stats.streams_submitted
            << " h2_streams_completed=" << stats.streams_completed
            << " h2_streams_timed_out=" << stats.streams_timed_out
            << " h2_streams_cancelled=" << stats.streams_cancelled
            << " h2_stream_slot_waits=" << stats.stream_slot_waits
            << " h2_stream_slot_wait_cancelled="
            << stats.stream_slot_wait_cancelled
            << " h2_connect_waits=" << stats.connect_waits
            << " h2_connect_wait_cancelled=" << stats.connect_wait_cancelled
            << " h2_max_active_streams=" << stats.max_active_streams
            << " h2_max_pending_stream_waiters=" << stats.max_pending_stream_waiters
            << " h2_peer_max_streams=" << stats.peer_max_concurrent_streams
            << " h2_configured_max_streams=" << stats.configured_max_concurrent_streams
            << "\n";
  client.shutdown();
}

}  // namespace

int main(int argc, char** argv) {
  asio::io_context io;
  auto args = parse_args(argc, argv);
  httpclient::H2Client::Options options;
  options.verify_tls = !args.insecure;
  options.sessions_per_origin = static_cast<std::size_t>(std::max(1, args.sessions));
  options.max_concurrent_streams =
      static_cast<std::size_t>(std::max(1, args.max_streams));
  httpclient::H2Client client(io, options);
  asio::co_spawn(io, run(args, client), asio::detached);
  io.run();
  return 0;
}
