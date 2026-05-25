#include "httpclient/asio_http_client.hpp"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <cstdlib>
#include <iostream>
#include <algorithm>
#include <string>
#include <string_view>
#include <vector>

using httpclient::AsioHttpClient;
using httpclient::Request;
namespace asio = boost::asio;

namespace {

struct Args {
  std::string url = "https://example.com";
  int concurrency = 4;
  int requests = 16;
  int warmup = 0;
  int shards = 0;
  int h1_actors = 8;
  int body_bytes = 0;
  bool insecure = false;
  bool no_proxy = false;
  bool stripe = false;
  bool h1_actor = false;
  bool discard_response = false;
  bool beast_h1 = false;
  bool awaitable_mode = false;
  bool concurrent_warmup = false;
};

Args parse_args(int argc, char** argv) {
  Args args;
  for (int i = 1; i < argc; ++i) {
    std::string_view s(argv[i]);
    auto next = [&](std::string_view key) {
      return i + 1 < argc && s == key;
    };
    if (next("--url")) {
      args.url = argv[++i];
    } else if (next("--concurrency")) {
      args.concurrency = std::atoi(argv[++i]);
    } else if (next("--requests")) {
      args.requests = std::atoi(argv[++i]);
    } else if (next("--warmup")) {
      args.warmup = std::atoi(argv[++i]);
    } else if (next("--shards")) {
      args.shards = std::atoi(argv[++i]);
    } else if (next("--h1-actors")) {
      args.h1_actors = std::atoi(argv[++i]);
    } else if (next("--body-bytes")) {
      args.body_bytes = std::atoi(argv[++i]);
    } else if (s == "--insecure") {
      args.insecure = true;
    } else if (s == "--no-proxy") {
      args.no_proxy = true;
    } else if (s == "--stripe") {
      args.stripe = true;
    } else if (s == "--no-stripe") {
      args.stripe = false;
    } else if (s == "--h1-actor") {
      args.h1_actor = true;
    } else if (s == "--discard-response") {
      args.discard_response = true;
    } else if (s == "--beast-h1") {
      args.beast_h1 = true;
    } else if (s == "--awaitable") {
      args.awaitable_mode = true;
    } else if (s == "--concurrent-warmup") {
      args.concurrent_warmup = true;
    }
  }
  return args;
}

AsioHttpClient::Stats diff_stats(const AsioHttpClient::Stats& after,
                                 const AsioHttpClient::Stats& before) {
  return AsioHttpClient::Stats{
      after.h1_conn_created - before.h1_conn_created,
      after.h1_idle_hit - before.h1_idle_hit,
      after.h1_idle_miss - before.h1_idle_miss,
      after.h1_conn_reused - before.h1_conn_reused,
      after.h1_return_to_idle - before.h1_return_to_idle,
      after.h1_close_after_response - before.h1_close_after_response,
      after.h1_reuse_failed - before.h1_reuse_failed,
      after.h1_reconnect_after_idle - before.h1_reconnect_after_idle,
  };
}

asio::awaitable<void> run_bench(Args args) {
  AsioHttpClient::Options options;
  if (args.shards > 0) {
    options.shard_count = static_cast<std::size_t>(args.shards);
  }
  options.enable_ssl_verify = !args.insecure;
  options.stripe_origins_across_shards = args.stripe;
  options.use_h1_connection_actor = args.h1_actor;
  options.use_lightweight_h1 = !args.beast_h1;
  options.max_connections_per_origin =
      static_cast<std::size_t>(std::max(1, args.concurrency));
  options.h1_actor_connections_per_origin =
      static_cast<std::size_t>(std::max(1, args.h1_actors));
  AsioHttpClient client(options);

  auto make_request = [&](const std::shared_ptr<std::string>& payload) {
    Request req;
    req.url = args.url;
    req.method = payload->empty() ? "GET" : "POST";
    req.body = *payload;
    req.timeout_ms = 5000;
    req.verify_peer = !args.insecure;
    req.verify_host = !args.insecure;
    req.disable_proxy = args.no_proxy;
    req.store_response_body = !args.discard_response;
    req.store_response_headers = !args.discard_response;
    return req;
  };

  struct State {
    std::atomic<int> issued = 0;
    std::atomic<int> ok = 0;
    std::atomic<int> fail = 0;
    std::atomic<int> printed_errors = 0;
    std::atomic<int> completed = 0;
  };
  auto state = std::make_shared<State>();
  auto payload =
      std::make_shared<std::string>(static_cast<std::size_t>(std::max(0, args.body_bytes)),
                                    'x');

  if (args.warmup > 0 && args.concurrent_warmup) {
    struct WarmupState {
      std::atomic<int> issued = 0;
      std::atomic<int> completed = 0;
    };
    auto warmup_state = std::make_shared<WarmupState>();
    auto warmup_done = std::make_shared<asio::steady_timer>(
        co_await asio::this_coro::executor);
    warmup_done->expires_at(asio::steady_timer::time_point::max());
    auto warmup_one = std::make_shared<std::function<void()>>();
    *warmup_one = [&, warmup_state, warmup_done, payload, warmup_one]() {
      auto id = warmup_state->issued.fetch_add(1);
      if (id >= args.warmup) {
        return;
      }
      client.async_request_callback(
          make_request(payload),
          [&, warmup_state, warmup_done, warmup_one](httpclient::Response resp) mutable {
            if (!resp.error.empty() && warmup_state->completed.load() < 3) {
              std::cerr << "warmup_error=" << resp.error << "\n";
            }
            if (warmup_state->completed.fetch_add(1) + 1 == args.warmup) {
              warmup_done->cancel();
            }
            (*warmup_one)();
          });
    };
    auto starters = std::min(args.concurrency, args.warmup);
    for (int i = 0; i < starters; ++i) {
      (*warmup_one)();
    }
    if (warmup_state->completed.load() < args.warmup) {
      boost::system::error_code ec;
      co_await warmup_done->async_wait(asio::redirect_error(asio::use_awaitable, ec));
    }
  } else {
    for (int i = 0; i < args.warmup; ++i) {
      auto resp = co_await client.async_request(make_request(payload));
      if (!resp.error.empty()) {
        std::cerr << "warmup_error=" << resp.error << "\n";
        break;
      }
    }
  }

  auto stats_before = client.stats();
  auto start = std::chrono::steady_clock::now();
  auto ex = co_await asio::this_coro::executor;
  auto done_timer = std::make_shared<asio::steady_timer>(
      ex);
  done_timer->expires_at(asio::steady_timer::time_point::max());

  auto handle_response = [&, state, done_timer, ex](httpclient::Response resp) {
    if (resp.error.empty() && resp.status >= 200 && resp.status < 500) {
      state->ok.fetch_add(1);
    } else {
      if (!resp.error.empty() && state->printed_errors.fetch_add(1) < 3) {
        std::cerr << "error=" << resp.error << "\n";
      }
      state->fail.fetch_add(1);
    }
    if (state->completed.fetch_add(1) + 1 == args.requests) {
      asio::post(ex, [done_timer] { done_timer->cancel(); });
    }
  };

  if (!args.awaitable_mode) {
    auto issue_one = std::make_shared<std::function<void()>>();
    *issue_one = [&, state, payload, issue_one, handle_response]() {
      auto id = state->issued.fetch_add(1);
      if (id >= args.requests) {
        return;
      }
      Request req = make_request(payload);
      client.async_request_callback(
          std::move(req),
          [&, issue_one, handle_response](httpclient::Response resp) mutable {
            handle_response(std::move(resp));
            (*issue_one)();
          });
    };

    auto starters = std::min(args.concurrency, args.requests);
    for (int i = 0; i < starters; ++i) {
      (*issue_one)();
    }
  } else {
    auto issue_loop = [&, state, payload, done_timer]() -> asio::awaitable<void> {
      for (int id = state->issued.fetch_add(1); id < args.requests;
           id = state->issued.fetch_add(1)) {
        Request req = make_request(payload);
        auto resp = co_await client.async_request(std::move(req));
        if (resp.error.empty() && resp.status >= 200 && resp.status < 500) {
          state->ok.fetch_add(1);
        } else {
          if (!resp.error.empty() && state->printed_errors.fetch_add(1) < 3) {
            std::cerr << "error=" << resp.error << "\n";
          }
          state->fail.fetch_add(1);
        }
        if (state->completed.fetch_add(1) + 1 == args.requests) {
          done_timer->cancel();
        }
      }
    };

    auto starters = std::min(args.concurrency, args.requests);
    for (int i = 0; i < starters; ++i) {
      asio::co_spawn(ex, issue_loop(), asio::detached);
    }
  }

  if (state->completed.load() < args.requests) {
    boost::system::error_code ec;
    co_await done_timer->async_wait(asio::redirect_error(asio::use_awaitable, ec));
  }

  auto wall_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - start)
                     .count();
  std::cout << "requests=" << args.requests << "\n";
  std::cout << "ok=" << state->ok.load() << " fail=" << state->fail.load() << "\n";
  std::cout << "wall_ms=" << wall_ms << "\n";
  auto stats = diff_stats(client.stats(), stats_before);
  std::cout << "h1_conn_created=" << stats.h1_conn_created
            << " h1_idle_hit=" << stats.h1_idle_hit
            << " h1_idle_miss=" << stats.h1_idle_miss
            << " h1_conn_reused=" << stats.h1_conn_reused
            << " h1_return_to_idle=" << stats.h1_return_to_idle
            << " h1_close_after_response=" << stats.h1_close_after_response
            << " h1_reuse_failed=" << stats.h1_reuse_failed
            << " h1_reconnect_after_idle=" << stats.h1_reconnect_after_idle
            << "\n";
}

}  // namespace

int main(int argc, char** argv) {
  auto args = parse_args(argc, argv);
  asio::io_context io;
  asio::co_spawn(io, run_bench(args), asio::detached);
  io.run();
  return 0;
}
