#include "asyncx/asyncx.hpp"
#include "httpclient/http_client.hpp"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>
#include <variant>

namespace asio = boost::asio;
using namespace std::chrono_literals;

namespace {

struct Args {
  std::string h1_url = "https://127.0.0.1:8945/echo";
  std::string h2_url = "https://127.0.0.1:8943/echo";
  std::size_t requests = 48;
  std::size_t concurrency = 16;
};

Args parse_args(int argc, char** argv) {
  Args args;
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--h1-url" && i + 1 < argc) {
      args.h1_url = argv[++i];
    } else if (arg == "--h2-url" && i + 1 < argc) {
      args.h2_url = argv[++i];
    } else if (arg == "--requests" && i + 1 < argc) {
      args.requests = static_cast<std::size_t>(std::max(1, std::atoi(argv[++i])));
    } else if (arg == "--concurrency" && i + 1 < argc) {
      args.concurrency =
          static_cast<std::size_t>(std::max(1, std::atoi(argv[++i])));
    }
  }
  return args;
}

std::string delayed_ping_url(std::string url, int delay_ms) {
  auto echo = url.rfind("/echo");
  if (echo != std::string::npos) {
    url.replace(echo, 5, "/ping");
  }
  url += "?delay_ms=" + std::to_string(delay_ms);
  return url;
}

httpclient::Request make_request(std::string url,
                                 httpclient::ProtocolPolicy protocol) {
  return httpclient::RequestBuilder::get(std::move(url))
      .protocol(protocol)
      .insecure()
      .no_proxy()
      .store_response(false, false)
      .build();
}

asio::awaitable<void> run_recovery(httpclient::HttpClient& client,
                                   const std::string& url,
                                   httpclient::ProtocolPolicy protocol) {
  co_await asyncx::for_each_limited(
      8, 4, [&](std::size_t) -> asio::awaitable<void> {
        auto response = co_await client.async_request(make_request(url, protocol));
        if (!response.error.empty()) {
          std::cerr << "recovery error: " << response.error << "\n";
        }
        assert(response.error.empty());
        assert(response.status == 200);
        co_return;
      },
      [](std::size_t, std::monostate) {});
}

asio::awaitable<void> cancel_many(httpclient::HttpClient& client,
                                  const std::string& url,
                                  httpclient::ProtocolPolicy protocol,
                                  std::size_t requests,
                                  std::size_t concurrency) {
  std::atomic<int> timed_out{0};
  const auto slow_url = delayed_ping_url(url, 200);
  co_await asyncx::for_each_limited(
      requests, concurrency, [&](std::size_t) -> asio::awaitable<void> {
        try {
          (void)co_await asyncx::wait_for(
              client.async_request(make_request(slow_url, protocol)), 20ms);
        } catch (const asyncx::TimeoutError&) {
          timed_out.fetch_add(1, std::memory_order_relaxed);
        }
        co_return;
      },
      [](std::size_t, std::monostate) {});

  if (timed_out.load(std::memory_order_relaxed) <
      static_cast<int>(requests / 2)) {
    std::cerr << "too few timeouts: " << timed_out.load() << " of " << requests
              << "\n";
  }
  assert(timed_out.load(std::memory_order_relaxed) >=
         static_cast<int>(requests / 2));
  co_await asyncx::sleep(250ms);
}

asio::awaitable<void> run(Args args) {
  httpclient::HttpClient::Options options;
  options.h1.enable_ssl_verify = false;
  options.h1.max_connections_per_origin = 4;
  options.h2.verify_tls = false;
  options.h2.sessions_per_origin = 1;
  options.h2.max_concurrent_streams = 4;
  options.detection_overflow_policy =
      httpclient::HttpClient::DetectionOverflowPolicy::WaitForDetection;

  auto ex = co_await asio::this_coro::executor;
  auto& io = static_cast<asio::io_context&>(
      const_cast<asio::execution_context&>(ex.context()));
  httpclient::HttpClient client(io, options);

  auto before_h1 = client.stats();
  co_await cancel_many(client, args.h1_url, httpclient::ProtocolPolicy::ForceH1,
                       args.requests, args.concurrency);
  auto after_h1 = client.stats();
  auto h1_cancel_delta =
      (after_h1.h1_pool.h1_cancelled - before_h1.h1_pool.h1_cancelled) +
      (after_h1.h1_pool.h1_pool_wait_cancelled -
       before_h1.h1_pool.h1_pool_wait_cancelled);
  if (h1_cancel_delta == 0) {
    std::cerr << "no H1 cancellation stats changed\n";
  }
  assert(h1_cancel_delta > 0);
  co_await run_recovery(client, args.h1_url, httpclient::ProtocolPolicy::ForceH1);

  auto before_h2 = client.stats();
  co_await cancel_many(client, args.h2_url, httpclient::ProtocolPolicy::ForceH2,
                       args.requests, args.concurrency);
  auto after_h2 = client.stats();
  auto h2_cancel_delta =
      (after_h2.h2_pool.streams_cancelled - before_h2.h2_pool.streams_cancelled) +
      (after_h2.h2_pool.stream_slot_wait_cancelled -
       before_h2.h2_pool.stream_slot_wait_cancelled);
  if (h2_cancel_delta == 0) {
    std::cerr << "no H2 cancellation stats changed\n";
  }
  assert(h2_cancel_delta > 0);
  co_await run_recovery(client, args.h2_url, httpclient::ProtocolPolicy::ForceH2);

  client.shutdown();
}

}  // namespace

int main(int argc, char** argv) {
  asio::io_context io;
  auto args = parse_args(argc, argv);
  asio::co_spawn(io, run(std::move(args)), asio::detached);
  io.run();
}
