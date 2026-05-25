#include "httpclient/curl_http_client.hpp"

#include <chrono>
#include <cstdlib>
#include <future>
#include <iostream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

using httpclient::CurlHttpClient;
using httpclient::Request;

namespace {

struct Args {
  std::string url = "https://example.com";
  int concurrency = 4;
  int requests = 16;
  int max_connections = 0;
  int max_host_connections = 0;
  int body_bytes = 0;
  int warmup = 0;
  bool insecure = false;
  bool no_proxy = false;
  bool fresh_connect = false;
  bool forbid_reuse = false;
  bool http1 = false;
  bool discard_response = false;
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
    } else if (next("--max-connections")) {
      args.max_connections = std::atoi(argv[++i]);
    } else if (next("--max-host-connections")) {
      args.max_host_connections = std::atoi(argv[++i]);
    } else if (next("--body-bytes")) {
      args.body_bytes = std::atoi(argv[++i]);
    } else if (next("--warmup")) {
      args.warmup = std::atoi(argv[++i]);
    } else if (s == "--insecure") {
      args.insecure = true;
    } else if (s == "--no-proxy") {
      args.no_proxy = true;
    } else if (s == "--http1") {
      args.http1 = true;
    } else if (s == "--fresh-connect") {
      args.fresh_connect = true;
    } else if (s == "--forbid-reuse") {
      args.forbid_reuse = true;
    } else if (s == "--discard-response") {
      args.discard_response = true;
    }
  }
  return args;
}

}  // namespace

int main(int argc, char** argv) {
  auto args = parse_args(argc, argv);
  CurlHttpClient::Options options;
  if (args.max_connections > 0) {
    options.max_total_connections =
        static_cast<std::size_t>(args.max_connections);
  }
  if (args.max_host_connections > 0) {
    options.max_host_connections =
        static_cast<std::size_t>(args.max_host_connections);
  }
  options.enable_http2 = !args.http1;
  CurlHttpClient client(options);

  auto make_request = [&] {
    Request req;
    req.url = args.url;
    req.method = args.body_bytes > 0 ? "POST" : "GET";
    if (args.body_bytes > 0) {
      req.body.assign(static_cast<std::size_t>(args.body_bytes), 'x');
    }
    req.timeout_ms = 5000;
    req.verify_peer = !args.insecure;
    req.verify_host = !args.insecure;
    req.disable_proxy = args.no_proxy;
    req.fresh_connect = args.fresh_connect;
    req.forbid_reuse = args.forbid_reuse;
    req.store_response_body = !args.discard_response;
    req.store_response_headers = !args.discard_response;
    req.headers.emplace_back("Accept: */*");
    return req;
  };

  for (int i = 0; i < args.warmup; ++i) {
    auto resp = client.request(make_request());
    if (!resp.error.empty()) {
      std::cerr << "warmup_error=" << resp.error << "\n";
      break;
    }
  }

  auto t0 = std::chrono::steady_clock::now();
  std::vector<std::future<httpclient::Response>> inflight;
  inflight.reserve(static_cast<std::size_t>(args.concurrency));
  long ok = 0;
  long fail = 0;
  double total_sec = 0.0;
  int issued = 0;

  auto collect = [&](httpclient::Response resp) {
    if (resp.error.empty() && resp.status >= 200 && resp.status < 500) {
      ++ok;
    } else {
      ++fail;
    }
    total_sec += resp.total_time_sec;
  };

  auto issue_one = [&] {
    inflight.emplace_back(client.async_request(make_request()));
    ++issued;
  };

  while (issued < args.requests &&
         static_cast<int>(inflight.size()) < args.concurrency) {
    issue_one();
  }

  while (!inflight.empty()) {
    bool collected = false;
    for (std::size_t i = 0; i < inflight.size();) {
      if (inflight[i].wait_for(std::chrono::seconds(0)) ==
          std::future_status::ready) {
        collect(inflight[i].get());
        inflight[i] = std::move(inflight.back());
        inflight.pop_back();
        collected = true;

        if (issued < args.requests) {
          issue_one();
        }
      } else {
        ++i;
      }
    }

    if (!collected) {
      std::this_thread::yield();
    }
  }

  auto t1 = std::chrono::steady_clock::now();
  auto wall_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

  std::cout << "requests=" << args.requests << "\n";
  std::cout << "ok=" << ok << " fail=" << fail << "\n";
  std::cout << "wall_ms=" << wall_ms << "\n";
  std::cout << "avg_reported_ms=" << (total_sec * 1000.0 / args.requests)
            << "\n";
  return fail == 0 ? 0 : 1;
}
