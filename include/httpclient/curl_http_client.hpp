#pragma once

#include "httpclient/request.hpp"

#include <cstddef>
#include <future>
#include <memory>

namespace httpclient {

class CurlHttpClient {
public:
  struct Options {
    std::size_t max_total_connections = 128;
    std::size_t max_host_connections = 16;
    bool enable_http2 = true;
    bool verbose = false;
  };

  CurlHttpClient();
  explicit CurlHttpClient(Options options);
  ~CurlHttpClient();

  CurlHttpClient(const CurlHttpClient&) = delete;
  CurlHttpClient& operator=(const CurlHttpClient&) = delete;
  CurlHttpClient(CurlHttpClient&&) = delete;
  CurlHttpClient& operator=(CurlHttpClient&&) = delete;

  std::future<Response> async_request(Request request);
  Response request(Request request);

  void shutdown();

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace httpclient
