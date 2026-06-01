#include "httpclient/http_client.hpp"

#include <cassert>
#include <future>
#include <string>

int main() {
  httpclient::HttpClient::Options options;
  options.h1.enable_ssl_verify = false;
  options.h2.verify_tls = false;
  options.h2.max_session_groups = 8;
  options.h2.session_group_idle_ttl = std::chrono::seconds(60);
  options.max_cookie_domains = 8;
  options.max_cookies_per_domain = 4;

  httpclient::HttpClient client(options);

  auto req = httpclient::RequestBuilder::get("https://127.0.0.1:1/ping")
                 .insecure()
                 .no_proxy()
                 .timeout_ms(1)
                 .build();
  auto future = client.request_async(std::move(req));
  auto response = future.get();
  assert(!response.error.empty() || response.status > 0);

  client.shutdown();
  return 0;
}
