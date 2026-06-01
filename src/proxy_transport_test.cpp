#include "httpclient/http_client.hpp"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>

#include <cassert>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>

namespace asio = boost::asio;

namespace {

struct Args {
  std::string http_url = "http://127.0.0.1:8980/echo";
  std::string https_url = "https://127.0.0.1:8985/echo";
  std::string h2_url = "https://127.0.0.1:8986/echo";
  std::string proxy_url = "http://127.0.0.1:8899";
  std::string auth_proxy_url = "http://user:pass@127.0.0.1:8898";
  std::string socks5_url = "socks5://127.0.0.1:8897";
  std::string https_proxy_url = "https://127.0.0.1:8896";
};

Args parse_args(int argc, char** argv) {
  Args args;
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--http-url" && i + 1 < argc) {
      args.http_url = argv[++i];
    } else if (arg == "--https-url" && i + 1 < argc) {
      args.https_url = argv[++i];
    } else if (arg == "--h2-url" && i + 1 < argc) {
      args.h2_url = argv[++i];
    } else if (arg == "--proxy-url" && i + 1 < argc) {
      args.proxy_url = argv[++i];
    } else if (arg == "--auth-proxy-url" && i + 1 < argc) {
      args.auth_proxy_url = argv[++i];
    } else if (arg == "--socks5-url" && i + 1 < argc) {
      args.socks5_url = argv[++i];
    } else if (arg == "--https-proxy-url" && i + 1 < argc) {
      args.https_proxy_url = argv[++i];
    }
  }
  return args;
}

bool contains(const std::string& text, const std::string& needle) {
  return text.find(needle) != std::string::npos;
}

void require(bool condition, std::string message) {
  if (!condition) {
    throw std::runtime_error(std::move(message));
  }
}

std::string stats_url(const std::string& proxy_url) {
  return proxy_url + "/__proxy_stats";
}

std::uint64_t json_u64(const std::string& json, const std::string& key) {
  auto pos = json.find("\"" + key + "\"");
  if (pos == std::string::npos) {
    return 0;
  }
  pos = json.find(':', pos);
  if (pos == std::string::npos) {
    return 0;
  }
  ++pos;
  while (pos < json.size() &&
         (json[pos] == ' ' || json[pos] == '\t' || json[pos] == '\n')) {
    ++pos;
  }
  std::uint64_t value = 0;
  while (pos < json.size() && json[pos] >= '0' && json[pos] <= '9') {
    value = value * 10 + static_cast<std::uint64_t>(json[pos] - '0');
    ++pos;
  }
  return value;
}

asio::awaitable<httpclient::Response> do_request(httpclient::HttpClient& client,
                                                  httpclient::Request request) {
  auto response = co_await client.async_request(std::move(request));
  if (!response.error.empty()) {
    std::cerr << "request error: " << response.error << "\n";
  }
  co_return response;
}

asio::awaitable<std::uint64_t> connect_count(httpclient::HttpClient& client,
                                             const std::string& proxy_url) {
  auto stats = co_await do_request(
      client, httpclient::RequestBuilder::get(stats_url(proxy_url))
                  .protocol(httpclient::ProtocolPolicy::ForceH1)
                  .timeout_ms(1000)
                  .no_proxy()
                  .build());
  require(stats.error.empty(), "proxy stats request failed: " + stats.error);
  require(stats.status == 200, "proxy stats status was not 200");
  co_return json_u64(stats.body, "connect");
}

asio::awaitable<void> run(Args args) {
  auto ex = co_await asio::this_coro::executor;
  auto& io = static_cast<asio::io_context&>(
      const_cast<asio::execution_context&>(ex.context()));

  httpclient::HttpClient::Options options;
  options.proxy = httpclient::ProxyConfig{args.proxy_url};
  options.h1.enable_ssl_verify = false;
  options.h2.verify_tls = false;
  httpclient::HttpClient client(io, options);

  auto via_client_proxy = co_await do_request(
      client, httpclient::RequestBuilder::get(args.http_url)
                  .protocol(httpclient::ProtocolPolicy::ForceH1)
                  .timeout_ms(3000)
                  .build());
  require(via_client_proxy.error.empty(),
          "client proxy request failed: " + via_client_proxy.error);
  require(via_client_proxy.status == 200, "client proxy status was not 200");
  require(contains(via_client_proxy.body, R"("proxy":"1")"),
          "client proxy did not forward through proxy");

  auto request_no_proxy = co_await do_request(
      client, httpclient::RequestBuilder::get(args.http_url)
                  .protocol(httpclient::ProtocolPolicy::ForceH1)
                  .timeout_ms(3000)
                  .no_proxy()
                  .build());
  require(request_no_proxy.error.empty(),
          "request no_proxy failed: " + request_no_proxy.error);
  require(request_no_proxy.status == 200, "request no_proxy status was not 200");
  require(!contains(request_no_proxy.body, R"("proxy":"1")"),
          "request no_proxy unexpectedly used proxy");

  httpclient::HttpClient::Options no_proxy_match_options;
  no_proxy_match_options.proxy = httpclient::ProxyConfig{args.proxy_url};
  no_proxy_match_options.no_proxy.push_back("127.0.0.1");
  no_proxy_match_options.h1.enable_ssl_verify = false;
  no_proxy_match_options.h2.verify_tls = false;
  httpclient::HttpClient no_proxy_match(io, no_proxy_match_options);
  auto bypassed_by_no_proxy = co_await do_request(
      no_proxy_match, httpclient::RequestBuilder::get(args.http_url)
                          .protocol(httpclient::ProtocolPolicy::ForceH1)
                          .timeout_ms(3000)
                          .build());
  require(bypassed_by_no_proxy.error.empty(),
          "Options::no_proxy request failed: " + bypassed_by_no_proxy.error);
  require(bypassed_by_no_proxy.status == 200,
          "Options::no_proxy status was not 200");
  require(!contains(bypassed_by_no_proxy.body, R"("proxy":"1")"),
          "Options::no_proxy unexpectedly used proxy");

  httpclient::HttpClient::Options override_options;
  override_options.proxy = httpclient::ProxyConfig{"http://127.0.0.1:1"};
  override_options.h1.enable_ssl_verify = false;
  override_options.h2.verify_tls = false;
  httpclient::HttpClient override_client(io, override_options);
  auto request_proxy_override = co_await do_request(
      override_client, httpclient::RequestBuilder::get(args.http_url)
                           .proxy(args.proxy_url)
                           .protocol(httpclient::ProtocolPolicy::ForceH1)
                           .timeout_ms(3000)
                           .build());
  require(request_proxy_override.error.empty(),
          "request proxy override failed: " + request_proxy_override.error);
  require(request_proxy_override.status == 200,
          "request proxy override status was not 200");
  require(contains(request_proxy_override.body, R"("proxy":"1")"),
          "request proxy override did not use proxy");

  httpclient::HttpClient::Options auth_proxy_options;
  auth_proxy_options.proxy = httpclient::ProxyConfig{args.auth_proxy_url};
  auth_proxy_options.h1.enable_ssl_verify = false;
  auth_proxy_options.h2.verify_tls = false;
  httpclient::HttpClient auth_proxy_client(io, auth_proxy_options);
  auto via_auth_proxy = co_await do_request(
      auth_proxy_client, httpclient::RequestBuilder::get(args.http_url)
                             .protocol(httpclient::ProtocolPolicy::ForceH1)
                             .timeout_ms(3000)
                             .build());
  require(via_auth_proxy.error.empty(),
          "authenticated HTTP proxy request failed: " + via_auth_proxy.error);
  require(via_auth_proxy.status == 200,
          "authenticated HTTP proxy status was not 200");
  require(contains(via_auth_proxy.body, R"("proxy":"1")"),
          "authenticated HTTP proxy did not use proxy");
  require(contains(via_auth_proxy.body, R"("proxy_auth":"Basic dXNlcjpwYXNz")"),
          "authenticated HTTP proxy did not send Proxy-Authorization to proxy");

  auto via_auth_connect = co_await do_request(
      auth_proxy_client, httpclient::RequestBuilder::get(args.https_url)
                             .protocol(httpclient::ProtocolPolicy::ForceH1)
                             .timeout_ms(3000)
                             .insecure()
                             .build());
  require(via_auth_connect.error.empty(),
          "authenticated CONNECT proxy request failed: " + via_auth_connect.error);
  require(via_auth_connect.status == 200,
          "authenticated CONNECT proxy status was not 200");
  require(contains(via_auth_connect.body, R"("proxy_auth":"")"),
          "Proxy-Authorization leaked through CONNECT tunnel to origin");

  httpclient::HttpClient::Options socks5_options;
  socks5_options.proxy = httpclient::ProxyConfig{args.socks5_url};
  socks5_options.h1.enable_ssl_verify = false;
  socks5_options.h2.verify_tls = false;
  httpclient::HttpClient socks5_client(io, socks5_options);
  auto via_socks5_http = co_await do_request(
      socks5_client, httpclient::RequestBuilder::get(args.http_url)
                         .protocol(httpclient::ProtocolPolicy::ForceH1)
                         .timeout_ms(3000)
                         .build());
  require(via_socks5_http.error.empty(),
          "SOCKS5 HTTP request failed: " + via_socks5_http.error);
  require(via_socks5_http.status == 200, "SOCKS5 HTTP status was not 200");

  auto via_socks5_h2 = co_await do_request(
      socks5_client, httpclient::RequestBuilder::get(args.h2_url)
                         .protocol(httpclient::ProtocolPolicy::ForceH2)
                         .timeout_ms(3000)
                         .insecure()
                         .build());
  require(via_socks5_h2.error.empty(),
          "SOCKS5 H2 request failed: " + via_socks5_h2.error);
  require(via_socks5_h2.status == 200, "SOCKS5 H2 status was not 200");

  httpclient::HttpClient::Options https_proxy_options;
  https_proxy_options.proxy = httpclient::ProxyConfig{args.https_proxy_url};
  https_proxy_options.h1.enable_ssl_verify = false;
  https_proxy_options.h2.verify_tls = false;
  httpclient::HttpClient https_proxy_client(io, https_proxy_options);
  auto via_https_proxy_http = co_await do_request(
      https_proxy_client, httpclient::RequestBuilder::get(args.http_url)
                              .protocol(httpclient::ProtocolPolicy::ForceH1)
                              .timeout_ms(3000)
                              .insecure()
                              .build());
  require(via_https_proxy_http.error.empty(),
          "HTTPS proxy HTTP request failed: " + via_https_proxy_http.error);
  require(via_https_proxy_http.status == 200,
          "HTTPS proxy HTTP status was not 200");
  require(contains(via_https_proxy_http.body, R"("proxy":"1")"),
          "HTTPS proxy HTTP did not use proxy");

  auto via_https_proxy_https = co_await do_request(
      https_proxy_client, httpclient::RequestBuilder::get(args.https_url)
                              .protocol(httpclient::ProtocolPolicy::ForceH1)
                              .timeout_ms(3000)
                              .insecure()
                              .build());
  require(via_https_proxy_https.error.empty(),
          "HTTPS target over HTTPS proxy failed: " +
              via_https_proxy_https.error);
  require(via_https_proxy_https.status == 200,
          "HTTPS target over HTTPS proxy status was not 200");
  require(contains(via_https_proxy_https.body, R"("proxy_auth":"")"),
          "HTTPS target over HTTPS proxy leaked Proxy-Authorization");

  setenv("HTTP_PROXY", args.proxy_url.c_str(), 1);
  httpclient::HttpClient::Options env_options;
  env_options.trust_env_proxy = true;
  env_options.h1.enable_ssl_verify = false;
  env_options.h2.verify_tls = false;
  httpclient::HttpClient env_client(io, env_options);
  auto via_env_proxy = co_await do_request(
      env_client, httpclient::RequestBuilder::get(args.http_url)
                      .protocol(httpclient::ProtocolPolicy::ForceH1)
                      .timeout_ms(3000)
                      .build());
  require(via_env_proxy.error.empty(),
          "env proxy request failed: " + via_env_proxy.error);
  require(via_env_proxy.status == 200, "env proxy status was not 200");
  require(contains(via_env_proxy.body, R"("proxy":"1")"),
          "env proxy did not use proxy");
  unsetenv("HTTP_PROXY");

  setenv("HTTP_PROXY", args.proxy_url.c_str(), 1);
  setenv("NO_PROXY", "127.0.0.1", 1);
  httpclient::HttpClient::Options env_no_proxy_options;
  env_no_proxy_options.trust_env_proxy = true;
  env_no_proxy_options.h1.enable_ssl_verify = false;
  env_no_proxy_options.h2.verify_tls = false;
  httpclient::HttpClient env_no_proxy_client(io, env_no_proxy_options);
  auto env_bypassed = co_await do_request(
      env_no_proxy_client, httpclient::RequestBuilder::get(args.http_url)
                               .protocol(httpclient::ProtocolPolicy::ForceH1)
                               .timeout_ms(3000)
                               .build());
  require(env_bypassed.error.empty(),
          "NO_PROXY bypass request failed: " + env_bypassed.error);
  require(env_bypassed.status == 200, "NO_PROXY bypass status was not 200");
  require(!contains(env_bypassed.body, R"("proxy":"1")"),
          "NO_PROXY did not bypass env proxy");
  unsetenv("HTTP_PROXY");
  unsetenv("NO_PROXY");

  auto before_connect = co_await connect_count(client, args.proxy_url);
  auto via_connect = co_await do_request(
      client, httpclient::RequestBuilder::get(args.https_url)
                  .protocol(httpclient::ProtocolPolicy::ForceH1)
                  .timeout_ms(3000)
                  .insecure()
                  .build());
  require(via_connect.error.empty(), "CONNECT request failed: " + via_connect.error);
  require(via_connect.status == 200, "CONNECT response status was not 200");
  auto after_connect = co_await connect_count(client, args.proxy_url);
  require(after_connect > before_connect, "CONNECT count did not increase");

  auto h2_proxy = co_await do_request(
      client, httpclient::RequestBuilder::get(args.https_url)
                  .protocol(httpclient::ProtocolPolicy::ForceH2)
                  .timeout_ms(1000)
                  .insecure()
                  .build());
  require(!h2_proxy.error.empty(),
          "H2 over H1-only origin proxy unexpectedly succeeded");
  require(contains(h2_proxy.error, "server did not negotiate h2"),
          "H2 over H1-only origin proxy returned unexpected error: " +
              h2_proxy.error);

  auto h2_request_proxy_override = co_await do_request(
      client, httpclient::RequestBuilder::get(args.h2_url)
                  .proxy(args.proxy_url)
                  .protocol(httpclient::ProtocolPolicy::ForceH2)
                  .timeout_ms(3000)
                  .insecure()
                  .build());
  require(h2_request_proxy_override.error.empty(),
          "request-level H2 over HTTP CONNECT proxy failed: " +
              h2_request_proxy_override.error);
  require(h2_request_proxy_override.status == 200,
          "request-level H2 over HTTP CONNECT proxy status was not 200");

  auto direct_h2_after_no_proxy = co_await do_request(
      client, httpclient::RequestBuilder::get(args.h2_url)
                  .proxy(args.proxy_url)
                  .no_proxy()
                  .protocol(httpclient::ProtocolPolicy::ForceH2)
                  .timeout_ms(3000)
                  .insecure()
                  .build());
  require(direct_h2_after_no_proxy.error.empty(),
          ".proxy().no_proxy() direct H2 failed: " +
              direct_h2_after_no_proxy.error);
  require(direct_h2_after_no_proxy.status == 200,
          ".proxy().no_proxy() direct H2 status was not 200");

  auto h2_http_proxy_again = co_await do_request(
      client, httpclient::RequestBuilder::get(args.h2_url)
                  .proxy(args.proxy_url)
                  .protocol(httpclient::ProtocolPolicy::ForceH2)
                  .timeout_ms(3000)
                  .insecure()
                  .build());
  require(h2_http_proxy_again.error.empty(),
          "second H2 over HTTP CONNECT proxy failed: " +
              h2_http_proxy_again.error);
  require(h2_http_proxy_again.status == 200,
          "second H2 over HTTP CONNECT proxy status was not 200");

  auto direct_h2_again = co_await do_request(
      client, httpclient::RequestBuilder::get(args.h2_url)
                  .no_proxy()
                  .protocol(httpclient::ProtocolPolicy::ForceH2)
                  .timeout_ms(3000)
                  .insecure()
                  .build());
  require(direct_h2_again.error.empty(),
          "direct H2 after proxied H2 failed: " + direct_h2_again.error);
  require(direct_h2_again.status == 200,
          "direct H2 after proxied H2 status was not 200");

  auto h2_https_proxy = co_await do_request(
      https_proxy_client, httpclient::RequestBuilder::get(args.h2_url)
                              .protocol(httpclient::ProtocolPolicy::ForceH2)
                              .timeout_ms(3000)
                              .insecure()
                              .build());
  require(h2_https_proxy.error.empty(),
          "H2 over HTTPS CONNECT proxy failed: " + h2_https_proxy.error);
  require(h2_https_proxy.status == 200,
          "H2 over HTTPS CONNECT proxy status was not 200");

  client.shutdown();
  no_proxy_match.shutdown();
  override_client.shutdown();
  env_client.shutdown();
  env_no_proxy_client.shutdown();
  auth_proxy_client.shutdown();
  socks5_client.shutdown();
  https_proxy_client.shutdown();
}

}  // namespace

int main(int argc, char** argv) {
  asio::io_context io;
  auto args = parse_args(argc, argv);
  asio::co_spawn(io, run(std::move(args)), asio::detached);
  io.run();
}
