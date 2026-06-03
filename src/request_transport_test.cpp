#include "asyncx/asyncx.hpp"
#include "httpclient/http_client.hpp"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>

#include <cassert>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>
#include <tuple>
#include <vector>

namespace asio = boost::asio;

namespace {

struct Args {
  std::string h1_url = "https://127.0.0.1:8845/echo";
  std::string h2_url = "https://127.0.0.1:8843/echo";
};

Args parse_args(int argc, char** argv) {
  Args args;
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--h1-url" && i + 1 < argc) {
      args.h1_url = argv[++i];
    } else if (arg == "--h2-url" && i + 1 < argc) {
      args.h2_url = argv[++i];
    }
  }
  return args;
}

bool contains(const std::string& text, const std::string& needle) {
  return text.find(needle) != std::string::npos;
}

std::string delayed_ping_url(std::string url, int delay_ms) {
  auto echo = url.rfind("/echo");
  if (echo != std::string::npos) {
    url.replace(echo, 5, "/ping");
  }
  url += "?delay_ms=" + std::to_string(delay_ms);
  return url;
}

asio::awaitable<void> run_case(httpclient::HttpClient& client,
                               httpclient::Request request,
                               std::initializer_list<std::string> expected) {
  auto response = co_await client.async_request(std::move(request));
  if (!response.error.empty()) {
    std::cerr << "request error: " << response.error << "\n";
  }
  assert(response.error.empty());
  assert(response.status == 200);
  for (const auto& item : expected) {
    if (!contains(response.body, item)) {
      std::cerr << "missing expected item: " << item << "\nbody=" << response.body
                << "\n";
    }
    assert(contains(response.body, item));
  }
}

asio::awaitable<void> run(Args args) {
  httpclient::HttpClient::Options options;
  options.h1.enable_ssl_verify = false;
  options.h2.verify_tls = false;
  options.h2.sessions_per_origin = 2;
  options.detection_overflow_policy =
      httpclient::HttpClient::DetectionOverflowPolicy::WaitForDetection;

  auto ex = co_await asio::this_coro::executor;
  auto& io = static_cast<asio::io_context&>(
      const_cast<asio::execution_context&>(ex.context()));
  httpclient::HttpClient client(io, options);

  auto json = httpclient::RequestBuilder::post(args.h2_url)
                  .json(R"({"hello":"world"})")
                  .accept("application/json")
                  .protocol(httpclient::ProtocolPolicy::ForceH2)
                  .insecure()
                  .no_proxy()
                  .build();
  co_await run_case(client, std::move(json),
                    {R"("method":"POST")", R"("content_type":"application/json")",
                     R"("accept":"application/json")",
                     R"("body":"{\"hello\":\"world\"}")"});

  auto form = httpclient::RequestBuilder::put(args.h1_url)
                  .form_urlencoded({{"a", "1"}, {"space", "hello world"}})
                  .protocol(httpclient::ProtocolPolicy::ForceH1)
                  .insecure()
                  .no_proxy()
                  .build();
  co_await run_case(client, std::move(form),
                    {R"("method":"PUT")",
                     R"("content_type":"application/x-www-form-urlencoded")",
                     R"("body":"a=1\u0026space=hello+world")"});

  std::vector<httpclient::MultipartPart> parts{
      {.name = "field", .value = "value", .filename = "", .content_type = ""},
      {.name = "file",
       .value = "abc",
       .filename = "a.txt",
       .content_type = "text/plain"},
  };
  auto multipart = httpclient::RequestBuilder::post(args.h1_url)
                       .multipart(parts)
                       .protocol(httpclient::ProtocolPolicy::ForceH1)
                       .insecure()
                       .no_proxy()
                       .build();
  co_await run_case(client, std::move(multipart),
                    {R"("method":"POST")",
                     R"("content_type":"multipart/form-data; boundary=)",
                     "filename=\\\"a.txt\\\""});

  auto del = httpclient::RequestBuilder::del(args.h2_url)
                 .protocol(httpclient::ProtocolPolicy::ForceH2)
                 .insecure()
                 .no_proxy()
                 .build();
  co_await run_case(client, std::move(del), {R"("method":"DELETE")"});

  auto redirect_url = args.h1_url;
  auto echo = redirect_url.rfind("/echo");
  if (echo != std::string::npos) {
    redirect_url.replace(echo, 5, "/redirect");
  }
  auto redirect = httpclient::RequestBuilder::post(redirect_url)
                      .body("payload", "text/plain")
                      .follow_redirects()
                      .protocol(httpclient::ProtocolPolicy::ForceH1)
                      .insecure()
                      .no_proxy()
                      .build();
  auto redirect_response = co_await client.async_request(std::move(redirect));
  assert(redirect_response.error.empty());
  assert(redirect_response.status == 200);
  assert(redirect_response.redirect_count == 1);
  assert(contains(redirect_response.body, R"("method":"GET")"));

  httpclient::HttpClient::Options cookie_options = options;
  cookie_options.enable_cookie_jar = true;
  cookie_options.max_cookies_per_domain = 2;
  cookie_options.default_headers.push_back("X-Default: yes");
  cookie_options.default_query_params.push_back({"default", "1"});
  auto base_url = args.h1_url;
  echo = base_url.rfind("/echo");
  if (echo != std::string::npos) {
    base_url.resize(echo + 1);
  }
  cookie_options.base_url = base_url;
  int request_hooks = 0;
  int response_hooks = 0;
  cookie_options.request_hooks.push_back([&](httpclient::Request& req) {
    ++request_hooks;
    if (!req.header("Authorization").has_value()) {
      req.set_header("Authorization", "Bearer hook-token");
    }
  });
  cookie_options.response_hooks.push_back([&](httpclient::Response& resp) {
    ++response_hooks;
    resp.headers.push_back("X-Hook: seen");
  });
  httpclient::HttpClient cookie_client(io, cookie_options);
  auto cookie_url = args.h1_url;
  echo = cookie_url.rfind("/echo");
  if (echo != std::string::npos) {
    cookie_url.replace(echo, 5, "/set-cookie");
  }
  auto set_cookie = httpclient::RequestBuilder::get(cookie_url)
                        .protocol(httpclient::ProtocolPolicy::ForceH1)
                        .insecure()
                        .no_proxy()
                        .build();
  auto set_cookie_response = co_await cookie_client.async_request(std::move(set_cookie));
  assert(set_cookie_response.error.empty());
  for (int i = 1; i <= 3; ++i) {
    auto extra_cookie = httpclient::RequestBuilder::get(
                            cookie_url + "?name=c" + std::to_string(i) +
                            "&value=v" + std::to_string(i))
                            .protocol(httpclient::ProtocolPolicy::ForceH1)
                            .insecure()
                            .no_proxy()
                            .build();
    auto extra_cookie_response =
        co_await cookie_client.async_request(std::move(extra_cookie));
    assert(extra_cookie_response.error.empty());
  }
  auto cookie_echo = httpclient::RequestBuilder::get("echo")
                         .protocol(httpclient::ProtocolPolicy::ForceH1)
                         .insecure()
                         .no_proxy()
                         .build();
  auto limited_cookie_response =
      co_await cookie_client.async_request(std::move(cookie_echo));
  assert(limited_cookie_response.error.empty());
  assert(limited_cookie_response.status == 200);
  assert(contains(limited_cookie_response.body, R"("authorization":"Bearer hook-token")"));
  assert(contains(limited_cookie_response.body, R"("query":"default=1")"));
  assert(!contains(limited_cookie_response.body, "session=abc"));
  assert(!contains(limited_cookie_response.body, "c1=v1"));
  assert(contains(limited_cookie_response.body, "c2=v2"));
  assert(contains(limited_cookie_response.body, "c3=v3"));
  assert(request_hooks >= 2);
  assert(response_hooks >= 2);

  auto status_url = args.h1_url;
  echo = status_url.rfind("/echo");
  if (echo != std::string::npos) {
    status_url.replace(echo, 5, "/status/500");
  }
  auto status = httpclient::RequestBuilder::get(status_url)
                    .protocol(httpclient::ProtocolPolicy::ForceH1)
                    .insecure()
                    .no_proxy()
                    .build();
  auto status_response = co_await client.async_request(std::move(status));
  assert(status_response.status == 500);
  bool raised = false;
  try {
    status_response.raise_for_status();
  } catch (const httpclient::HttpStatusError&) {
    raised = true;
  }
  if (!raised) {
    std::cerr << "raise_for_status did not throw\n";
    std::exit(1);
  }

  auto gzip_url = args.h1_url;
  echo = gzip_url.rfind("/echo");
  if (echo != std::string::npos) {
    gzip_url.replace(echo, 5, "/gzip");
  }
  auto gzip = httpclient::RequestBuilder::get(gzip_url)
                  .auto_decompress()
                  .protocol(httpclient::ProtocolPolicy::ForceH1)
                  .insecure()
                  .no_proxy()
                  .build();
  auto gzip_response = co_await client.async_request(std::move(gzip));
  assert(gzip_response.error.empty());
  assert(gzip_response.body == "compressed response");
  assert(!gzip_response.header("Content-Encoding").has_value());
  assert(gzip_response.text() == "compressed response");

  std::string h1_streamed;
  auto h1_stream = httpclient::RequestBuilder::get(args.h1_url)
                       .stream_response([&](std::string_view chunk) {
                         h1_streamed.append(chunk);
                       })
                       .protocol(httpclient::ProtocolPolicy::ForceH1)
                       .insecure()
                       .no_proxy()
                       .build();
  auto h1_stream_response = co_await client.async_request(std::move(h1_stream));
  assert(h1_stream_response.error.empty());
  assert(h1_stream_response.status == 200);
  assert(h1_stream_response.body.empty());
  assert(contains(h1_streamed, R"("method":"GET")"));

  std::string h2_streamed;
  auto h2_stream = httpclient::RequestBuilder::get(args.h2_url)
                       .stream_response([&](std::string_view chunk) {
                         h2_streamed.append(chunk);
                       })
                       .protocol(httpclient::ProtocolPolicy::ForceH2)
                       .insecure()
                       .no_proxy()
                       .build();
  auto h2_stream_response = co_await client.async_request(std::move(h2_stream));
  assert(h2_stream_response.error.empty());
  assert(h2_stream_response.status == 200);
  assert(h2_stream_response.body.empty());
  assert(contains(h2_streamed, R"("method":"GET")"));

  auto before_cancel = client.stats();
  bool h1_timed_out = false;
  try {
    auto slow_h1 = httpclient::RequestBuilder::get(delayed_ping_url(args.h1_url, 200))
                       .protocol(httpclient::ProtocolPolicy::ForceH1)
                       .insecure()
                       .no_proxy()
                       .build();
    (void)co_await asyncx::wait_for(client.async_request(std::move(slow_h1)),
                                    std::chrono::milliseconds(20));
  } catch (const asyncx::TimeoutError&) {
    h1_timed_out = true;
  }
  assert(h1_timed_out);
  co_await asyncx::sleep(std::chrono::milliseconds(50));

  auto after_h1_cancel = client.stats();
  assert(after_h1_cancel.h1_pool.h1_cancelled > before_cancel.h1_pool.h1_cancelled);
  assert(after_h1_cancel.h1_pool.h1_close_on_cancel >
         before_cancel.h1_pool.h1_close_on_cancel);
  auto h1_after_cancel = httpclient::RequestBuilder::get(args.h1_url)
                             .protocol(httpclient::ProtocolPolicy::ForceH1)
                             .insecure()
                             .no_proxy()
                             .build();
  auto h1_after_cancel_response =
      co_await client.async_request(std::move(h1_after_cancel));
  assert(h1_after_cancel_response.error.empty());
  assert(h1_after_cancel_response.status == 200);

  bool h2_timed_out = false;
  try {
    auto slow_h2 = httpclient::RequestBuilder::get(delayed_ping_url(args.h2_url, 200))
                       .protocol(httpclient::ProtocolPolicy::ForceH2)
                       .insecure()
                       .no_proxy()
                       .build();
    (void)co_await asyncx::wait_for(client.async_request(std::move(slow_h2)),
                                    std::chrono::milliseconds(20));
  } catch (const asyncx::TimeoutError&) {
    h2_timed_out = true;
  }
  assert(h2_timed_out);
  co_await asyncx::sleep(std::chrono::milliseconds(50));

  auto after_h2_cancel = client.stats();
  assert(after_h2_cancel.h2_pool.streams_cancelled >
         after_h1_cancel.h2_pool.streams_cancelled);
  auto h2_after_cancel = httpclient::RequestBuilder::get(args.h2_url)
                             .protocol(httpclient::ProtocolPolicy::ForceH2)
                             .insecure()
                             .no_proxy()
                             .build();
  auto h2_after_cancel_response =
      co_await client.async_request(std::move(h2_after_cancel));
  assert(h2_after_cancel_response.error.empty());
  assert(h2_after_cancel_response.status == 200);

  httpclient::HttpClient route_cache_client(io, options);
  auto auto_h1_first = httpclient::RequestBuilder::get(args.h1_url)
                           .insecure()
                           .no_proxy()
                           .build();
  auto auto_h1_first_response =
      co_await route_cache_client.async_request(std::move(auto_h1_first));
  assert(auto_h1_first_response.error.empty());
  assert(auto_h1_first_response.status == 200);
  auto route_after_first = route_cache_client.stats();
  auto auto_h1_second = httpclient::RequestBuilder::get(args.h1_url)
                            .insecure()
                            .no_proxy()
                            .build();
  auto auto_h1_second_response =
      co_await route_cache_client.async_request(std::move(auto_h1_second));
  assert(auto_h1_second_response.error.empty());
  assert(auto_h1_second_response.status == 200);
  auto route_after_second = route_cache_client.stats();
  assert(route_after_second.url_route_cache_hits >
         route_after_first.url_route_cache_hits);

  auto preconnect_req = httpclient::RequestBuilder::get(args.h1_url)
                            .insecure()
                            .no_proxy()
                            .build();
  co_await route_cache_client.preconnect(std::move(preconnect_req), 1);
  auto preconnect_after = route_cache_client.stats();
  assert(preconnect_after.h1_pool.h1_conn_created >=
         route_after_second.h1_pool.h1_conn_created);
  auto preconnect_hit = httpclient::RequestBuilder::get(args.h1_url)
                            .insecure()
                            .no_proxy()
                            .build();
  auto preconnect_hit_response =
      co_await route_cache_client.async_request(std::move(preconnect_hit));
  assert(preconnect_hit_response.error.empty());
  assert(preconnect_hit_response.status == 200);
  auto after_preconnect_hit = route_cache_client.stats();
  assert(after_preconnect_hit.h1_pool.h1_conn_created ==
         preconnect_after.h1_pool.h1_conn_created);
  assert(after_preconnect_hit.h1_pool.h1_conn_reused >
         preconnect_after.h1_pool.h1_conn_reused);

  httpclient::HttpClient::Options h1_wait_options = options;
  h1_wait_options.h1.max_connections_per_origin = 1;
  httpclient::HttpClient h1_wait_client(io, h1_wait_options);
  auto h1_wait_before = h1_wait_client.stats();
  bool h1_wait_timed_out = false;
  try {
    auto slow_h1 = httpclient::RequestBuilder::get(delayed_ping_url(args.h1_url, 200))
                       .protocol(httpclient::ProtocolPolicy::ForceH1)
                       .insecure()
                       .no_proxy()
                       .build();
    auto queued_h1 = httpclient::RequestBuilder::get(args.h1_url)
                         .protocol(httpclient::ProtocolPolicy::ForceH1)
                         .insecure()
                         .no_proxy()
                         .build();
    auto slow_task =
        asyncx::create_task(ex, h1_wait_client.async_request(std::move(slow_h1)));
    co_await asyncx::sleep(std::chrono::milliseconds(30));
    (void)co_await asyncx::wait_for(
        h1_wait_client.async_request(std::move(queued_h1)),
        std::chrono::milliseconds(20));
    (void)co_await slow_task.await();
  } catch (const asyncx::TimeoutError&) {
    h1_wait_timed_out = true;
  }
  assert(h1_wait_timed_out);
  co_await asyncx::sleep(std::chrono::milliseconds(80));
  auto h1_wait_after = h1_wait_client.stats();
  assert(h1_wait_after.h1_pool.h1_pool_wait_cancelled >
         h1_wait_before.h1_pool.h1_pool_wait_cancelled);
  auto h1_wait_recovery = httpclient::RequestBuilder::get(args.h1_url)
                              .protocol(httpclient::ProtocolPolicy::ForceH1)
                              .insecure()
                              .no_proxy()
                              .build();
  auto h1_wait_recovery_response =
      co_await h1_wait_client.async_request(std::move(h1_wait_recovery));
  assert(h1_wait_recovery_response.error.empty());
  assert(h1_wait_recovery_response.status == 200);

  httpclient::HttpClient::Options h2_wait_options = options;
  h2_wait_options.h2.sessions_per_origin = 1;
  h2_wait_options.h2.max_concurrent_streams = 1;
  httpclient::HttpClient h2_wait_client(io, h2_wait_options);
  auto h2_wait_before = h2_wait_client.stats();
  bool h2_wait_timed_out = false;
  try {
    auto slow_h2 = httpclient::RequestBuilder::get(delayed_ping_url(args.h2_url, 200))
                       .protocol(httpclient::ProtocolPolicy::ForceH2)
                       .insecure()
                       .no_proxy()
                       .build();
    auto queued_h2 = httpclient::RequestBuilder::get(args.h2_url)
                         .protocol(httpclient::ProtocolPolicy::ForceH2)
                         .insecure()
                         .no_proxy()
                         .build();
    auto slow_task =
        asyncx::create_task(ex, h2_wait_client.async_request(std::move(slow_h2)));
    co_await asyncx::sleep(std::chrono::milliseconds(30));
    (void)co_await asyncx::wait_for(
        h2_wait_client.async_request(std::move(queued_h2)),
        std::chrono::milliseconds(20));
    (void)co_await slow_task.await();
  } catch (const asyncx::TimeoutError&) {
    h2_wait_timed_out = true;
  }
  assert(h2_wait_timed_out);
  co_await asyncx::sleep(std::chrono::milliseconds(80));
  auto h2_wait_after = h2_wait_client.stats();
  assert(h2_wait_after.h2_pool.stream_slot_wait_cancelled >
         h2_wait_before.h2_pool.stream_slot_wait_cancelled);
  auto h2_wait_recovery = httpclient::RequestBuilder::get(args.h2_url)
                              .protocol(httpclient::ProtocolPolicy::ForceH2)
                              .insecure()
                              .no_proxy()
                              .build();
  auto h2_wait_recovery_response =
      co_await h2_wait_client.async_request(std::move(h2_wait_recovery));
  assert(h2_wait_recovery_response.error.empty());
  assert(h2_wait_recovery_response.status == 200);

  client.shutdown();
  cookie_client.shutdown();
  route_cache_client.shutdown();
  h1_wait_client.shutdown();
  h2_wait_client.shutdown();
}

}  // namespace

int main(int argc, char** argv) {
  asio::io_context io;
  auto args = parse_args(argc, argv);
  asio::co_spawn(io, run(std::move(args)), asio::detached);
  io.run();
}
