#include "httpclient/http_client.hpp"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>

#include <cassert>
#include <iostream>
#include <string>
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

  client.shutdown();
}

}  // namespace

int main(int argc, char** argv) {
  asio::io_context io;
  auto args = parse_args(argc, argv);
  asio::co_spawn(io, run(std::move(args)), asio::detached);
  io.run();
}
