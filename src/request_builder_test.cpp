#include "httpclient/request.hpp"

#include <cassert>
#include <memory>
#include <string>
#include <vector>

using httpclient::FormField;
using httpclient::MultipartPart;
using httpclient::RequestBuilder;

int main() {
  {
    auto req = RequestBuilder::post("https://example.test/json")
                   .json(R"({"ok":true})")
                   .accept("application/json")
                   .build();
    assert(req.method == "POST");
    assert(req.url == "https://example.test/json");
    assert(req.body == R"({"ok":true})");
    assert(req.header("content-type").value() == "application/json");
    assert(req.header("ACCEPT").value() == "application/json");
  }

  {
    auto req = RequestBuilder::put("https://example.test/form")
                   .form_urlencoded({{"a", "1"}, {"space", "hello world"},
                                     {"sym", "a+b&c"}})
                   .build();
    assert(req.method == "PUT");
    assert(req.body == "a=1&space=hello+world&sym=a%2Bb%26c");
    assert(req.header("Content-Type").value() ==
           "application/x-www-form-urlencoded");
  }

  {
    auto req = RequestBuilder::get("https://example.test/search#frag")
                   .query_param("q", "hello world")
                   .query_param("sym", "a+b&c")
                   .basic_auth("alice", "secret")
                   .build();
    assert(req.url ==
           "https://example.test/search?q=hello+world&sym=a%2Bb%26c#frag");
    assert(req.header("authorization").value() == "Basic YWxpY2U6c2VjcmV0");
  }

  {
    auto req = RequestBuilder::get("https://example.test/api")
                   .bearer_auth("token")
                   .follow_redirects()
                   .max_redirects(3)
                   .retries(2, 10)
                   .cookie_jar(false)
                   .auto_decompress()
                   .timeout(httpclient::Request::Timeout{.total_ms = 200,
                                                         .connect_ms = 50,
                                                         .read_ms = 100})
                   .build();
    assert(req.header("Authorization").value() == "Bearer token");
    assert(req.follow_redirects.value());
    assert(req.max_redirects == 3);
    assert(req.max_retries == 2);
    assert(req.retry_backoff_ms == 10);
    assert(!req.use_cookie_jar);
    assert(req.auto_decompress);
    assert(req.timeout_ms == 200);
    assert(req.timeout.connect_ms == 50);
  }

  {
    std::vector<FormField> fields{{"x", "10"}, {"y", "a/b"}};
    assert(httpclient::form_urlencode(fields) == "x=10&y=a%2Fb");
  }

  {
    std::vector<MultipartPart> parts{
        MultipartPart{.name = "name",
                      .value = "alice",
                      .filename = "",
                      .content_type = ""},
        MultipartPart{.name = "file",
                      .value = "abc",
                      .filename = "a.txt",
                      .content_type = "text/plain"},
    };
    auto body = httpclient::multipart_form_data_body(parts, "boundary-test");
    assert(body.find("--boundary-test\r\n") != std::string::npos);
    assert(body.find("name=\"name\"") != std::string::npos);
    assert(body.find("filename=\"a.txt\"") != std::string::npos);
    assert(body.find("Content-Type: text/plain\r\n") != std::string::npos);
    assert(body.ends_with("--boundary-test--\r\n"));
  }

  {
    auto req = RequestBuilder::del("https://example.test/item")
                   .header("X-Test", "1")
                   .header("x-test", "2")
                   .body("raw", "application/octet-stream")
                   .protocol(httpclient::ProtocolPolicy::ForceH1)
                   .timeout_ms(123)
                   .insecure()
                   .no_proxy()
                   .store_response(false, false)
                   .build();
    assert(req.method == "DELETE");
    assert(req.header("X-Test").value() == "2");
    assert(req.header("content-type").value() == "application/octet-stream");
    assert(req.protocol_policy == httpclient::ProtocolPolicy::ForceH1);
    assert(req.timeout_ms == 123);
    assert(!req.verify_peer && !req.verify_host);
    assert(req.disable_proxy);
    assert(!req.store_response_body && !req.store_response_headers);
  }

  {
    httpclient::Request req;
    auto shared = std::make_shared<const std::string>("shared-body");
    req.set_shared_body(shared);
    assert(req.has_body());
    assert(req.body_size() == shared->size());
    assert(req.body_view() == *shared);
    auto copied = req;
    assert(copied.body_view() == "shared-body");
    copied.body = "local-body";
    assert(copied.body_view() == "local-body");
    copied.set_body("owned-body");
    assert(!copied.shared_body);
    assert(copied.body_view() == "owned-body");
  }

  {
    auto req = RequestBuilder::get("http://example.test/proxy")
                   .proxy("http://127.0.0.1:8899")
                   .build();
    assert(req.proxy.has_value());
    assert(req.proxy->url == "http://127.0.0.1:8899");
    assert(req.proxy_override);
    assert(!req.disable_proxy);
    req = RequestBuilder::get("http://example.test/proxy")
              .proxy("http://127.0.0.1:8899")
              .no_proxy()
              .build();
    assert(req.disable_proxy);
    assert(!req.proxy.has_value());
    assert(!req.proxy_override);
  }

  {
    bool saw_chunk = false;
    auto req = RequestBuilder::get("https://example.test/stream")
                   .stream_response([&](std::string_view) {
                     saw_chunk = true;
                   })
                   .build();
    assert(!req.store_response_body);
    assert(req.on_body_chunk);
    req.on_body_chunk("x");
    assert(saw_chunk);
  }
}
