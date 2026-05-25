#include "httpclient/request.hpp"

#include <cassert>
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
}
