#pragma once

#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <initializer_list>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace httpclient {

using BodyChunkHandler = std::function<void(std::string_view)>;

struct ProxyConfig {
  std::string url;
};

enum class ProtocolPolicy {
  Auto,
  ForceH1,
  ForceH2,
  PreferH1,
  PreferH2,
};

struct Request {
  struct Timeout {
    long total_ms = -1;
    long connect_ms = -1;
    long read_ms = -1;
    long write_ms = -1;
    long pool_ms = -1;
  };

  std::string method = "GET";
  std::string url;
  std::vector<std::string> headers;
  std::string body;
  std::shared_ptr<const std::string> shared_body;
  long timeout_ms = 5000;
  Timeout timeout;
  bool verify_peer = true;
  bool verify_host = true;
  bool disable_proxy = false;
  std::optional<ProxyConfig> proxy;
  bool proxy_override = false;
  bool fresh_connect = false;
  bool forbid_reuse = false;
  bool store_response_body = true;
  bool store_response_headers = true;
  bool measure_total_time = true;
  BodyChunkHandler on_body_chunk;
  ProtocolPolicy protocol_policy = ProtocolPolicy::Auto;
  std::optional<bool> follow_redirects;
  int max_redirects = -1;
  int max_retries = -1;
  long retry_backoff_ms = -1;
  bool use_cookie_jar = true;
  bool auto_decompress = false;

  bool has_body() const noexcept;
  std::size_t body_size() const noexcept;
  std::string_view body_view() const noexcept;
  const char* body_data() const noexcept;
  void set_body(std::string value);
  void set_shared_body(std::shared_ptr<const std::string> value);
  void set_header(std::string name, std::string value);
  void add_header(std::string name, std::string value);
  void add_header_line(std::string header);
  bool remove_header(std::string_view name);
  std::optional<std::string_view> header(std::string_view name) const;
  void set_content_type(std::string value);
  void set_accept(std::string value);
};

struct FormField {
  std::string name;
  std::string value;
};

using QueryParam = FormField;

struct MultipartPart {
  std::string name;
  std::string value;
  std::string filename;
  std::string content_type;
};

class RequestBuilder {
public:
  static RequestBuilder method(std::string method, std::string url);
  static RequestBuilder get(std::string url);
  static RequestBuilder post(std::string url);
  static RequestBuilder put(std::string url);
  static RequestBuilder del(std::string url);
  static RequestBuilder patch(std::string url);

  RequestBuilder& method(std::string method);
  RequestBuilder& url(std::string url);
  RequestBuilder& header(std::string name, std::string value);
  RequestBuilder& header_line(std::string header);
  RequestBuilder& accept(std::string value);
  RequestBuilder& content_type(std::string value);
  RequestBuilder& query_param(std::string name, std::string value);
  RequestBuilder& query_params(const std::vector<QueryParam>& params);
  RequestBuilder& basic_auth(std::string username, std::string password);
  RequestBuilder& bearer_auth(std::string token);
  RequestBuilder& body(std::string body);
  RequestBuilder& body(std::string body, std::string content_type);
  RequestBuilder& json(std::string json_text);
  RequestBuilder& bytes(std::string bytes, std::string content_type);
  RequestBuilder& form_urlencoded(
      std::initializer_list<std::pair<std::string_view, std::string_view>> fields);
  RequestBuilder& form_urlencoded(const std::vector<FormField>& fields);
  RequestBuilder& multipart(const std::vector<MultipartPart>& parts);
  RequestBuilder& timeout_ms(long timeout_ms);
  RequestBuilder& timeout(Request::Timeout timeout);
  RequestBuilder& insecure(bool value = true);
  RequestBuilder& no_proxy(bool value = true);
  RequestBuilder& proxy(std::string proxy_url);
  RequestBuilder& protocol(ProtocolPolicy policy);
  RequestBuilder& follow_redirects(bool value = true);
  RequestBuilder& max_redirects(int value);
  RequestBuilder& retries(int max_retries, long backoff_ms = 0);
  RequestBuilder& cookie_jar(bool value = true);
  RequestBuilder& auto_decompress(bool value = true);
  RequestBuilder& on_body_chunk(BodyChunkHandler handler);
  RequestBuilder& stream_response(BodyChunkHandler handler,
                                  bool store_body = false);
  RequestBuilder& store_response(bool body, bool headers = true);
  Request build();
  Request& request();
  const Request& request() const;

private:
  explicit RequestBuilder(Request request);

  Request request_;
};

std::string form_urlencode(
    std::initializer_list<std::pair<std::string_view, std::string_view>> fields);
std::string form_urlencode(const std::vector<FormField>& fields);
std::string append_query_params(std::string url,
                                const std::vector<QueryParam>& params);
std::string basic_auth_value(std::string_view username,
                             std::string_view password);
std::string multipart_form_data_body(const std::vector<MultipartPart>& parts,
                                     std::string_view boundary);
std::string make_multipart_boundary();

struct Response {
  long status = 0;
  std::string body;
  std::vector<std::string> headers;
  std::string error;
  std::string final_url;
  int redirect_count = 0;
  double total_time_sec = 0.0;
  double namelookup_time_sec = 0.0;
  double connect_time_sec = 0.0;
  double appconnect_time_sec = 0.0;
  double pretransfer_time_sec = 0.0;
  double starttransfer_time_sec = 0.0;
  long num_connects = 0;
  long http_version = 0;
  bool reused_connection = false;
  std::string primary_ip;

  std::optional<std::string_view> header(std::string_view name) const;
  std::vector<std::string_view> headers_named(std::string_view name) const;
  std::string_view text() const;
  std::string_view bytes() const;
  std::optional<std::string_view> content_type() const;
  bool is_success() const;
  bool is_redirect() const;
  void raise_for_status() const;
};

class HttpStatusError : public std::runtime_error {
public:
  explicit HttpStatusError(const Response& response);
  const Response& response() const { return response_; }

private:
  Response response_;
};

}  // namespace httpclient
