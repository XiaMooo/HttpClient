#pragma once

#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace httpclient {

enum class ProtocolPolicy {
  Auto,
  ForceH1,
  ForceH2,
  PreferH1,
  PreferH2,
};

struct Request {
  std::string method = "GET";
  std::string url;
  std::vector<std::string> headers;
  std::string body;
  long timeout_ms = 5000;
  bool verify_peer = true;
  bool verify_host = true;
  bool disable_proxy = false;
  bool fresh_connect = false;
  bool forbid_reuse = false;
  bool store_response_body = true;
  bool store_response_headers = true;
  bool measure_total_time = true;
  ProtocolPolicy protocol_policy = ProtocolPolicy::Auto;

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
  RequestBuilder& body(std::string body);
  RequestBuilder& body(std::string body, std::string content_type);
  RequestBuilder& json(std::string json_text);
  RequestBuilder& bytes(std::string bytes, std::string content_type);
  RequestBuilder& form_urlencoded(
      std::initializer_list<std::pair<std::string_view, std::string_view>> fields);
  RequestBuilder& form_urlencoded(const std::vector<FormField>& fields);
  RequestBuilder& multipart(const std::vector<MultipartPart>& parts);
  RequestBuilder& timeout_ms(long timeout_ms);
  RequestBuilder& insecure(bool value = true);
  RequestBuilder& no_proxy(bool value = true);
  RequestBuilder& protocol(ProtocolPolicy policy);
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
std::string multipart_form_data_body(const std::vector<MultipartPart>& parts,
                                     std::string_view boundary);
std::string make_multipart_boundary();

struct Response {
  long status = 0;
  std::string body;
  std::vector<std::string> headers;
  std::string error;
  double total_time_sec = 0.0;
  double namelookup_time_sec = 0.0;
  double connect_time_sec = 0.0;
  double appconnect_time_sec = 0.0;
  double pretransfer_time_sec = 0.0;
  double starttransfer_time_sec = 0.0;
  long num_connects = 0;
  long http_version = 0;
  std::string primary_ip;
};

}  // namespace httpclient
