#include "httpclient/request.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cctype>
#include <array>
#include <sstream>
#include <utility>

namespace httpclient {
namespace {

bool header_name_equal(std::string_view a, std::string_view b) {
  if (a.size() != b.size()) {
    return false;
  }
  for (std::size_t i = 0; i < a.size(); ++i) {
    auto ca = static_cast<unsigned char>(a[i]);
    auto cb = static_cast<unsigned char>(b[i]);
    if (std::tolower(ca) != std::tolower(cb)) {
      return false;
    }
  }
  return true;
}

std::pair<std::string_view, std::string_view> split_header_view(
    std::string_view header) {
  auto pos = header.find(':');
  if (pos == std::string_view::npos) {
    return {header, {}};
  }
  auto value = header.substr(pos + 1);
  while (!value.empty() &&
         std::isspace(static_cast<unsigned char>(value.front()))) {
    value.remove_prefix(1);
  }
  return {header.substr(0, pos), value};
}

std::string header_line(std::string name, std::string value) {
  std::string out;
  out.reserve(name.size() + value.size() + 2);
  out.append(std::move(name));
  out.append(": ");
  out.append(std::move(value));
  return out;
}

bool is_unreserved(unsigned char c) {
  return std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~';
}

void append_urlencoded(std::string& out, std::string_view value) {
  static constexpr char hex[] = "0123456789ABCDEF";
  for (auto ch : value) {
    auto c = static_cast<unsigned char>(ch);
    if (is_unreserved(c)) {
      out.push_back(static_cast<char>(c));
    } else if (c == ' ') {
      out.push_back('+');
    } else {
      out.push_back('%');
      out.push_back(hex[c >> 4]);
      out.push_back(hex[c & 0x0F]);
    }
  }
}

std::string base64_encode(std::string_view value) {
  static constexpr char alphabet[] =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  std::string out;
  out.reserve(((value.size() + 2) / 3) * 4);
  std::size_t i = 0;
  while (i + 3 <= value.size()) {
    const auto a = static_cast<unsigned char>(value[i++]);
    const auto b = static_cast<unsigned char>(value[i++]);
    const auto c = static_cast<unsigned char>(value[i++]);
    out.push_back(alphabet[a >> 2]);
    out.push_back(alphabet[((a & 0x03) << 4) | (b >> 4)]);
    out.push_back(alphabet[((b & 0x0F) << 2) | (c >> 6)]);
    out.push_back(alphabet[c & 0x3F]);
  }
  if (i < value.size()) {
    const auto a = static_cast<unsigned char>(value[i++]);
    out.push_back(alphabet[a >> 2]);
    if (i < value.size()) {
      const auto b = static_cast<unsigned char>(value[i]);
      out.push_back(alphabet[((a & 0x03) << 4) | (b >> 4)]);
      out.push_back(alphabet[(b & 0x0F) << 2]);
      out.push_back('=');
    } else {
      out.push_back(alphabet[(a & 0x03) << 4]);
      out.push_back('=');
      out.push_back('=');
    }
  }
  return out;
}

}  // namespace

bool Request::has_body() const noexcept {
  return !body.empty() || (shared_body && !shared_body->empty());
}

std::size_t Request::body_size() const noexcept {
  return !body.empty() ? body.size() : (shared_body ? shared_body->size() : 0);
}

std::string_view Request::body_view() const noexcept {
  if (!body.empty()) {
    return body;
  }
  if (shared_body) {
    return *shared_body;
  }
  return {};
}

const char* Request::body_data() const noexcept {
  auto view = body_view();
  return view.empty() ? nullptr : view.data();
}

void Request::set_body(std::string value) {
  body = std::move(value);
  shared_body.reset();
}

void Request::set_shared_body(std::shared_ptr<const std::string> value) {
  shared_body = std::move(value);
  body.clear();
}

void Request::set_header(std::string name, std::string value) {
  remove_header(name);
  add_header(std::move(name), std::move(value));
}

void Request::add_header(std::string name, std::string value) {
  headers.push_back(header_line(std::move(name), std::move(value)));
}

void Request::add_header_line(std::string header) {
  headers.push_back(std::move(header));
}

bool Request::remove_header(std::string_view name) {
  auto old_size = headers.size();
  headers.erase(
      std::remove_if(headers.begin(), headers.end(),
                     [&](const std::string& header) {
                       auto [header_name, _] = split_header_view(header);
                       return header_name_equal(header_name, name);
                     }),
      headers.end());
  return headers.size() != old_size;
}

std::optional<std::string_view> Request::header(std::string_view name) const {
  for (const auto& line : headers) {
    auto [header_name, value] = split_header_view(line);
    if (header_name_equal(header_name, name)) {
      return value;
    }
  }
  return std::nullopt;
}

void Request::set_content_type(std::string value) {
  set_header("Content-Type", std::move(value));
}

void Request::set_accept(std::string value) {
  set_header("Accept", std::move(value));
}

RequestBuilder RequestBuilder::method(std::string method, std::string url) {
  Request request;
  request.method = std::move(method);
  request.url = std::move(url);
  return RequestBuilder(std::move(request));
}

RequestBuilder RequestBuilder::get(std::string url) {
  return method("GET", std::move(url));
}

RequestBuilder RequestBuilder::post(std::string url) {
  return method("POST", std::move(url));
}

RequestBuilder RequestBuilder::put(std::string url) {
  return method("PUT", std::move(url));
}

RequestBuilder RequestBuilder::del(std::string url) {
  return method("DELETE", std::move(url));
}

RequestBuilder RequestBuilder::patch(std::string url) {
  return method("PATCH", std::move(url));
}

RequestBuilder& RequestBuilder::method(std::string method) {
  request_.method = std::move(method);
  return *this;
}

RequestBuilder& RequestBuilder::url(std::string url) {
  request_.url = std::move(url);
  return *this;
}

RequestBuilder& RequestBuilder::header(std::string name, std::string value) {
  request_.set_header(std::move(name), std::move(value));
  return *this;
}

RequestBuilder& RequestBuilder::header_line(std::string header) {
  request_.add_header_line(std::move(header));
  return *this;
}

RequestBuilder& RequestBuilder::accept(std::string value) {
  request_.set_accept(std::move(value));
  return *this;
}

RequestBuilder& RequestBuilder::content_type(std::string value) {
  request_.set_content_type(std::move(value));
  return *this;
}

RequestBuilder& RequestBuilder::query_param(std::string name, std::string value) {
  std::vector<QueryParam> params{{std::move(name), std::move(value)}};
  request_.url = append_query_params(std::move(request_.url), params);
  return *this;
}

RequestBuilder& RequestBuilder::query_params(const std::vector<QueryParam>& params) {
  request_.url = append_query_params(std::move(request_.url), params);
  return *this;
}

RequestBuilder& RequestBuilder::basic_auth(std::string username,
                                           std::string password) {
  request_.set_header("Authorization",
                      basic_auth_value(username, password));
  return *this;
}

RequestBuilder& RequestBuilder::bearer_auth(std::string token) {
  request_.set_header("Authorization", "Bearer " + std::move(token));
  return *this;
}

RequestBuilder& RequestBuilder::body(std::string body) {
  request_.set_body(std::move(body));
  return *this;
}

RequestBuilder& RequestBuilder::body(std::string body, std::string content_type) {
  request_.set_body(std::move(body));
  request_.set_content_type(std::move(content_type));
  return *this;
}

RequestBuilder& RequestBuilder::json(std::string json_text) {
  request_.set_body(std::move(json_text));
  request_.set_content_type("application/json");
  return *this;
}

RequestBuilder& RequestBuilder::bytes(std::string bytes, std::string content_type) {
  return body(std::move(bytes), std::move(content_type));
}

RequestBuilder& RequestBuilder::form_urlencoded(
    std::initializer_list<std::pair<std::string_view, std::string_view>> fields) {
  request_.set_body(httpclient::form_urlencode(fields));
  request_.set_content_type("application/x-www-form-urlencoded");
  return *this;
}

RequestBuilder& RequestBuilder::form_urlencoded(const std::vector<FormField>& fields) {
  request_.set_body(httpclient::form_urlencode(fields));
  request_.set_content_type("application/x-www-form-urlencoded");
  return *this;
}

RequestBuilder& RequestBuilder::multipart(const std::vector<MultipartPart>& parts) {
  auto boundary = make_multipart_boundary();
  request_.set_body(multipart_form_data_body(parts, boundary));
  request_.set_content_type("multipart/form-data; boundary=" + boundary);
  return *this;
}

RequestBuilder& RequestBuilder::timeout_ms(long timeout_ms) {
  request_.timeout_ms = timeout_ms;
  request_.timeout.total_ms = timeout_ms;
  return *this;
}

RequestBuilder& RequestBuilder::timeout(Request::Timeout timeout) {
  request_.timeout = timeout;
  if (timeout.total_ms >= 0) {
    request_.timeout_ms = timeout.total_ms;
  }
  return *this;
}

RequestBuilder& RequestBuilder::insecure(bool value) {
  request_.verify_peer = !value;
  request_.verify_host = !value;
  return *this;
}

RequestBuilder& RequestBuilder::no_proxy(bool value) {
  request_.disable_proxy = value;
  if (value) {
    request_.proxy.reset();
    request_.proxy_override = false;
  }
  return *this;
}

RequestBuilder& RequestBuilder::proxy(std::string proxy_url) {
  request_.proxy = ProxyConfig{std::move(proxy_url)};
  request_.disable_proxy = false;
  request_.proxy_override = true;
  return *this;
}

RequestBuilder& RequestBuilder::protocol(ProtocolPolicy policy) {
  request_.protocol_policy = policy;
  return *this;
}

RequestBuilder& RequestBuilder::follow_redirects(bool value) {
  request_.follow_redirects = value;
  return *this;
}

RequestBuilder& RequestBuilder::max_redirects(int value) {
  request_.max_redirects = value;
  return *this;
}

RequestBuilder& RequestBuilder::retries(int max_retries, long backoff_ms) {
  request_.max_retries = max_retries;
  request_.retry_backoff_ms = backoff_ms;
  return *this;
}

RequestBuilder& RequestBuilder::cookie_jar(bool value) {
  request_.use_cookie_jar = value;
  return *this;
}

RequestBuilder& RequestBuilder::auto_decompress(bool value) {
  request_.auto_decompress = value;
  return *this;
}

RequestBuilder& RequestBuilder::on_body_chunk(BodyChunkHandler handler) {
  request_.on_body_chunk = std::move(handler);
  return *this;
}

RequestBuilder& RequestBuilder::stream_response(BodyChunkHandler handler,
                                                bool store_body) {
  request_.on_body_chunk = std::move(handler);
  request_.store_response_body = store_body;
  return *this;
}

RequestBuilder& RequestBuilder::store_response(bool body, bool headers) {
  request_.store_response_body = body;
  request_.store_response_headers = headers;
  return *this;
}

Request RequestBuilder::build() {
  return std::move(request_);
}

Request& RequestBuilder::request() {
  return request_;
}

const Request& RequestBuilder::request() const {
  return request_;
}

RequestBuilder::RequestBuilder(Request request) : request_(std::move(request)) {}

std::optional<std::string_view> Response::header(std::string_view name) const {
  for (const auto& line : headers) {
    auto [header_name, value] = split_header_view(line);
    if (header_name_equal(header_name, name)) {
      return value;
    }
  }
  return std::nullopt;
}

std::vector<std::string_view> Response::headers_named(std::string_view name) const {
  std::vector<std::string_view> out;
  for (const auto& line : headers) {
    auto [header_name, value] = split_header_view(line);
    if (header_name_equal(header_name, name)) {
      out.push_back(value);
    }
  }
  return out;
}

std::string_view Response::text() const {
  return body;
}

std::string_view Response::bytes() const {
  return body;
}

std::optional<std::string_view> Response::content_type() const {
  return header("Content-Type");
}

bool Response::is_success() const {
  return error.empty() && status >= 200 && status < 400;
}

bool Response::is_redirect() const {
  return status == 301 || status == 302 || status == 303 || status == 307 ||
         status == 308;
}

void Response::raise_for_status() const {
  if (error.empty() && status >= 400) {
    throw HttpStatusError(*this);
  }
}

HttpStatusError::HttpStatusError(const Response& response)
    : std::runtime_error("HTTP status error: " +
                         std::to_string(response.status)),
      response_(response) {}

std::string form_urlencode(
    std::initializer_list<std::pair<std::string_view, std::string_view>> fields) {
  std::string out;
  bool first = true;
  for (const auto& [name, value] : fields) {
    if (!first) {
      out.push_back('&');
    }
    first = false;
    append_urlencoded(out, name);
    out.push_back('=');
    append_urlencoded(out, value);
  }
  return out;
}

std::string form_urlencode(const std::vector<FormField>& fields) {
  std::string out;
  bool first = true;
  for (const auto& field : fields) {
    if (!first) {
      out.push_back('&');
    }
    first = false;
    append_urlencoded(out, field.name);
    out.push_back('=');
    append_urlencoded(out, field.value);
  }
  return out;
}

std::string multipart_form_data_body(const std::vector<MultipartPart>& parts,
                                     std::string_view boundary) {
  std::string out;
  for (const auto& part : parts) {
    out.append("--");
    out.append(boundary);
    out.append("\r\nContent-Disposition: form-data; name=\"");
    out.append(part.name);
    out.push_back('"');
    if (!part.filename.empty()) {
      out.append("; filename=\"");
      out.append(part.filename);
      out.push_back('"');
    }
    out.append("\r\n");
    if (!part.content_type.empty()) {
      out.append("Content-Type: ");
      out.append(part.content_type);
      out.append("\r\n");
    }
    out.append("\r\n");
    out.append(part.value);
    out.append("\r\n");
  }
  out.append("--");
  out.append(boundary);
  out.append("--\r\n");
  return out;
}

std::string append_query_params(std::string url,
                                const std::vector<QueryParam>& params) {
  if (params.empty()) {
    return url;
  }
  auto fragment = url.find('#');
  std::string suffix;
  if (fragment != std::string::npos) {
    suffix = url.substr(fragment);
    url.resize(fragment);
  }
  url.push_back(url.find('?') == std::string::npos ? '?' : '&');
  bool first = true;
  for (const auto& field : params) {
    if (!first) {
      url.push_back('&');
    }
    first = false;
    append_urlencoded(url, field.name);
    url.push_back('=');
    append_urlencoded(url, field.value);
  }
  url.append(suffix);
  return url;
}

std::string basic_auth_value(std::string_view username,
                             std::string_view password) {
  std::string raw;
  raw.reserve(username.size() + password.size() + 1);
  raw.append(username);
  raw.push_back(':');
  raw.append(password);
  return "Basic " + base64_encode(raw);
}

std::string make_multipart_boundary() {
  static std::atomic<std::uint64_t> counter{0};
  auto now = static_cast<std::uint64_t>(
      std::chrono::steady_clock::now().time_since_epoch().count());
  std::ostringstream oss;
  oss << "----httpclient-boundary-" << std::hex << now << "-"
      << counter.fetch_add(1, std::memory_order_relaxed);
  return oss.str();
}

}  // namespace httpclient
