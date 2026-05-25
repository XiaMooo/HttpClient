#include "httpclient/request.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cctype>
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

}  // namespace

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

RequestBuilder& RequestBuilder::body(std::string body) {
  request_.body = std::move(body);
  return *this;
}

RequestBuilder& RequestBuilder::body(std::string body, std::string content_type) {
  request_.body = std::move(body);
  request_.set_content_type(std::move(content_type));
  return *this;
}

RequestBuilder& RequestBuilder::json(std::string json_text) {
  request_.body = std::move(json_text);
  request_.set_content_type("application/json");
  return *this;
}

RequestBuilder& RequestBuilder::bytes(std::string bytes, std::string content_type) {
  return body(std::move(bytes), std::move(content_type));
}

RequestBuilder& RequestBuilder::form_urlencoded(
    std::initializer_list<std::pair<std::string_view, std::string_view>> fields) {
  request_.body = httpclient::form_urlencode(fields);
  request_.set_content_type("application/x-www-form-urlencoded");
  return *this;
}

RequestBuilder& RequestBuilder::form_urlencoded(const std::vector<FormField>& fields) {
  request_.body = httpclient::form_urlencode(fields);
  request_.set_content_type("application/x-www-form-urlencoded");
  return *this;
}

RequestBuilder& RequestBuilder::multipart(const std::vector<MultipartPart>& parts) {
  auto boundary = make_multipart_boundary();
  request_.body = multipart_form_data_body(parts, boundary);
  request_.set_content_type("multipart/form-data; boundary=" + boundary);
  return *this;
}

RequestBuilder& RequestBuilder::timeout_ms(long timeout_ms) {
  request_.timeout_ms = timeout_ms;
  return *this;
}

RequestBuilder& RequestBuilder::insecure(bool value) {
  request_.verify_peer = !value;
  request_.verify_host = !value;
  return *this;
}

RequestBuilder& RequestBuilder::no_proxy(bool value) {
  request_.disable_proxy = value;
  return *this;
}

RequestBuilder& RequestBuilder::protocol(ProtocolPolicy policy) {
  request_.protocol_policy = policy;
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
