#pragma once

#include "httpclient/request.hpp"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/write.hpp>

#include <array>
#include <cstddef>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace httpclient::proxy_support {
namespace asio = boost::asio;

enum class Scheme {
  Http,
  Https,
  Socks5,
};

struct Url {
  std::string scheme;
  std::string host;
  std::string port;
  std::string username;
  std::string password;
};

struct EffectiveProxy {
  bool enabled = false;
  Url url;
  std::string authorization;
  Scheme scheme = Scheme::Http;
};

inline std::string percent_decode(std::string_view value) {
  auto hex_value = [](char ch) -> int {
    if (ch >= '0' && ch <= '9') return ch - '0';
    if (ch >= 'a' && ch <= 'f') return 10 + ch - 'a';
    if (ch >= 'A' && ch <= 'F') return 10 + ch - 'A';
    return -1;
  };
  std::string out;
  out.reserve(value.size());
  for (std::size_t i = 0; i < value.size(); ++i) {
    if (value[i] == '%' && i + 2 < value.size()) {
      auto hi = hex_value(value[i + 1]);
      auto lo = hex_value(value[i + 2]);
      if (hi >= 0 && lo >= 0) {
        out.push_back(static_cast<char>((hi << 4) | lo));
        i += 2;
        continue;
      }
    }
    out.push_back(value[i]);
  }
  return out;
}

inline std::string base64_encode(std::string_view value) {
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

inline std::string basic_auth_value(std::string_view username,
                                    std::string_view password) {
  std::string raw;
  raw.reserve(username.size() + password.size() + 1);
  raw.append(username);
  raw.push_back(':');
  raw.append(password);
  return "Basic " + base64_encode(raw);
}

inline Url parse_proxy_url(const std::string& value) {
  auto scheme_end = value.find("://");
  if (scheme_end == std::string::npos) {
    throw std::invalid_argument("proxy url must include scheme");
  }
  Url out;
  out.scheme = value.substr(0, scheme_end);
  auto rest_start = scheme_end + 3;
  auto path_start = value.find('/', rest_start);
  std::string authority =
      path_start == std::string::npos
          ? value.substr(rest_start)
          : value.substr(rest_start, path_start - rest_start);

  auto at = authority.rfind('@');
  if (at != std::string::npos) {
    auto userinfo = std::string_view(authority).substr(0, at);
    authority.erase(0, at + 1);
    auto colon = userinfo.find(':');
    if (colon == std::string_view::npos) {
      out.username = percent_decode(userinfo);
    } else {
      out.username = percent_decode(userinfo.substr(0, colon));
      out.password = percent_decode(userinfo.substr(colon + 1));
    }
  }

  auto colon = authority.rfind(':');
  if (colon == std::string::npos) {
    out.host = authority;
    if (out.scheme == "https") {
      out.port = "443";
    } else {
      out.port = "1080";
    }
    if (out.scheme == "http") {
      out.port = "80";
    }
  } else {
    out.host = authority.substr(0, colon);
    out.port = authority.substr(colon + 1);
  }
  if (out.host.empty()) {
    throw std::invalid_argument("proxy host is empty");
  }
  return out;
}

inline EffectiveProxy proxy_for_request(const Request& request) {
  if (request.disable_proxy || !request.proxy.has_value() ||
      request.proxy->url.empty()) {
    return {};
  }
  auto parsed = parse_proxy_url(request.proxy->url);
  Scheme scheme = Scheme::Http;
  if (parsed.scheme == "https") {
    scheme = Scheme::Https;
  } else if (parsed.scheme == "socks5" || parsed.scheme == "socks5h") {
    scheme = Scheme::Socks5;
  } else if (parsed.scheme != "http") {
    throw std::invalid_argument("unsupported proxy scheme");
  }
  std::string authorization;
  if (scheme != Scheme::Socks5 &&
      (!parsed.username.empty() || !parsed.password.empty())) {
    authorization = basic_auth_value(parsed.username, parsed.password);
  }
  return EffectiveProxy{true, std::move(parsed), std::move(authorization), scheme};
}

template <class Stream>
asio::awaitable<void> read_exact(Stream& stream, void* data, std::size_t size) {
  co_await asio::async_read(stream, asio::buffer(data, size), asio::use_awaitable);
}

inline long parse_http_status_code(std::string_view headers) {
  auto first_space = headers.find(' ');
  if (first_space == std::string_view::npos || first_space + 4 > headers.size()) {
    return 0;
  }
  long status = 0;
  for (std::size_t i = first_space + 1; i < first_space + 4; ++i) {
    if (headers[i] < '0' || headers[i] > '9') {
      return 0;
    }
    status = status * 10 + (headers[i] - '0');
  }
  return status;
}

template <class Stream>
asio::awaitable<void> establish_http_connect_tunnel(
    Stream& stream, std::string_view target_host, std::string_view target_port,
    std::string_view authorization) {
  std::string request;
  request.reserve(128 + target_host.size() + target_port.size());
  request.append("CONNECT ");
  request.append(target_host);
  request.push_back(':');
  request.append(target_port);
  request.append(" HTTP/1.1\r\nHost: ");
  request.append(target_host);
  request.push_back(':');
  request.append(target_port);
  request.append("\r\nProxy-Connection: keep-alive\r\n");
  if (!authorization.empty()) {
    request.append("Proxy-Authorization: ");
    request.append(authorization);
    request.append("\r\n");
  }
  request.append("\r\n");
  co_await asio::async_write(stream, asio::buffer(request), asio::use_awaitable);

  std::string response;
  std::array<char, 4096> tmp{};
  for (;;) {
    auto n = co_await stream.async_read_some(asio::buffer(tmp), asio::use_awaitable);
    response.append(tmp.data(), n);
    auto end = response.find("\r\n\r\n");
    if (end != std::string::npos) {
      auto status = parse_http_status_code(std::string_view(response.data(), end + 4));
      if (status < 200 || status >= 300) {
        throw std::runtime_error("proxy CONNECT failed");
      }
      co_return;
    }
    if (response.size() > 65536) {
      throw std::runtime_error("proxy CONNECT response too large");
    }
  }
}

template <class Stream>
asio::awaitable<void> establish_socks5_tunnel(
    Stream& stream, std::string_view target_host, std::string_view target_port,
    const Url& proxy_url) {
  std::array<unsigned char, 4> greeting{};
  if (!proxy_url.username.empty() || !proxy_url.password.empty()) {
    greeting = {0x05, 0x01, 0x02, 0x00};
  } else {
    greeting = {0x05, 0x01, 0x00, 0x00};
  }
  co_await asio::async_write(stream, asio::buffer(greeting.data(), 3),
                             asio::use_awaitable);
  std::array<unsigned char, 2> method{};
  co_await read_exact(stream, method.data(), method.size());
  if (method[0] != 0x05 || method[1] == 0xff) {
    throw std::runtime_error("socks5 proxy did not accept authentication method");
  }
  if (method[1] == 0x02) {
    if (proxy_url.username.size() > 255 || proxy_url.password.size() > 255) {
      throw std::runtime_error("socks5 username/password too long");
    }
    std::vector<unsigned char> auth;
    auth.reserve(3 + proxy_url.username.size() + proxy_url.password.size());
    auth.push_back(0x01);
    auth.push_back(static_cast<unsigned char>(proxy_url.username.size()));
    auth.insert(auth.end(), proxy_url.username.begin(), proxy_url.username.end());
    auth.push_back(static_cast<unsigned char>(proxy_url.password.size()));
    auth.insert(auth.end(), proxy_url.password.begin(), proxy_url.password.end());
    co_await asio::async_write(stream, asio::buffer(auth), asio::use_awaitable);
    std::array<unsigned char, 2> auth_result{};
    co_await read_exact(stream, auth_result.data(), auth_result.size());
    if (auth_result[0] != 0x01 || auth_result[1] != 0x00) {
      throw std::runtime_error("socks5 authentication failed");
    }
  } else if (method[1] != 0x00) {
    throw std::runtime_error("unsupported socks5 authentication method");
  }

  if (target_host.size() > 255) {
    throw std::runtime_error("socks5 target host too long");
  }
  auto port = static_cast<unsigned>(std::stoul(std::string(target_port)));
  if (port > 65535) {
    throw std::runtime_error("socks5 target port out of range");
  }
  std::vector<unsigned char> connect;
  connect.reserve(7 + target_host.size());
  connect.push_back(0x05);
  connect.push_back(0x01);
  connect.push_back(0x00);
  connect.push_back(0x03);
  connect.push_back(static_cast<unsigned char>(target_host.size()));
  connect.insert(connect.end(), target_host.begin(), target_host.end());
  connect.push_back(static_cast<unsigned char>((port >> 8) & 0xff));
  connect.push_back(static_cast<unsigned char>(port & 0xff));
  co_await asio::async_write(stream, asio::buffer(connect), asio::use_awaitable);

  std::array<unsigned char, 4> reply{};
  co_await read_exact(stream, reply.data(), reply.size());
  if (reply[0] != 0x05 || reply[1] != 0x00) {
    throw std::runtime_error("socks5 CONNECT failed");
  }
  std::size_t addr_len = 0;
  if (reply[3] == 0x01) {
    addr_len = 4;
  } else if (reply[3] == 0x03) {
    unsigned char len = 0;
    co_await read_exact(stream, &len, 1);
    addr_len = len;
  } else if (reply[3] == 0x04) {
    addr_len = 16;
  } else {
    throw std::runtime_error("socks5 CONNECT returned invalid address type");
  }
  std::array<unsigned char, 260> discard{};
  co_await read_exact(stream, discard.data(), addr_len + 2);
}

}  // namespace httpclient::proxy_support
