#include "httpclient/curl_http_client.hpp"

#include <chrono>
#include <cstring>
#include <iostream>
#include <string_view>
#include <string>
#include <vector>

using httpclient::CurlHttpClient;
using httpclient::Request;

int main(int argc, char** argv) {
  std::string url = "https://example.com";
  int rounds = 4;
  bool insecure = false;
  bool fresh_connect = false;
  bool forbid_reuse = false;
  if (argc > 1) {
    url = argv[1];
  }
  if (argc > 2) {
    rounds = std::stoi(argv[2]);
  }
  for (int i = 3; i < argc; ++i) {
    std::string_view arg(argv[i]);
    if (arg == "--insecure") {
      insecure = true;
    } else if (arg == "--fresh-connect") {
      fresh_connect = true;
    } else if (arg == "--forbid-reuse") {
      forbid_reuse = true;
    }
  }

  CurlHttpClient client;
  std::vector<httpclient::Response> responses;
  responses.reserve(rounds);

  for (int i = 0; i < rounds; ++i) {
    Request req;
    req.url = url;
    req.method = "GET";
    req.timeout_ms = 8000;
    req.verify_peer = !insecure;
    req.verify_host = !insecure;
    req.disable_proxy = true;
    req.fresh_connect = fresh_connect;
    req.forbid_reuse = forbid_reuse;
    req.headers.emplace_back("Accept: */*");
    responses.push_back(client.request(std::move(req)));
  }

  for (int i = 0; i < rounds; ++i) {
    const auto& r = responses[i];
    std::cout << "round=" << i
              << " status=" << r.status
              << " error=" << r.error
              << " total_ms=" << r.total_time_sec * 1000.0
              << " dns_ms=" << r.namelookup_time_sec * 1000.0
              << " connect_ms=" << r.connect_time_sec * 1000.0
              << " appconnect_ms=" << r.appconnect_time_sec * 1000.0
              << " pre_ms=" << r.pretransfer_time_sec * 1000.0
              << " ttfb_ms=" << r.starttransfer_time_sec * 1000.0
              << " num_connects=" << r.num_connects
              << " http_version=" << r.http_version
              << " ip=" << r.primary_ip
              << "\n";
  }

  return 0;
}
