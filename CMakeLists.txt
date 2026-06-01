cmake_minimum_required(VERSION 3.20)

project(HttpClientRuntime LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

include(CTest)

option(HTTPCLIENT_ENABLE_CURL_BASELINE
       "Build libcurl baseline client and benchmark" OFF)

find_package(PkgConfig REQUIRED)
pkg_check_modules(OPENSSL REQUIRED openssl)
pkg_check_modules(NGHTTP2 REQUIRED libnghttp2)
pkg_check_modules(ZLIB REQUIRED zlib)
if(HTTPCLIENT_ENABLE_CURL_BASELINE)
  pkg_check_modules(CURL REQUIRED libcurl)
endif()

function(httpclient_enable_warnings target)
  target_compile_options(${target} PRIVATE -Wall -Wextra -Wpedantic)
  if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    target_compile_options(${target} PRIVATE -Wno-mismatched-new-delete)
  endif()
endfunction()

function(httpclient_add_bench target source library)
  add_executable(${target} ${source})
  target_link_libraries(${target} PRIVATE ${library})
  target_include_directories(${target} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
  httpclient_enable_warnings(${target})
endfunction()

function(httpclient_add_test_executable target source)
  add_executable(${target} ${source})
  if(ARGN)
    target_link_libraries(${target} PRIVATE ${ARGN})
  endif()
  target_include_directories(${target} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
  target_compile_options(${target} PRIVATE -UNDEBUG)
  httpclient_enable_warnings(${target})
endfunction()

add_library(httpclient_core
    src/request.cpp
)
target_include_directories(httpclient_core
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)
httpclient_enable_warnings(httpclient_core)

add_library(asio_httpclient
    src/asio_http_client.cpp
)
target_include_directories(asio_httpclient
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${OPENSSL_INCLUDE_DIRS}
)
target_compile_options(asio_httpclient PRIVATE ${OPENSSL_CFLAGS_OTHER})
target_link_directories(asio_httpclient PRIVATE ${OPENSSL_LIBRARY_DIRS})
target_link_libraries(asio_httpclient PUBLIC httpclient_core ${OPENSSL_LIBRARIES})
httpclient_enable_warnings(asio_httpclient)

add_library(h2_httpclient
    src/h2_client.cpp
)
target_include_directories(h2_httpclient
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${OPENSSL_INCLUDE_DIRS}
        ${NGHTTP2_INCLUDE_DIRS}
)
target_compile_options(h2_httpclient PRIVATE
    ${OPENSSL_CFLAGS_OTHER}
    ${NGHTTP2_CFLAGS_OTHER}
)
target_link_directories(h2_httpclient PRIVATE
    ${OPENSSL_LIBRARY_DIRS}
    ${NGHTTP2_LIBRARY_DIRS}
)
target_link_libraries(h2_httpclient PUBLIC
    httpclient_core
    ${OPENSSL_LIBRARIES}
    ${NGHTTP2_LIBRARIES}
)
httpclient_enable_warnings(h2_httpclient)

add_library(httpclient
    src/http_client.cpp
)
target_include_directories(httpclient
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${ZLIB_INCLUDE_DIRS}
)
target_compile_options(httpclient PRIVATE ${ZLIB_CFLAGS_OTHER})
target_link_directories(httpclient PRIVATE ${ZLIB_LIBRARY_DIRS})
target_link_libraries(httpclient PUBLIC asio_httpclient h2_httpclient ${ZLIB_LIBRARIES})
httpclient_enable_warnings(httpclient)

httpclient_add_bench(asio_httpclient_bench src/asio_bench.cpp asio_httpclient)
httpclient_add_bench(h2_httpclient_bench src/h2_bench.cpp h2_httpclient)
httpclient_add_bench(httpclient_bench src/httpclient_bench.cpp httpclient)

if(HTTPCLIENT_ENABLE_CURL_BASELINE)
  add_library(curl_httpclient
      src/curl_http_client.cpp
  )
  target_include_directories(curl_httpclient
      PUBLIC
          ${CMAKE_CURRENT_SOURCE_DIR}/include
          ${CURL_INCLUDE_DIRS}
  )
  target_compile_options(curl_httpclient PRIVATE ${CURL_CFLAGS_OTHER})
  target_link_directories(curl_httpclient PRIVATE ${CURL_LIBRARY_DIRS})
  target_link_libraries(curl_httpclient PUBLIC httpclient_core ${CURL_LIBRARIES})
  httpclient_enable_warnings(curl_httpclient)

  httpclient_add_bench(curl_httpclient_bench src/curl_bench.cpp curl_httpclient)
  httpclient_add_bench(curl_httpclient_tls_test src/curl_tls_reuse_test.cpp
                       curl_httpclient)
endif()

httpclient_add_test_executable(request_builder_test src/request_builder_test.cpp httpclient_core)
httpclient_add_test_executable(request_transport_test src/request_transport_test.cpp httpclient)
httpclient_add_test_executable(httpclient_api_test src/httpclient_api_test.cpp httpclient)
httpclient_add_test_executable(proxy_transport_test src/proxy_transport_test.cpp httpclient)
httpclient_add_test_executable(asyncx_test src/asyncx_test.cpp)
httpclient_add_test_executable(asyncx_cross_executor_test src/asyncx_cross_executor_test.cpp)

if(BUILD_TESTING)
  add_test(NAME request_builder_test COMMAND request_builder_test)
  add_test(NAME httpclient_api_test COMMAND httpclient_api_test)
  add_test(NAME asyncx_test COMMAND asyncx_test)
  add_test(NAME asyncx_cross_executor_test COMMAND asyncx_cross_executor_test)
  add_test(
      NAME proxy_transport_smoke
      COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/scripts/smoke_proxy_transport.sh
              $<TARGET_FILE_DIR:proxy_transport_test>
  )
  set_tests_properties(proxy_transport_smoke PROPERTIES TIMEOUT 30)
  add_test(
      NAME request_transport_smoke
      COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/scripts/smoke_request_transport.sh
              $<TARGET_FILE_DIR:request_transport_test>
  )
  set_tests_properties(request_transport_smoke PROPERTIES TIMEOUT 30)
  add_test(
      NAME httpclient_local_smoke
      COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/scripts/smoke_httpclient_local.sh
              $<TARGET_FILE_DIR:httpclient_bench>
  )
  set_tests_properties(httpclient_local_smoke PROPERTIES TIMEOUT 30)
endif()
