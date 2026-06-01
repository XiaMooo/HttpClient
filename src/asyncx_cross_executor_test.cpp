#include "asyncx/asyncx.hpp"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <thread>

namespace asio = boost::asio;
using namespace std::chrono_literals;

namespace {

asio::awaitable<int> value_after(int value, std::chrono::milliseconds delay,
                                 std::atomic<int>* completed = nullptr) {
  co_await asyncx::sleep(delay);
  if (completed) {
    completed->fetch_add(1, std::memory_order_release);
  }
  co_return value;
}

asio::awaitable<void> run_cross_executor_tests(asio::io_context& other_io) {
  auto other_ex = asio::any_io_executor(other_io.get_executor());

  auto remote = asyncx::create_task(other_ex, value_after(10, 2ms));
  auto [a, b] = co_await asyncx::gather(remote, value_after(20, 2ms));
  if (a != 10 || b != 20) {
    std::cerr << "cross gather failed a=" << a << " b=" << b << "\n";
    std::exit(1);
  }

  auto remote_slow = asyncx::create_task(other_ex, value_after(30, 30ms));
  auto [slow, fast] = co_await asyncx::one_of(remote_slow, value_after(40, 1ms));
  if (slow.has_value() || !fast.has_value() || *fast != 40) {
    std::cerr << "cross one_of failed\n";
    std::exit(1);
  }
  auto late = co_await remote_slow.await();
  if (late != 30) {
    std::cerr << "cross one_of late await failed\n";
    std::exit(1);
  }

  std::atomic<int> remote_completed{0};
  auto remote_race =
      asyncx::create_task(other_ex, value_after(50, 100ms, &remote_completed));
  auto [remote_win, local_win] =
      co_await asyncx::race(remote_race, value_after(60, 1ms));
  if (remote_win.has_value() || !local_win.has_value() || *local_win != 60) {
    std::cerr << "cross race result failed\n";
    std::exit(1);
  }
  bool cancelled = false;
  try {
    (void)co_await remote_race.await();
  } catch (const asyncx::CancelledError&) {
    cancelled = true;
  }
  if (!cancelled) {
    std::cerr << "cross race did not cancel remote task\n";
    std::exit(1);
  }
  if (remote_completed.load(std::memory_order_acquire) != 0) {
    std::cerr << "cross race remote task completed despite cancel\n";
    std::exit(1);
  }

  auto remote_timeout = asyncx::create_task(other_ex, value_after(70, 100ms));
  bool timed_out = false;
  try {
    (void)co_await asyncx::wait_for(remote_timeout, 1ms);
  } catch (const asyncx::TimeoutError&) {
    timed_out = true;
  }
  if (!timed_out) {
    std::cerr << "cross wait_for did not time out\n";
    std::exit(1);
  }
  bool timeout_cancelled = false;
  try {
    (void)co_await remote_timeout.await();
  } catch (const asyncx::CancelledError&) {
    timeout_cancelled = true;
  }
  if (!timeout_cancelled) {
    std::cerr << "cross wait_for did not cancel remote task\n";
    std::exit(1);
  }

  std::cout << "cross_executor_ok=1\n";
  co_return;
}

}  // namespace

int main() {
  asio::io_context main_io;
  asio::io_context other_io;

  auto other_work = asio::make_work_guard(other_io);
  std::jthread other_thread([&] { other_io.run(); });

  asio::co_spawn(main_io, run_cross_executor_tests(other_io), asio::detached);
  main_io.run();

  other_work.reset();
  other_io.stop();
  return 0;
}
