#include "asyncx/asyncx.hpp"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <chrono>
#include <cstdlib>
#include <stdexcept>
#include <iostream>
#include <optional>
#include <thread>
#include <tuple>
#include <vector>

namespace asio = boost::asio;
using namespace std::chrono_literals;

namespace {

asio::awaitable<int> value_after(int value, std::chrono::milliseconds delay) {
  co_await asyncx::sleep(delay);
  co_return value;
}

asio::awaitable<int> fail_after(std::chrono::milliseconds delay,
                                const char* message = "boom") {
  co_await asyncx::sleep(delay);
  throw std::runtime_error(message);
}

struct AddJob {
  int a;
  int b;

  int run() { return a + b; }
};

struct MultiplyJob {
  int a;
  int b;

  int operator()() { return a * b; }
};

struct VoidJob {
  bool* flag;

  void run() { *flag = true; }
};

struct ThrowJob {
  int run() { throw std::runtime_error("job failed"); }
};

asio::awaitable<void> run_tests() {
  auto [a, b, c] = co_await asyncx::gather(value_after(1, 1ms),
                                           value_after(2, 1ms),
                                           value_after(3, 1ms));
  if (a != 1 || b != 2 || c != 3) {
    std::cerr << "gather failed\n";
    std::exit(1);
  }

  auto direct_task = co_await asyncx::create_task(value_after(21, 1ms));
  direct_task.set_name("direct");
  auto direct = co_await direct_task.await();
  if (direct != 21 || direct_task.name() != "direct" ||
      direct_task.status() != asyncx::TaskStatus::Finished ||
      direct_task.id() == 0) {
    std::cerr << "direct task await/introspection failed\n";
    std::exit(1);
  }
  asyncx::assert_same_executor(direct_task, direct_task);

  bool exception_handler_called = false;
  asyncx::set_exception_handler([&](std::exception_ptr) {
    exception_handler_called = true;
  });
  asyncx::set_debug(true);
  auto debug_failed =
      co_await asyncx::create_task(fail_after(1ms, "debug boom"));
  try {
    (void)co_await debug_failed.await();
  } catch (const std::runtime_error&) {
  }
  asyncx::set_debug(false);
  asyncx::set_exception_handler({});
  if (!exception_handler_called || !debug_failed.exception()) {
    std::cerr << "debug exception handler failed\n";
    std::exit(1);
  }

  auto limited = co_await asyncx::gather_limited(
      8, 3, [](std::size_t i) { return value_after(static_cast<int>(i), 1ms); });
  if (limited.size() != 8 || limited[0] != 0 || limited[7] != 7) {
    std::cerr << "gather_limited failed\n";
    std::exit(1);
  }
  for (int round = 0; round < 32; ++round) {
    auto repeated = co_await asyncx::gather_limited(
        16, 4,
        [](std::size_t i) { return value_after(static_cast<int>(i), 1ms); });
    if (repeated.size() != 16 || repeated.front() != 0 ||
        repeated.back() != 15) {
      std::cerr << "repeated gather_limited failed\n";
      std::exit(1);
    }
  }
  bool limited_failed = false;
  try {
    (void)co_await asyncx::gather_limited(
        8, 3, [](std::size_t i) -> asio::awaitable<int> {
          if (i == 2) {
            co_await fail_after(1ms, "limited boom");
          }
          co_return co_await value_after(static_cast<int>(i), 1ms);
        });
  } catch (const std::runtime_error&) {
    limited_failed = true;
  }
  if (!limited_failed) {
    std::cerr << "gather_limited exception propagation failed\n";
    std::exit(1);
  }

  std::atomic<int> for_each_sum{0};
  co_await asyncx::for_each_limited(
      16, 4,
      [](std::size_t i) { return value_after(static_cast<int>(i), 1ms); },
      [&](std::size_t, int value) { for_each_sum.fetch_add(value); });
  if (for_each_sum.load() != 120) {
    std::cerr << "for_each_limited failed\n";
    std::exit(1);
  }
  bool for_each_failed = false;
  try {
    co_await asyncx::for_each_limited(
        8, 3, [](std::size_t i) -> asio::awaitable<int> {
          if (i == 2) {
            co_await fail_after(1ms, "for_each boom");
          }
          co_return co_await value_after(static_cast<int>(i), 1ms);
        },
        [](std::size_t, int) {});
  } catch (const std::runtime_error&) {
    for_each_failed = true;
  }
  if (!for_each_failed) {
    std::cerr << "for_each_limited exception propagation failed\n";
    std::exit(1);
  }

  auto [good, bad] = co_await asyncx::gather(
      asyncx::return_exceptions, value_after(1, 1ms),
      fail_after(1ms, "return exceptions boom"));
  if (!good.has_value() || good.value() != 1 || !bad.exception()) {
    std::cerr << "gather return_exceptions failed\n";
    std::exit(1);
  }
  try {
    std::rethrow_exception(bad.take_exception());
  } catch (const std::runtime_error&) {
  }

  asyncx::Queue<int> queue(1);
  co_await queue.put(31);
  if (queue.size() != 1 || queue.try_put(32)) {
    std::cerr << "queue bounded put failed\n";
    std::exit(1);
  }
  if (co_await queue.get() != 31 || !queue.empty()) {
    std::cerr << "queue get failed\n";
    std::exit(1);
  }

  asyncx::Semaphore sem(1);
  if (!sem.try_acquire() || sem.try_acquire()) {
    std::cerr << "semaphore try_acquire failed\n";
    std::exit(1);
  }
  auto sem_task = co_await asyncx::create_task([&]() -> asio::awaitable<int> {
    co_await sem.acquire();
    co_return 41;
  }());
  co_await asyncx::sleep(1ms);
  if (sem_task.done()) {
    std::cerr << "semaphore acquire did not block\n";
    std::exit(1);
  }
  sem.release();
  if (co_await sem_task.await() != 41) {
    std::cerr << "semaphore release failed\n";
    std::exit(1);
  }

  asyncx::Lock lock;
  {
    auto guard = co_await lock.acquire();
    if (lock.try_acquire()) {
      std::cerr << "lock try_acquire while held failed\n";
      std::exit(1);
    }
  }
  if (!lock.try_acquire()) {
    std::cerr << "lock release failed\n";
    std::exit(1);
  }
  lock.release();

  asyncx::Event event;
  auto event_task = co_await asyncx::create_task([&]() -> asio::awaitable<int> {
    co_await event.wait();
    co_return 51;
  }());
  co_await asyncx::sleep(1ms);
  if (event_task.done()) {
    std::cerr << "event wait did not block\n";
    std::exit(1);
  }
  event.set();
  if (co_await event_task.await() != 51 || !event.is_set()) {
    std::cerr << "event set failed\n";
    std::exit(1);
  }

  std::vector<asyncx::Task<int>> waited_tasks;
  waited_tasks.push_back(co_await asyncx::create_task(value_after(61, 1ms)));
  waited_tasks.push_back(co_await asyncx::create_task(value_after(62, 20ms)));
  auto first_wait =
      co_await asyncx::wait(waited_tasks, asyncx::ReturnWhen::FirstCompleted);
  if (first_wait.done.empty() || first_wait.pending.empty()) {
    std::cerr << "wait first_completed failed\n";
    std::exit(1);
  }
  auto order = co_await asyncx::as_completed(waited_tasks);
  if (order.size() != 2 || order[0] != 0) {
    std::cerr << "as_completed failed\n";
    std::exit(1);
  }

  auto shielded = co_await asyncx::create_task(value_after(71, 10ms));
  bool shield_timeout = false;
  try {
    (void)co_await asyncx::wait_for(asyncx::shield(shielded), 1ms);
  } catch (const asyncx::TimeoutError&) {
    shield_timeout = true;
  }
  if (!shield_timeout || co_await shielded.await() != 71) {
    std::cerr << "shield failed\n";
    std::exit(1);
  }

  auto group_ex = co_await asio::this_coro::executor;
  asyncx::TaskGroup group(group_ex);
  auto grouped = group.create_task(value_after(81, 1ms), "grouped");
  co_await group.join();
  if (co_await grouped.await() != 81 || grouped.name() != "grouped") {
    std::cerr << "task group failed\n";
    std::exit(1);
  }

  bool gather_failed = false;
  try {
    (void)co_await asyncx::gather(value_after(1, 5ms),
                                  fail_after(1ms, "gather boom"),
                                  value_after(2, 5ms));
  } catch (const std::runtime_error&) {
    gather_failed = true;
  }
  if (!gather_failed) {
    std::cerr << "gather exception propagation failed\n";
    std::exit(1);
  }

  auto [first, sleeper, later] =
      co_await asyncx::one_of(value_after(7, 1ms), asyncx::sleep(100),
                              value_after(8, 100ms));
  if (!first.has_value() || *first != 7 || sleeper.has_value() ||
      later.has_value()) {
    std::cerr << "one_of value branch failed\n";
    std::exit(1);
  }

  auto [winner, loser] =
      co_await asyncx::race(value_after(9, 1ms), asyncx::sleep(100));
  if (!winner.has_value() || *winner != 9 || loser.has_value()) {
    std::cerr << "race value branch failed\n";
    std::exit(1);
  }

  bool timed_out = false;
  try {
    (void)co_await asyncx::wait_for(value_after(3, 50ms), 1);
  } catch (const asyncx::timeout_error&) {
    timed_out = true;
  }
  if (!timed_out) {
    std::cerr << "wait_for did not time out\n";
    std::exit(1);
  }

  auto value = co_await asyncx::wait_for(value_after(4, 1ms), 100ms);
  if (value != 4) {
    std::cerr << "wait_for value failed\n";
    std::exit(1);
  }

  auto task = co_await asyncx::create_task(value_after(11, 20ms));
  auto [task_result, fast_result] =
      co_await asyncx::one_of(task, value_after(12, 1ms));
  if (task_result.has_value() || !fast_result.has_value() ||
      *fast_result != 12) {
    std::cerr << "mixed task/awaitable one_of failed\n";
    std::exit(1);
  }
  auto late = co_await task.await();
  if (late != 11) {
    std::cerr << "late task await failed\n";
    std::exit(1);
  }

  auto slow_task = co_await asyncx::create_task(value_after(13, 100ms));
  auto [slow_result, quick_result] =
      co_await asyncx::race(slow_task, value_after(14, 1ms));
  if (slow_result.has_value() || !quick_result.has_value() ||
      *quick_result != 14) {
    std::cerr << "mixed task/awaitable race failed\n";
    std::exit(1);
  }
  bool cancelled = false;
  try {
    (void)co_await slow_task.await();
  } catch (const asyncx::CancelledError&) {
    cancelled = true;
  }
  if (!cancelled) {
    std::cerr << "race did not cancel slow task\n";
    std::exit(1);
  }

  asyncx::ThreadPool pool(2);
  auto io_thread = std::this_thread::get_id();
  auto pool_thread = std::thread::id{};
  auto pooled = co_await asyncx::run_in_pool(pool, [&] {
    pool_thread = std::this_thread::get_id();
    int sum = 0;
    for (int i = 0; i < 1000; ++i) {
      sum += i;
    }
    return sum;
  });
  if (pooled != 499500 || pool_thread == io_thread) {
    std::cerr << "run_in_pool value failed\n";
    std::exit(1);
  }

  bool void_ran = false;
  co_await asyncx::run_in_pool(pool, [&] { void_ran = true; });
  if (!void_ran) {
    std::cerr << "run_in_pool void failed\n";
    std::exit(1);
  }

  bool pool_failed = false;
  try {
    (void)co_await asyncx::run_in_pool(pool, []() -> int {
      throw std::runtime_error("pool boom");
    });
  } catch (const std::runtime_error&) {
    pool_failed = true;
  }
  if (!pool_failed) {
    std::cerr << "run_in_pool exception failed\n";
    std::exit(1);
  }

  auto sum = co_await asyncx::run_job_in_pool<AddJob>(pool, 20, 22);
  if (sum != 42) {
    std::cerr << "run_job_in_pool run() failed\n";
    std::exit(1);
  }

  auto product =
      co_await asyncx::run_job_in_pool<MultiplyJob>(pool, 6, 7);
  if (product != 42) {
    std::cerr << "run_job_in_pool operator() failed\n";
    std::exit(1);
  }

  bool static_void_ran = false;
  co_await asyncx::run_job_in_pool<VoidJob>(pool, &static_void_ran);
  if (!static_void_ran) {
    std::cerr << "run_job_in_pool void failed\n";
    std::exit(1);
  }

  bool static_job_failed = false;
  try {
    (void)co_await asyncx::run_job_in_pool<ThrowJob>(pool);
  } catch (const std::runtime_error&) {
    static_job_failed = true;
  }
  if (!static_job_failed) {
    std::cerr << "run_job_in_pool exception failed\n";
    std::exit(1);
  }

  co_return;
}

}  // namespace

int main() {
  asio::io_context io;
  asio::co_spawn(io, run_tests(), asio::detached);
  io.run();
  return 0;
}
