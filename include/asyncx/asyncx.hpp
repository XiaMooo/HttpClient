#pragma once

#include <boost/asio/awaitable.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/system/system_error.hpp>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace asyncx {
namespace asio = boost::asio;

class TimeoutError : public std::runtime_error {
public:
  TimeoutError() : std::runtime_error("asyncx wait_for timeout") {}
};

class CancelledError : public std::runtime_error {
public:
  CancelledError() : std::runtime_error("asyncx task cancelled") {}
};

using timeout_error = TimeoutError;
using cancelled_error = CancelledError;

enum class TaskStatus {
  Pending,
  Finished,
  CancelRequested,
};

enum class ReturnWhen {
  AllCompleted,
  FirstCompleted,
  FirstException,
};

struct ReturnExceptionsTag {};
inline constexpr ReturnExceptionsTag return_exceptions{};

inline std::atomic<bool>& debug_flag() {
  static std::atomic<bool> enabled{false};
  return enabled;
}

inline void set_debug(bool enabled) { debug_flag().store(enabled); }

inline bool debug_enabled() { return debug_flag().load(); }

using ExceptionHandler = std::function<void(std::exception_ptr)>;

inline ExceptionHandler& exception_handler_ref() {
  static ExceptionHandler handler;
  return handler;
}

inline void set_exception_handler(ExceptionHandler handler) {
  exception_handler_ref() = std::move(handler);
}

class ThreadPool {
public:
  explicit ThreadPool(std::size_t threads)
      : pool_(threads == 0 ? 1 : threads) {}

  ThreadPool()
      : ThreadPool(std::max<std::size_t>(
            1, std::thread::hardware_concurrency())) {}

  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;

  ~ThreadPool() { join(); }

  asio::thread_pool& native() { return pool_; }

  void join() {
    if (!joined_.exchange(true)) {
      pool_.join();
    }
  }

private:
  asio::thread_pool pool_;
  std::atomic<bool> joined_{false};
};

template <typename T>
struct result_value {
  using type = T;
};

template <>
struct result_value<void> {
  using type = std::monostate;
};

template <typename T>
using result_value_t = typename result_value<T>::type;

template <typename T>
class Outcome {
public:
  using value_type = result_value_t<T>;

  Outcome() = default;
  explicit Outcome(value_type value) : value_(std::move(value)) {}
  explicit Outcome(std::exception_ptr error) : error_(std::move(error)) {}

  bool has_value() const { return value_.has_value() && !error_; }
  explicit operator bool() const { return has_value(); }
  std::exception_ptr exception() const { return error_; }
  std::exception_ptr take_exception() { return std::exchange(error_, nullptr); }

  value_type& value() & {
    rethrow_if_exception();
    return *value_;
  }

  value_type&& value() && {
    rethrow_if_exception();
    return std::move(*value_);
  }

  void rethrow_if_exception() const {
    if (error_) {
      std::rethrow_exception(error_);
    }
  }

private:
  std::optional<value_type> value_;
  std::exception_ptr error_;
};

template <typename T>
class Task;

namespace detail {

inline std::exception_ptr normalize_exception(std::exception_ptr error) {
  if (!error) {
    return nullptr;
  }
  try {
    std::rethrow_exception(error);
  } catch (const CancelledError&) {
    return error;
  } catch (const boost::system::system_error& e) {
    if (e.code() == asio::error::operation_aborted) {
      return std::make_exception_ptr(CancelledError{});
    }
    return error;
  } catch (...) {
    return error;
  }
}

inline bool is_cancel_exception(std::exception_ptr error) {
  if (!error) {
    return false;
  }
  try {
    std::rethrow_exception(error);
  } catch (const CancelledError&) {
    return true;
  } catch (const boost::system::system_error& e) {
    return e.code() == asio::error::operation_aborted;
  } catch (...) {
    return false;
  }
}

inline std::uint64_t next_task_id() {
  static std::atomic<std::uint64_t> id{1};
  return id.fetch_add(1);
}

inline void maybe_report_exception(std::exception_ptr error) {
  if (!error || !debug_enabled()) {
    return;
  }
  auto& handler = exception_handler_ref();
  if (handler) {
    handler(error);
  }
}

template <typename T>
struct TaskState {
  using Value = result_value_t<T>;

  explicit TaskState(asio::any_io_executor executor)
      : executor(std::move(executor)), id(next_task_id()) {}

  asio::any_io_executor executor;
  mutable std::mutex mutex;
  std::optional<Value> value;
  std::exception_ptr error;
  bool done = false;
  bool cancel_requested = false;
  std::uint64_t id = 0;
  std::string name;
  asio::cancellation_signal cancel;
  std::vector<std::weak_ptr<asio::steady_timer>> waiters;
  std::vector<std::function<void()>> continuations;
};

template <typename T>
struct AwaitResult;

template <typename T>
struct AwaitResult<Task<T>> {
  using type = T;
};

template <typename T>
struct AwaitResult<Task<T>&> {
  using type = T;
};

template <typename T>
struct AwaitResult<const Task<T>&> {
  using type = T;
};

template <typename T, typename Executor>
struct AwaitResult<asio::awaitable<T, Executor>> {
  using type = T;
};

template <typename Awaitable>
using await_result_t = typename AwaitResult<std::decay_t<Awaitable>>::type;

template <typename T>
struct WaitState {
  using value_type = T;
  std::optional<result_value_t<T>> value;
  std::exception_ptr error;
  bool done = false;
};

template <typename Executor>
struct GroupState {
  explicit GroupState(std::size_t total) : total(total) {}

  std::size_t total = 0;
  std::atomic<std::size_t> done_count{0};
  std::atomic<bool> wake_once{false};
  std::atomic<bool> cancel_once{false};
  std::shared_ptr<asio::steady_timer> wake;
  std::mutex error_mutex;
  std::exception_ptr first_error;
};

template <typename T>
struct VectorGroupState {
  explicit VectorGroupState(std::size_t total) : total(total), values(total) {}

  std::size_t total = 0;
  std::atomic<std::size_t> done_count{0};
  std::atomic<bool> wake_once{false};
  std::shared_ptr<asio::steady_timer> wake;
  std::mutex mutex;
  std::vector<std::optional<result_value_t<T>>> values;
  std::exception_ptr first_error;
};

template <typename T>
struct LimitedGroupState {
  explicit LimitedGroupState(std::size_t total) : total(total), values(total) {}

  std::size_t total = 0;
  std::atomic<std::size_t> next{0};
  std::atomic<std::size_t> completed{0};
  std::atomic<bool> stop{false};
  std::atomic<bool> wake_once{false};
  std::shared_ptr<asio::steady_timer> wake;
  std::mutex mutex;
  std::vector<std::optional<result_value_t<T>>> values;
  std::exception_ptr first_error;
};

struct LimitedForEachState {
  explicit LimitedForEachState(std::size_t total) : total(total) {}

  std::size_t total = 0;
  std::atomic<std::size_t> next{0};
  std::atomic<std::size_t> completed{0};
  std::atomic<bool> stop{false};
  std::atomic<bool> wake_once{false};
  std::shared_ptr<asio::steady_timer> wake;
  std::mutex mutex;
  std::exception_ptr first_error;
};

template <std::size_t I = 0, typename Tuple>
std::exception_ptr first_error(const Tuple& states, bool ignore_cancelled) {
  if constexpr (I < std::tuple_size_v<Tuple>) {
    auto error = std::get<I>(states)->error;
    if (error && (!ignore_cancelled || !is_cancel_exception(error))) {
      return error;
    }
    return first_error<I + 1>(states, ignore_cancelled);
  } else {
    return nullptr;
  }
}

template <std::size_t I = 0, typename Tuple>
std::optional<std::size_t> first_done_index(const Tuple& states) {
  if constexpr (I < std::tuple_size_v<Tuple>) {
    auto& state = std::get<I>(states);
    if (state->done && state->value.has_value()) {
      return I;
    }
    return first_done_index<I + 1>(states);
  } else {
    return std::nullopt;
  }
}

template <std::size_t I = 0, typename Tuple>
auto make_values_tuple(Tuple& states) {
  if constexpr (I == std::tuple_size_v<Tuple>) {
    return std::tuple<>{};
  } else {
    return std::tuple_cat(
        std::make_tuple(std::move(*std::get<I>(states)->value)),
        make_values_tuple<I + 1>(states));
  }
}

template <std::size_t I = 0, typename Tuple>
auto make_outcome_tuple(Tuple& states) {
  if constexpr (I == std::tuple_size_v<Tuple>) {
    return std::tuple<>{};
  } else {
    using T = typename std::remove_reference_t<
        decltype(*std::get<I>(states))>::value_type;
    auto& state = *std::get<I>(states);
    auto outcome = state.error
                       ? Outcome<T>(state.error)
                       : Outcome<T>(std::move(*state.value));
    return std::tuple_cat(std::make_tuple(std::move(outcome)),
                          make_outcome_tuple<I + 1>(states));
  }
}

template <std::size_t I = 0, typename Tuple>
auto make_optional_tuple(Tuple& states) {
  if constexpr (I == std::tuple_size_v<Tuple>) {
    return std::tuple<>{};
  } else {
    return std::tuple_cat(
        std::make_tuple(std::move(std::get<I>(states)->value)),
        make_optional_tuple<I + 1>(states));
  }
}

template <std::size_t I = 0, typename Tuple>
void cancel_all(Tuple& tasks) {
  if constexpr (I < std::tuple_size_v<Tuple>) {
    std::get<I>(tasks).cancel();
    cancel_all<I + 1>(tasks);
  }
}

template <std::size_t I = 0, typename Tuple>
void cancel_all_except(Tuple& tasks, std::size_t keep_index) {
  if constexpr (I < std::tuple_size_v<Tuple>) {
    if (I != keep_index) {
      std::get<I>(tasks).cancel();
    }
    cancel_all_except<I + 1>(tasks, keep_index);
  }
}

template <typename T>
void add_completion(const Task<T>& task, std::function<void()> fn);

template <typename T>
std::exception_ptr task_error(const Task<T>& task);

template <typename T>
result_value_t<T> task_value_for_vector_gather(const Task<T>& task);

}  // namespace detail

template <typename T>
class Task {
public:
  using value_type = T;

  Task() = default;

  bool valid() const { return static_cast<bool>(state_); }

  bool done() const {
    if (!state_) {
      return false;
    }
    std::lock_guard<std::mutex> lock(state_->mutex);
    return state_->done;
  }

  TaskStatus status() const {
    if (!state_) {
      return TaskStatus::Finished;
    }
    std::lock_guard<std::mutex> lock(state_->mutex);
    if (state_->done) {
      return TaskStatus::Finished;
    }
    if (state_->cancel_requested) {
      return TaskStatus::CancelRequested;
    }
    return TaskStatus::Pending;
  }

  std::uint64_t id() const {
    if (!state_) {
      return 0;
    }
    return state_->id;
  }

  std::string name() const {
    if (!state_) {
      return {};
    }
    std::lock_guard<std::mutex> lock(state_->mutex);
    return state_->name;
  }

  void set_name(std::string name) const {
    if (!state_) {
      return;
    }
    std::lock_guard<std::mutex> lock(state_->mutex);
    state_->name = std::move(name);
  }

  std::exception_ptr exception() const {
    if (!state_) {
      return nullptr;
    }
    std::lock_guard<std::mutex> lock(state_->mutex);
    return state_->error;
  }

  asio::any_io_executor executor() const {
    if (!state_) {
      throw std::logic_error("invalid asyncx::Task has no executor");
    }
    return state_->executor;
  }

  void cancel() const {
    if (!state_) {
      return;
    }
    auto state = state_;
    asio::post(state->executor, [state] {
      {
        std::lock_guard<std::mutex> lock(state->mutex);
        state->cancel_requested = true;
      }
      state->cancel.emit(asio::cancellation_type::all);
    });
  }

  asio::awaitable<T> await() const {
    if (!state_) {
      throw std::logic_error("awaiting invalid asyncx::Task");
    }

    auto ex = co_await asio::this_coro::executor;
    auto timer = std::make_shared<asio::steady_timer>(ex);
    bool ready = false;
    {
      std::lock_guard<std::mutex> lock(state_->mutex);
      ready = state_->done;
      if (!ready) {
        timer->expires_at(asio::steady_timer::time_point::max());
        state_->waiters.push_back(timer);
      }
    }

    if (!ready) {
      boost::system::error_code ec;
      co_await timer->async_wait(asio::redirect_error(asio::use_awaitable, ec));
    }

    std::exception_ptr error;
    std::optional<result_value_t<T>> value;
    {
      std::lock_guard<std::mutex> lock(state_->mutex);
      if (!state_->done) {
        throw CancelledError();
      }
      error = state_->error;
      value = state_->value;
    }

    if (error) {
      std::rethrow_exception(error);
    }

    if constexpr (std::is_void_v<T>) {
      co_return;
    } else {
      co_return std::move(*value);
    }
  }

private:
  explicit Task(std::shared_ptr<detail::TaskState<T>> state)
      : state_(std::move(state)) {}

  template <typename U, typename Executor>
  friend Task<U> create_task(const Executor&, asio::awaitable<U, Executor>);

  template <typename U>
  friend struct detail::AwaitResult;

  template <typename U>
  friend void detail::add_completion(const Task<U>& task, std::function<void()> fn);

  template <typename U>
  friend std::exception_ptr detail::task_error(const Task<U>& task);

  template <typename U>
  friend result_value_t<U> detail::task_value_for_vector_gather(
      const Task<U>& task);

  std::shared_ptr<detail::TaskState<T>> state_;
};

namespace detail {

template <typename T>
void complete_task(const std::shared_ptr<TaskState<T>>& state,
                   std::optional<result_value_t<T>> value,
                   std::exception_ptr error) {
  std::vector<std::shared_ptr<asio::steady_timer>> waiters;
  std::vector<std::function<void()>> continuations;
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    if (state->done) {
      return;
    }
    state->value = std::move(value);
    state->error = normalize_exception(error);
    state->done = true;
    for (auto& waiter : state->waiters) {
      if (auto timer = waiter.lock()) {
        waiters.push_back(std::move(timer));
      }
    }
    state->waiters.clear();
    continuations = std::move(state->continuations);
  }
  for (auto& waiter : waiters) {
    waiter->cancel();
  }
  for (auto& continuation : continuations) {
    continuation();
  }
  maybe_report_exception(state->error);
}

template <typename T>
void add_completion(const Task<T>& task, std::function<void()> fn) {
  if (!task.state_) {
    throw std::logic_error("using invalid asyncx::Task");
  }

  bool ready = false;
  {
    std::lock_guard<std::mutex> lock(task.state_->mutex);
    ready = task.state_->done;
    if (!ready) {
      task.state_->continuations.push_back(std::move(fn));
    }
  }
  if (ready) {
    fn();
  }
}

template <typename T>
std::exception_ptr task_error(const Task<T>& task) {
  if (!task.state_) {
    throw std::logic_error("using invalid asyncx::Task");
  }
  std::lock_guard<std::mutex> lock(task.state_->mutex);
  if (!task.state_->done) {
    throw std::logic_error("reading unfinished asyncx::Task");
  }
  return task.state_->error;
}

template <typename T>
result_value_t<T> task_value_for_vector_gather(const Task<T>& task) {
  if (!task.state_) {
    throw std::logic_error("using invalid asyncx::Task");
  }
  std::lock_guard<std::mutex> lock(task.state_->mutex);
  if (!task.state_->done) {
    throw std::logic_error("reading unfinished asyncx::Task");
  }
  if (task.state_->error) {
    std::rethrow_exception(task.state_->error);
  }

  if constexpr (std::is_void_v<T>) {
    return std::monostate{};
  } else {
    if constexpr (std::is_copy_constructible_v<result_value_t<T>>) {
      if (task.state_.use_count() == 1) {
        return std::move(*task.state_->value);
      }
      return *task.state_->value;
    } else {
      return std::move(*task.state_->value);
    }
  }
}

template <typename T>
Task<T> ensure_task(const asio::any_io_executor&, const Task<T>& task) {
  return task;
}

template <typename T>
Task<T> ensure_task(const asio::any_io_executor&, Task<T>&& task) {
  return std::move(task);
}

}  // namespace detail

template <typename T, typename Executor>
Task<T> create_task(const Executor& ex, asio::awaitable<T, Executor> task) {
  auto state = std::make_shared<detail::TaskState<T>>(asio::any_io_executor(ex));
  Task<T> out(state);

  asio::co_spawn(
      ex,
      [state, task = std::move(task)]() mutable -> asio::awaitable<void, Executor> {
        try {
          co_await asio::this_coro::reset_cancellation_state(
              asio::enable_total_cancellation());
          if constexpr (std::is_void_v<T>) {
            co_await std::move(task);
            detail::complete_task<T>(state, std::monostate{}, nullptr);
          } else {
            auto value = co_await std::move(task);
            detail::complete_task<T>(state, std::move(value), nullptr);
          }
        } catch (...) {
          detail::complete_task<T>(state, std::nullopt, std::current_exception());
        }
        co_return;
      },
      asio::bind_cancellation_slot(state->cancel.slot(), asio::detached));

  return out;
}

template <typename T, typename Executor>
asio::awaitable<Task<T>, Executor> create_task(asio::awaitable<T, Executor> task) {
  auto ex = co_await asio::this_coro::executor;
  co_return create_task(ex, std::move(task));
}

template <typename A, typename B>
bool same_executor(const Task<A>& a, const Task<B>& b) {
  return a.executor() == b.executor();
}

template <typename A, typename B>
void assert_same_executor(const Task<A>& a, const Task<B>& b) {
  if (!same_executor(a, b)) {
    throw std::logic_error("asyncx tasks belong to different executors");
  }
}

namespace detail {

template <typename T, typename Executor>
Task<T> ensure_task(const asio::any_io_executor& ex,
                    asio::awaitable<T, Executor> task) {
  return create_task(ex, std::move(task));
}

template <typename T>
asio::awaitable<void> await_and_discard(Task<T> task) {
  if constexpr (std::is_void_v<T>) {
    co_await task.await();
  } else {
    (void)co_await task.await();
  }
  co_return;
}

template <typename T, typename Executor>
asio::awaitable<void, Executor> wait_task(Task<T> task,
                                          std::shared_ptr<WaitState<T>> state,
                                          std::shared_ptr<GroupState<Executor>> group,
                                          std::size_t index,
                                          auto on_done) {
  try {
    if constexpr (std::is_void_v<T>) {
      co_await task.await();
      state->value = std::monostate{};
    } else {
      state->value = co_await task.await();
    }
  } catch (...) {
    state->error = normalize_exception(std::current_exception());
  }

  state->done = true;
  on_done(index);
  if (group->done_count.fetch_add(1) + 1 == group->total) {
    if (!group->wake_once.exchange(true)) {
      group->wake->cancel();
    }
  }
}

template <typename Executor, typename TaskTuple, typename StateTuple, typename Group,
          typename OnDone, std::size_t... I>
void spawn_waiters(const Executor& ex, TaskTuple& tasks, StateTuple& states,
                   std::shared_ptr<Group> group, OnDone on_done,
                   std::index_sequence<I...>) {
  (asio::co_spawn(ex,
                  wait_task(std::get<I>(tasks), std::get<I>(states), group, I,
                            on_done),
                  asio::detached),
   ...);
}

template <typename T>
asio::awaitable<void> run_vector_gather_item(
    asio::awaitable<T> task, std::shared_ptr<VectorGroupState<T>> group,
    std::size_t index) {
  std::optional<result_value_t<T>> value;
  std::exception_ptr error;
  try {
    if constexpr (std::is_void_v<T>) {
      co_await std::move(task);
      value = std::monostate{};
    } else {
      value = co_await std::move(task);
    }
  } catch (...) {
    error = normalize_exception(std::current_exception());
  }

  {
    std::lock_guard<std::mutex> lock(group->mutex);
    if (error && !group->first_error) {
      group->first_error = error;
    }
    group->values[index] = std::move(value);
  }

  if (error) {
    if (!group->wake_once.exchange(true)) {
      group->wake->cancel();
    }
  }
  if (group->done_count.fetch_add(1) + 1 == group->total) {
    if (!group->wake_once.exchange(true)) {
      group->wake->cancel();
    }
  }
}

inline void cancel_one_waiter(
    std::deque<std::weak_ptr<asio::steady_timer>>& waiters) {
  while (!waiters.empty()) {
    auto waiter = waiters.front();
    waiters.pop_front();
    if (auto timer = waiter.lock()) {
      timer->cancel();
      return;
    }
  }
}

inline void cancel_all_waiters(
    std::deque<std::weak_ptr<asio::steady_timer>>& waiters) {
  auto copy = std::move(waiters);
  waiters.clear();
  for (auto& waiter : copy) {
    if (auto timer = waiter.lock()) {
      timer->cancel();
    }
  }
}

}  // namespace detail

class Event {
public:
  explicit Event(bool set = false) : set_(set) {}

  void set() {
    std::lock_guard<std::mutex> lock(mutex_);
    set_ = true;
    detail::cancel_all_waiters(waiters_);
  }

  void clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    set_ = false;
  }

  bool is_set() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return set_;
  }

  asio::awaitable<void> wait() {
    auto ex = co_await asio::this_coro::executor;
    for (;;) {
      auto timer = std::make_shared<asio::steady_timer>(ex);
      {
        std::lock_guard<std::mutex> lock(mutex_);
        if (set_) {
          co_return;
        }
        timer->expires_at(asio::steady_timer::time_point::max());
        waiters_.push_back(timer);
      }
      boost::system::error_code ec;
      co_await timer->async_wait(asio::redirect_error(asio::use_awaitable, ec));
    }
  }

private:
  mutable std::mutex mutex_;
  bool set_ = false;
  std::deque<std::weak_ptr<asio::steady_timer>> waiters_;
};

class Semaphore {
public:
  explicit Semaphore(std::size_t permits) : permits_(permits) {}

  asio::awaitable<void> acquire() {
    auto ex = co_await asio::this_coro::executor;
    for (;;) {
      auto timer = std::make_shared<asio::steady_timer>(ex);
      {
        std::lock_guard<std::mutex> lock(mutex_);
        if (permits_ > 0) {
          --permits_;
          co_return;
        }
        timer->expires_at(asio::steady_timer::time_point::max());
        waiters_.push_back(timer);
      }
      boost::system::error_code ec;
      co_await timer->async_wait(asio::redirect_error(asio::use_awaitable, ec));
    }
  }

  bool try_acquire() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (permits_ == 0) {
      return false;
    }
    --permits_;
    return true;
  }

  void release(std::size_t n = 1) {
    std::lock_guard<std::mutex> lock(mutex_);
    permits_ += n;
    for (std::size_t i = 0; i < n; ++i) {
      detail::cancel_one_waiter(waiters_);
    }
  }

  std::size_t available() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return permits_;
  }

private:
  mutable std::mutex mutex_;
  std::size_t permits_ = 0;
  std::deque<std::weak_ptr<asio::steady_timer>> waiters_;
};

class LockGuard;

class Lock {
public:
  Lock() : semaphore_(1) {}

  asio::awaitable<LockGuard> acquire();
  bool try_acquire() { return semaphore_.try_acquire(); }
  void release() { semaphore_.release(); }

private:
  Semaphore semaphore_;
};

class LockGuard {
public:
  explicit LockGuard(Lock& lock) : lock_(&lock) {}
  LockGuard(const LockGuard&) = delete;
  LockGuard& operator=(const LockGuard&) = delete;

  LockGuard(LockGuard&& other) noexcept : lock_(other.lock_) {
    other.lock_ = nullptr;
  }

  LockGuard& operator=(LockGuard&& other) noexcept {
    if (this != &other) {
      release();
      lock_ = other.lock_;
      other.lock_ = nullptr;
    }
    return *this;
  }

  ~LockGuard() { release(); }

  void release() {
    if (lock_) {
      lock_->release();
      lock_ = nullptr;
    }
  }

private:
  Lock* lock_ = nullptr;
};

inline asio::awaitable<LockGuard> Lock::acquire() {
  co_await semaphore_.acquire();
  co_return LockGuard(*this);
}

template <typename T>
class Queue {
public:
  explicit Queue(std::size_t max_size = 0) : max_size_(max_size) {}

  asio::awaitable<void> put(T value) {
    auto ex = co_await asio::this_coro::executor;
    for (;;) {
      auto timer = std::make_shared<asio::steady_timer>(ex);
      {
        std::lock_guard<std::mutex> lock(mutex_);
        if (max_size_ == 0 || items_.size() < max_size_) {
          items_.push_back(std::move(value));
          detail::cancel_one_waiter(get_waiters_);
          co_return;
        }
        timer->expires_at(asio::steady_timer::time_point::max());
        put_waiters_.push_back(timer);
      }
      boost::system::error_code ec;
      co_await timer->async_wait(asio::redirect_error(asio::use_awaitable, ec));
    }
  }

  bool try_put(T value) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (max_size_ != 0 && items_.size() >= max_size_) {
      return false;
    }
    items_.push_back(std::move(value));
    detail::cancel_one_waiter(get_waiters_);
    return true;
  }

  asio::awaitable<T> get() {
    auto ex = co_await asio::this_coro::executor;
    for (;;) {
      auto timer = std::make_shared<asio::steady_timer>(ex);
      {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!items_.empty()) {
          auto value = std::move(items_.front());
          items_.pop_front();
          detail::cancel_one_waiter(put_waiters_);
          co_return value;
        }
        timer->expires_at(asio::steady_timer::time_point::max());
        get_waiters_.push_back(timer);
      }
      boost::system::error_code ec;
      co_await timer->async_wait(asio::redirect_error(asio::use_awaitable, ec));
    }
  }

  std::optional<T> try_get() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (items_.empty()) {
      return std::nullopt;
    }
    auto value = std::move(items_.front());
    items_.pop_front();
    detail::cancel_one_waiter(put_waiters_);
    return value;
  }

  std::size_t size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return items_.size();
  }

  bool empty() const { return size() == 0; }

private:
  mutable std::mutex mutex_;
  std::size_t max_size_ = 0;
  std::deque<T> items_;
  std::deque<std::weak_ptr<asio::steady_timer>> get_waiters_;
  std::deque<std::weak_ptr<asio::steady_timer>> put_waiters_;
};

template <typename Rep, typename Period,
          typename Executor = asio::any_io_executor>
asio::awaitable<void, Executor> sleep(std::chrono::duration<Rep, Period> duration) {
  auto ex = co_await asio::this_coro::executor;
  asio::steady_timer timer(ex);
  timer.expires_after(duration);
  boost::system::error_code ec;
  co_await timer.async_wait(asio::redirect_error(asio::use_awaitable, ec));
  if (ec == asio::error::operation_aborted) {
    throw CancelledError();
  }
}

template <typename Executor = asio::any_io_executor>
asio::awaitable<void, Executor> sleep(std::uint64_t milliseconds) {
  co_await sleep(std::chrono::milliseconds(milliseconds));
}

template <typename Awaitable, typename Fn>
auto then(Awaitable&& task, Fn&& fn)
    -> asio::awaitable<std::invoke_result_t<
        Fn, result_value_t<detail::await_result_t<Awaitable>>>> {
  using T = detail::await_result_t<Awaitable>;
  auto ex = co_await asio::this_coro::executor;
  auto handle = detail::ensure_task(ex, std::forward<Awaitable>(task));
  if constexpr (std::is_void_v<T>) {
    co_await handle.await();
    co_return std::forward<Fn>(fn)(std::monostate{});
  } else {
    auto value = co_await handle.await();
    co_return std::forward<Fn>(fn)(std::move(value));
  }
}

template <typename... Awaitables>
asio::awaitable<std::tuple<result_value_t<detail::await_result_t<Awaitables>>...>>
gather(Awaitables&&... awaitables) {
  using ResultTuple =
      std::tuple<result_value_t<detail::await_result_t<Awaitables>>...>;
  auto ex = co_await asio::this_coro::executor;
  auto tasks = std::make_tuple(
      detail::ensure_task(ex, std::forward<Awaitables>(awaitables))...);
  auto states = std::make_tuple(
      std::make_shared<detail::WaitState<detail::await_result_t<Awaitables>>>()...);
  auto group = std::make_shared<detail::GroupState<asio::any_io_executor>>(
      sizeof...(Awaitables));
  group->wake = std::make_shared<asio::steady_timer>(ex);
  group->wake->expires_at(asio::steady_timer::time_point::max());

  auto on_done = [group](std::size_t) {
    if (!group->wake_once.exchange(true)) {
      group->wake->cancel();
    }
  };
  detail::spawn_waiters(ex, tasks, states, group, on_done,
                        std::index_sequence_for<Awaitables...>{});

  while (group->done_count.load() < group->total) {
    boost::system::error_code ec;
    co_await group->wake->async_wait(
        asio::redirect_error(asio::use_awaitable, ec));
    auto error = detail::first_error(states, false);
    if (error) {
      if (!group->cancel_once.exchange(true)) {
        detail::cancel_all(tasks);
      }
    }
    if (group->done_count.load() < group->total) {
      group->wake_once.store(false);
      group->wake->expires_at(asio::steady_timer::time_point::max());
    }
  }

  auto error = detail::first_error(states, false);
  if (error) {
    std::rethrow_exception(error);
  }

  co_return static_cast<ResultTuple>(detail::make_values_tuple(states));
}

template <typename... Awaitables>
asio::awaitable<std::tuple<Outcome<detail::await_result_t<Awaitables>>...>>
gather(ReturnExceptionsTag, Awaitables&&... awaitables) {
  using ResultTuple =
      std::tuple<Outcome<detail::await_result_t<Awaitables>>...>;
  auto ex = co_await asio::this_coro::executor;
  auto tasks = std::make_tuple(
      detail::ensure_task(ex, std::forward<Awaitables>(awaitables))...);
  auto states = std::make_tuple(
      std::make_shared<detail::WaitState<detail::await_result_t<Awaitables>>>()...);
  auto group = std::make_shared<detail::GroupState<asio::any_io_executor>>(
      sizeof...(Awaitables));
  group->wake = std::make_shared<asio::steady_timer>(ex);
  group->wake->expires_at(asio::steady_timer::time_point::max());

  auto on_done = [group](std::size_t) {
    if (!group->wake_once.exchange(true)) {
      group->wake->cancel();
    }
  };
  detail::spawn_waiters(ex, tasks, states, group, on_done,
                        std::index_sequence_for<Awaitables...>{});

  while (group->done_count.load() < group->total) {
    boost::system::error_code ec;
    co_await group->wake->async_wait(
        asio::redirect_error(asio::use_awaitable, ec));
    if (group->done_count.load() < group->total) {
      group->wake_once.store(false);
      group->wake->expires_at(asio::steady_timer::time_point::max());
    }
  }

  co_return static_cast<ResultTuple>(detail::make_outcome_tuple(states));
}

template <typename T>
asio::awaitable<std::vector<result_value_t<T>>> gather(
    std::vector<Task<T>> tasks) {
  std::vector<result_value_t<T>> out;
  out.reserve(tasks.size());
  if (tasks.empty()) {
    co_return out;
  }

  auto ex = co_await asio::this_coro::executor;
  auto group = std::make_shared<detail::GroupState<asio::any_io_executor>>(
      tasks.size());
  group->wake = std::make_shared<asio::steady_timer>(ex);
  group->wake->expires_at(asio::steady_timer::time_point::max());

  for (const auto& task : tasks) {
    detail::add_completion<T>(task, [group, task] {
      if (auto error = detail::task_error<T>(task)) {
        bool should_wake = false;
        {
          std::lock_guard<std::mutex> lock(group->error_mutex);
          if (!group->first_error) {
            group->first_error = error;
            should_wake = true;
          }
        }
        if (should_wake && !group->wake_once.exchange(true)) {
          group->wake->cancel();
        }
      }

      if (group->done_count.fetch_add(1) + 1 == group->total) {
        if (!group->wake_once.exchange(true)) {
          group->wake->cancel();
        }
      }
    });
  }

  while (group->done_count.load() < group->total) {
    boost::system::error_code ec;
    co_await group->wake->async_wait(
        asio::redirect_error(asio::use_awaitable, ec));
    {
      std::lock_guard<std::mutex> lock(group->error_mutex);
      if (group->first_error) {
        std::rethrow_exception(group->first_error);
      }
    }
    if (group->done_count.load() < group->total) {
      group->wake_once.store(false);
      group->wake->expires_at(asio::steady_timer::time_point::max());
    }
  }

  for (const auto& task : tasks) {
    out.emplace_back(detail::task_value_for_vector_gather<T>(task));
  }
  co_return out;
}

template <typename T>
asio::awaitable<std::vector<result_value_t<T>>> gather(
    std::vector<asio::awaitable<T>> awaitables) {
  std::vector<result_value_t<T>> out;
  out.reserve(awaitables.size());
  if (awaitables.empty()) {
    co_return out;
  }

  auto ex = co_await asio::this_coro::executor;
  auto group =
      std::make_shared<detail::VectorGroupState<T>>(awaitables.size());
  group->wake = std::make_shared<asio::steady_timer>(ex);
  group->wake->expires_at(asio::steady_timer::time_point::max());

  for (std::size_t i = 0; i < awaitables.size(); ++i) {
    asio::co_spawn(ex,
                   detail::run_vector_gather_item<T>(std::move(awaitables[i]),
                                                      group, i),
                   asio::detached);
  }

  while (group->done_count.load() < group->total) {
    boost::system::error_code ec;
    co_await group->wake->async_wait(
        asio::redirect_error(asio::use_awaitable, ec));
    {
      std::lock_guard<std::mutex> lock(group->mutex);
      if (group->first_error) {
        std::rethrow_exception(group->first_error);
      }
    }
    if (group->done_count.load() < group->total) {
      group->wake_once.store(false);
      group->wake->expires_at(asio::steady_timer::time_point::max());
    }
  }

  for (auto& value : group->values) {
    out.emplace_back(std::move(*value));
  }
  co_return out;
}

template <typename Fn>
auto gather_limited(std::size_t total, std::size_t concurrency, Fn&& fn)
    -> asio::awaitable<std::vector<result_value_t<detail::await_result_t<
        std::invoke_result_t<std::decay_t<Fn>&, std::size_t>>>>> {
  using Awaitable = std::invoke_result_t<std::decay_t<Fn>&, std::size_t>;
  using T = detail::await_result_t<Awaitable>;
  using Value = result_value_t<T>;

  std::vector<Value> out;
  out.reserve(total);
  if (total == 0) {
    co_return out;
  }

  if (concurrency == 0) {
    concurrency = 1;
  }
  if (concurrency > total) {
    concurrency = total;
  }

  auto ex = co_await asio::this_coro::executor;
  auto group = std::make_shared<detail::LimitedGroupState<T>>(total);
  group->wake = std::make_shared<asio::steady_timer>(ex);
  group->wake->expires_at(asio::steady_timer::time_point::max());
  auto fn_ptr = std::make_shared<std::decay_t<Fn>>(std::forward<Fn>(fn));
  auto launch_one = std::make_shared<std::function<void()>>();
  std::weak_ptr<std::function<void()>> weak_launch_one = launch_one;

  *launch_one = [ex, group, fn_ptr, weak_launch_one] {
    if (group->stop.load()) {
      return;
    }

    auto index = group->next.fetch_add(1);
    if (index >= group->total) {
      return;
    }

    asio::co_spawn(
        ex,
        [group, fn_ptr, weak_launch_one, index]() mutable -> asio::awaitable<void> {
          std::optional<Value> value;
          std::exception_ptr error;
          try {
            auto awaitable = (*fn_ptr)(index);
            if constexpr (std::is_void_v<T>) {
              co_await std::move(awaitable);
              value = std::monostate{};
            } else {
              value = co_await std::move(awaitable);
            }
          } catch (...) {
            error = detail::normalize_exception(std::current_exception());
          }

          if (error) {
            bool should_wake = false;
            {
              std::lock_guard<std::mutex> lock(group->mutex);
              if (!group->first_error) {
                group->first_error = error;
                should_wake = true;
              }
            }
            group->stop.store(true);
            if (should_wake && !group->wake_once.exchange(true)) {
              group->wake->cancel();
            }
          } else {
            {
              std::lock_guard<std::mutex> lock(group->mutex);
              group->values[index] = std::move(value);
            }
            if (auto launch_one = weak_launch_one.lock()) {
              (*launch_one)();
            }
          }

          if (group->completed.fetch_add(1) + 1 == group->total) {
            if (!group->wake_once.exchange(true)) {
              group->wake->cancel();
            }
          }
          co_return;
        },
        asio::detached);
  };

  for (std::size_t i = 0; i < concurrency; ++i) {
    (*launch_one)();
  }

  for (;;) {
    boost::system::error_code ec;
    co_await group->wake->async_wait(
        asio::redirect_error(asio::use_awaitable, ec));
    {
      std::lock_guard<std::mutex> lock(group->mutex);
      if (group->first_error) {
        std::rethrow_exception(group->first_error);
      }
    }
    if (group->completed.load() >= group->total) {
      break;
    }
    if (!group->stop.load()) {
      group->wake_once.store(false);
      group->wake->expires_at(asio::steady_timer::time_point::max());
    }
  }

  for (auto& value : group->values) {
    out.emplace_back(std::move(*value));
  }
  co_return out;
}

template <typename Fn, typename OnResult>
asio::awaitable<void> for_each_limited(std::size_t total, std::size_t concurrency,
                                       Fn&& fn, OnResult&& on_result) {
  using Awaitable = std::invoke_result_t<std::decay_t<Fn>&, std::size_t>;
  using T = detail::await_result_t<Awaitable>;
  using FnType = std::decay_t<Fn>;
  using OnResultType = std::decay_t<OnResult>;

  if (total == 0) {
    co_return;
  }

  if (concurrency == 0) {
    concurrency = 1;
  }
  if (concurrency > total) {
    concurrency = total;
  }

  auto ex = co_await asio::this_coro::executor;
  auto group = std::make_shared<detail::LimitedForEachState>(total);
  group->wake = std::make_shared<asio::steady_timer>(ex);
  group->wake->expires_at(asio::steady_timer::time_point::max());
  struct Workers {
    Workers(std::shared_ptr<detail::LimitedForEachState> group, FnType fn,
            OnResultType on_result)
        : group(std::move(group)),
          fn(std::move(fn)),
          on_result(std::move(on_result)) {}

    std::shared_ptr<detail::LimitedForEachState> group;
    FnType fn;
    OnResultType on_result;

    asio::awaitable<void> run() {
      for (;;) {
        if (group->stop.load()) {
          co_return;
        }
        auto index = group->next.fetch_add(1);
        if (index >= group->total) {
          co_return;
        }

        std::exception_ptr error;
        try {
          auto awaitable = fn(index);
          if constexpr (std::is_void_v<T>) {
            co_await std::move(awaitable);
            on_result(index, std::monostate{});
          } else {
            auto value = co_await std::move(awaitable);
            on_result(index, std::move(value));
          }
        } catch (...) {
          error = detail::normalize_exception(std::current_exception());
        }

        if (error) {
          bool should_wake = false;
          {
            std::lock_guard<std::mutex> lock(group->mutex);
            if (!group->first_error) {
              group->first_error = error;
              should_wake = true;
            }
          }
          group->stop.store(true);
          if (should_wake && !group->wake_once.exchange(true)) {
            group->wake->cancel();
          }
        }

        if (group->completed.fetch_add(1) + 1 == group->total) {
          if (!group->wake_once.exchange(true)) {
            group->wake->cancel();
          }
        }
      }
    }
  };
  auto workers = std::make_shared<Workers>(
      group, std::forward<Fn>(fn), std::forward<OnResult>(on_result));

  for (std::size_t i = 0; i < concurrency; ++i) {
    asio::co_spawn(
        ex,
        [workers]() mutable -> asio::awaitable<void> {
          co_await workers->run();
        },
        asio::detached);
  }

  while (group->completed.load() < group->total) {
    boost::system::error_code ec;
    co_await group->wake->async_wait(
        asio::redirect_error(asio::use_awaitable, ec));
    {
      std::lock_guard<std::mutex> lock(group->mutex);
      if (group->first_error) {
        std::rethrow_exception(group->first_error);
      }
    }
    if (group->completed.load() < group->total) {
      group->wake_once.store(false);
      group->wake->expires_at(asio::steady_timer::time_point::max());
    }
  }

  co_return;
}

template <typename Awaitable>
asio::awaitable<detail::await_result_t<Awaitable>> shield(
    Awaitable&& awaitable) {
  using T = detail::await_result_t<Awaitable>;
  auto ex = co_await asio::this_coro::executor;
  auto task = detail::ensure_task(ex, std::forward<Awaitable>(awaitable));
  if constexpr (std::is_void_v<T>) {
    co_await task.await();
    co_return;
  } else {
    co_return co_await task.await();
  }
}

template <typename T>
struct WaitResult {
  std::vector<Task<T>> done;
  std::vector<Task<T>> pending;
};

template <typename T>
asio::awaitable<WaitResult<T>> wait(std::vector<Task<T>> tasks,
                                    ReturnWhen return_when =
                                        ReturnWhen::AllCompleted) {
  WaitResult<T> result;
  if (tasks.empty()) {
    co_return result;
  }

  auto ex = co_await asio::this_coro::executor;
  auto group = std::make_shared<detail::GroupState<asio::any_io_executor>>(
      tasks.size());
  group->wake = std::make_shared<asio::steady_timer>(ex);
  group->wake->expires_at(asio::steady_timer::time_point::max());

  for (const auto& task : tasks) {
    detail::add_completion<T>(task, [group, task, return_when] {
      if (return_when == ReturnWhen::FirstException) {
        if (auto error = detail::task_error<T>(task)) {
          bool should_wake = false;
          {
            std::lock_guard<std::mutex> lock(group->error_mutex);
            if (!group->first_error) {
              group->first_error = error;
              should_wake = true;
            }
          }
          if (should_wake && !group->wake_once.exchange(true)) {
            group->wake->cancel();
          }
        }
      }

      auto completed = group->done_count.fetch_add(1) + 1;
      if (return_when == ReturnWhen::FirstCompleted ||
          completed == group->total) {
        if (!group->wake_once.exchange(true)) {
          group->wake->cancel();
        }
      }
    });
  }

  while (true) {
    const auto completed = group->done_count.load();
    bool should_return = completed >= group->total;
    if (return_when == ReturnWhen::FirstCompleted && completed > 0) {
      should_return = true;
    }
    if (return_when == ReturnWhen::FirstException) {
      std::lock_guard<std::mutex> lock(group->error_mutex);
      should_return = should_return || static_cast<bool>(group->first_error);
    }
    if (should_return) {
      break;
    }

    boost::system::error_code ec;
    co_await group->wake->async_wait(
        asio::redirect_error(asio::use_awaitable, ec));
    if (group->done_count.load() < group->total) {
      group->wake_once.store(false);
      group->wake->expires_at(asio::steady_timer::time_point::max());
    }
  }

  for (const auto& task : tasks) {
    if (task.done()) {
      result.done.push_back(task);
    } else {
      result.pending.push_back(task);
    }
  }
  co_return result;
}

template <typename T>
asio::awaitable<std::vector<std::size_t>> as_completed(
    std::vector<Task<T>> tasks) {
  std::vector<std::size_t> order;
  order.reserve(tasks.size());
  if (tasks.empty()) {
    co_return order;
  }

  auto ex = co_await asio::this_coro::executor;
  auto group = std::make_shared<detail::GroupState<asio::any_io_executor>>(
      tasks.size());
  group->wake = std::make_shared<asio::steady_timer>(ex);
  group->wake->expires_at(asio::steady_timer::time_point::max());

  auto order_state = std::make_shared<std::pair<std::mutex, std::vector<std::size_t>>>();
  order_state->second.reserve(tasks.size());
  for (std::size_t i = 0; i < tasks.size(); ++i) {
    detail::add_completion<T>(tasks[i], [group, order_state, i] {
      {
        std::lock_guard<std::mutex> lock(order_state->first);
        order_state->second.push_back(i);
      }
      if (group->done_count.fetch_add(1) + 1 == group->total) {
        if (!group->wake_once.exchange(true)) {
          group->wake->cancel();
        }
      }
    });
  }

  while (group->done_count.load() < group->total) {
    boost::system::error_code ec;
    co_await group->wake->async_wait(
        asio::redirect_error(asio::use_awaitable, ec));
    if (group->done_count.load() < group->total) {
      group->wake_once.store(false);
      group->wake->expires_at(asio::steady_timer::time_point::max());
    }
  }

  {
    std::lock_guard<std::mutex> lock(order_state->first);
    order = std::move(order_state->second);
  }
  co_return order;
}

template <typename... Awaitables>
asio::awaitable<
    std::tuple<std::optional<result_value_t<detail::await_result_t<Awaitables>>>...>>
one_of(Awaitables&&... awaitables) {
  auto ex = co_await asio::this_coro::executor;
  auto tasks = std::make_tuple(
      detail::ensure_task(ex, std::forward<Awaitables>(awaitables))...);
  auto states = std::make_tuple(
      std::make_shared<detail::WaitState<detail::await_result_t<Awaitables>>>()...);
  auto group = std::make_shared<detail::GroupState<asio::any_io_executor>>(
      sizeof...(Awaitables));
  group->wake = std::make_shared<asio::steady_timer>(ex);
  group->wake->expires_at(asio::steady_timer::time_point::max());

  auto on_done = [group](std::size_t) {
    if (!group->wake_once.exchange(true)) {
      group->wake->cancel();
    }
  };
  detail::spawn_waiters(ex, tasks, states, group, on_done,
                        std::index_sequence_for<Awaitables...>{});

  boost::system::error_code ec;
  co_await group->wake->async_wait(
      asio::redirect_error(asio::use_awaitable, ec));

  auto error = detail::first_error(states, false);
  if (error && !detail::first_done_index(states).has_value()) {
    std::rethrow_exception(error);
  }
  co_return detail::make_optional_tuple(states);
}

template <typename... Awaitables>
asio::awaitable<
    std::tuple<std::optional<result_value_t<detail::await_result_t<Awaitables>>>...>>
race(Awaitables&&... awaitables) {
  auto ex = co_await asio::this_coro::executor;
  auto tasks = std::make_tuple(
      detail::ensure_task(ex, std::forward<Awaitables>(awaitables))...);
  auto states = std::make_tuple(
      std::make_shared<detail::WaitState<detail::await_result_t<Awaitables>>>()...);
  auto group = std::make_shared<detail::GroupState<asio::any_io_executor>>(
      sizeof...(Awaitables));
  group->wake = std::make_shared<asio::steady_timer>(ex);
  group->wake->expires_at(asio::steady_timer::time_point::max());

  auto on_done = [group, &tasks](std::size_t index) {
    if (!group->cancel_once.exchange(true)) {
      detail::cancel_all_except(tasks, index);
      if (!group->wake_once.exchange(true)) {
        group->wake->cancel();
      }
    }
  };
  detail::spawn_waiters(ex, tasks, states, group, on_done,
                        std::index_sequence_for<Awaitables...>{});

  boost::system::error_code ec;
  co_await group->wake->async_wait(
      asio::redirect_error(asio::use_awaitable, ec));

  auto error = detail::first_error(states, true);
  if (error && !detail::first_done_index(states).has_value()) {
    std::rethrow_exception(error);
  }
  co_return detail::make_optional_tuple(states);
}

template <typename Awaitable, typename Rep, typename Period>
asio::awaitable<detail::await_result_t<Awaitable>> wait_for(
    Awaitable&& awaitable, std::chrono::duration<Rep, Period> timeout) {
  using T = detail::await_result_t<Awaitable>;
  auto result = co_await race(std::forward<Awaitable>(awaitable), sleep(timeout));
  auto& value = std::get<0>(result);
  auto& timed_out = std::get<1>(result);
  if (timed_out.has_value()) {
    throw TimeoutError();
  }
  if constexpr (std::is_void_v<T>) {
    co_return;
  } else {
    co_return std::move(*value);
  }
}

template <typename Awaitable>
asio::awaitable<detail::await_result_t<Awaitable>> wait_for(
    Awaitable&& awaitable, std::uint64_t milliseconds) {
  co_return co_await wait_for(std::forward<Awaitable>(awaitable),
                              std::chrono::milliseconds(milliseconds));
}

class TaskGroup {
public:
  explicit TaskGroup(asio::any_io_executor executor)
      : executor_(std::move(executor)) {}

  TaskGroup(const TaskGroup&) = delete;
  TaskGroup& operator=(const TaskGroup&) = delete;

  ~TaskGroup() {
    if (!joined_) {
      cancel();
    }
  }

  template <typename T, typename Executor>
  Task<T> create_task(asio::awaitable<T, Executor> awaitable,
                      std::string name = {}) {
    auto task = asyncx::create_task(executor_, std::move(awaitable));
    task.set_name(std::move(name));
    cancelers_.push_back([task] { task.cancel(); });
    joins_.push_back(asyncx::create_task(
        executor_, detail::await_and_discard<T>(task)));
    return task;
  }

  void cancel() {
    for (auto& canceler : cancelers_) {
      canceler();
    }
  }

  asio::awaitable<void> join() {
    for (auto& join_task : joins_) {
      try {
        co_await join_task.await();
      } catch (const CancelledError&) {
      }
    }
    joined_ = true;
    co_return;
  }

  std::size_t size() const { return joins_.size(); }

private:
  asio::any_io_executor executor_;
  std::vector<std::function<void()>> cancelers_;
  std::vector<Task<void>> joins_;
  bool joined_ = false;
};

template <typename T>
struct PoolState {
  std::mutex mutex;
  std::optional<result_value_t<T>> value;
  std::exception_ptr error;
  bool done = false;
  std::shared_ptr<asio::steady_timer> wake;
};

template <typename Fn>
auto run_in_pool(asio::thread_pool& pool, Fn fn)
    -> asio::awaitable<std::invoke_result_t<std::decay_t<Fn>&>> {
  using FnType = std::decay_t<Fn>;
  using R = std::invoke_result_t<FnType&>;

  auto ex = co_await asio::this_coro::executor;
  auto state = std::make_shared<PoolState<R>>();
  state->wake = std::make_shared<asio::steady_timer>(ex);
  state->wake->expires_at(asio::steady_timer::time_point::max());

  auto work = std::make_shared<FnType>(std::move(fn));
  asio::post(pool, [state, work, ex] {
    std::optional<result_value_t<R>> value;
    std::exception_ptr error;
    try {
      if constexpr (std::is_void_v<R>) {
        (*work)();
        value = std::monostate{};
      } else {
        value = (*work)();
      }
    } catch (...) {
      error = std::current_exception();
    }

    {
      std::lock_guard<std::mutex> lock(state->mutex);
      state->value = std::move(value);
      state->error = std::move(error);
      state->done = true;
    }
    asio::post(ex, [state] {
      state->wake->expires_at(asio::steady_timer::clock_type::now());
    });
  });

  bool ready = false;
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    ready = state->done;
  }
  if (!ready) {
    boost::system::error_code ec;
    co_await state->wake->async_wait(
        asio::redirect_error(asio::use_awaitable, ec));
  }

  std::optional<result_value_t<R>> value;
  std::exception_ptr error;
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    value = std::move(state->value);
    error = state->error;
  }
  if (error) {
    std::rethrow_exception(error);
  }

  if constexpr (std::is_void_v<R>) {
    co_return;
  } else {
    co_return std::move(*value);
  }
}

template <typename Fn>
auto run_in_pool(ThreadPool& pool, Fn&& fn)
    -> decltype(run_in_pool(pool.native(), std::forward<Fn>(fn))) {
  return run_in_pool(pool.native(), std::forward<Fn>(fn));
}

template <typename Job>
decltype(auto) run_static_job(Job& job) {
  if constexpr (requires { job.run(); }) {
    return job.run();
  } else {
    return job();
  }
}

template <typename Job, typename... Args>
auto run_job_in_pool(asio::thread_pool& pool, Args&&... args)
    -> asio::awaitable<decltype(run_static_job(
        std::declval<Job&>()))> {
  return run_in_pool(pool, [job = Job{std::forward<Args>(args)...}]() mutable
                              -> decltype(run_static_job(job)) {
    return run_static_job(job);
  });
}

template <typename Job, typename... Args>
auto run_job_in_pool(ThreadPool& pool, Args&&... args)
    -> decltype(run_job_in_pool<Job>(pool.native(),
                                     std::forward<Args>(args)...)) {
  return run_job_in_pool<Job>(pool.native(), std::forward<Args>(args)...);
}

}  // namespace asyncx
