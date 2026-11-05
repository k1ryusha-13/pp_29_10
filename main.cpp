#include <iostream>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <functional>
#include <chrono>

struct task_t
{
  void operator()() {};
};

struct th_pool_t
{
  using task_function_t = std::function<void(void)>;

  explicit th_pool_t(unsigned int thread_count);
  ~th_pool_t();

  void submit(task_function_t task_function);

  void stop_now();
  void wait();

private:
  void execute();
  std::queue<task_function_t> task_queue_;
  std::mutex mutex_;
  std::condition_variable condition_;
  bool stop_requested_{false};
  std::vector<std::thread> threads_;

  size_t active_count_{0};
  std::condition_variable wait_condition_;
};

void th_pool_t::submit(task_function_t task_function)
{
  mutex_.lock();
  task_queue_.push(std::move(task_function));
  mutex_.unlock();
  condition_.notify_one();
}

th_pool_t::th_pool_t(unsigned int thread_count)
{
  auto thread_function = [this](){
    execute();
  };
  for (; thread_count; --thread_count)
  {
    threads_.emplace_back(thread_function);
  }
}

void th_pool_t::stop_now()
{
  std::lock_guard<std::mutex> lock(mutex_);
  stop_requested_ = true;

  std::queue<task_function_t> empty;
  std::swap(task_queue_, empty);

  condition_.notify_all();
}

void th_pool_t::wait()
{
  std::unique_lock<std::mutex> lock(mutex_);
  wait_condition_.wait(lock, [this](){ return task_queue_.empty() && active_count_ == 0; });
}

th_pool_t::~th_pool_t()
{
  mutex_.lock();
  stop_requested_ = true;
  mutex_.unlock();
  condition_.notify_all();
  for (auto &&thread : threads_)
  {
    thread.join();
  }
}

void th_pool_t::execute()
{
  std::unique_lock lock(mutex_);
  do
  {
    auto proceed = [&]()
    {
      return !task_queue_.empty() || stop_requested_;
    };
    condition_.wait(lock, proceed);
    if (task_queue_.empty())
    {
      continue;
    }

    auto current_task = std::move(task_queue_.front());
    task_queue_.pop();

    active_count_++;

    lock.unlock();
    try
    {
      current_task();
    }
    catch (...)
    {
    }
    lock.lock();

    active_count_--;

    if (task_queue_.empty() && active_count_ == 0)
    {
      wait_condition_.notify_all();
    }

  } while (!stop_requested_ || !task_queue_.empty());
}
