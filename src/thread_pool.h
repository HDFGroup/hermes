/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Distributed under BSD 3-Clause license.                                   *
 * Copyright by The HDF Group.                                               *
 * Copyright by the Illinois Institute of Technology.                        *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of Hermes. The full Hermes copyright notice, including  *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the top directory. If you do not  *
 * have access to the file, you may request a copy from help@hdfgroup.org.   *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#ifndef HERMES_THREAD_POOL_H_
#define HERMES_THREAD_POOL_H_

#include <condition_variable>
#include <future>
#include <mutex>
#include <queue>
#include <thread>

namespace hermes {
/**
   A class to represent thread pool
 */
class ThreadPool {
 public:
  /** construct thread pool with \a num_threads number of threads */
  explicit ThreadPool(
      unsigned num_threads = std::thread::hardware_concurrency()) {
    while (num_threads--) {
      threads.emplace_back([this] {
        while (true) {
          std::unique_lock<std::mutex> lock(mutex);
          condvar.wait(lock, [this]() {
            return !queue_high.empty() || !queue_low.empty();
          });
          bool high_priority = !queue_high.empty();
          auto task = high_priority ? std::move(queue_high.front())
                                    : std::move(queue_low.front());
          if (task.valid()) {
            if (high_priority) {
              queue_high.pop();
            } else {
              queue_low.pop();
            }
            lock.unlock();
            // run the task - this cannot throw; any exception
            // will be stored in the corresponding future
            task();
          } else {
            // an empty task is used to signal end of stream
            // don't pop it off the top; all threads need to see it
            break;
          }
        }
      });
    }
  }

  /** a template for running thread pool */
  template <typename F, typename R = std::result_of_t<F && ()>>
  std::future<R> run(F&& f, bool high_priority = false) const {
    auto task = std::packaged_task<R()>(std::forward<F>(f));
    auto future = task.get_future();
    {
      std::lock_guard<std::mutex> lock(mutex);
      // conversion to packaged_task<void()> erases the return type
      // so it can be stored in the queue. the future will still
      // contain the correct type
      if (high_priority) {
        queue_high.push(std::packaged_task<void()>(std::move(task)));
      } else {
        queue_low.push(std::packaged_task<void()>(std::move(task)));
      }
    }
    condvar.notify_one();
    return future;
  }

  ~ThreadPool() {
    // push a single empty task onto the queue and notify all threads,
    // then wait for them to terminate
    {
      std::lock_guard<std::mutex> lock(mutex);
      queue_low.push({});
    }
    condvar.notify_all();
    for (auto& thread : threads) {
      thread.join();
    }
  }

 private:
  std::vector<std::thread> threads; /**< a vector of threads */
  /** high-priority  queue */
  mutable std::queue<std::packaged_task<void()>> queue_low;
  /** low-priority queue */
  mutable std::queue<std::packaged_task<void()>> queue_high;
  mutable std::mutex mutex;                /**< mutex lock */
  mutable std::condition_variable condvar; /**< conditional variable */
};
}  // namespace hermes
#endif  // HERMES_THREAD_POOL_H_
