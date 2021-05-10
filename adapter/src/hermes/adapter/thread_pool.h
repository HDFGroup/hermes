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

#ifndef HERMES_ADAPTER_THREAD_POOL_H
#define HERMES_ADAPTER_THREAD_POOL_H

/**
 * Standard header
 */
#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <future>

namespace hermes::adapter {
class ThreadPool {
 public:
  ThreadPool(unsigned num_threads = std::thread::hardware_concurrency()) {
    while (num_threads--) {
      threads.emplace_back([this] {
        while(true) {
          std::unique_lock<std::mutex> lock(mutex);
          condvar.wait(lock, [this] {return !queue.empty();});
          auto task = std::move(queue.front());
          if (task.valid()) {
            queue.pop();
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

  template<typename F, typename R = std::result_of_t<F&&()>>
  std::future<R> run(F&& f) const {
    auto task = std::packaged_task<R()>(std::forward<F>(f));
    auto future = task.get_future();
    {
      std::lock_guard<std::mutex> lock(mutex);
      // conversion to packaged_task<void()> erases the return type
      // so it can be stored in the queue. the future will still
      // contain the correct type
      queue.push(std::packaged_task<void()>(std::move(task)));
    }
    condvar.notify_one();
    return future;
  }

  ~ThreadPool() {
    // push a single empty task onto the queue and notify all threads,
    // then wait for them to terminate
    {
      std::lock_guard<std::mutex> lock(mutex);
      queue.push({});
    }
    condvar.notify_all();
    for (auto& thread : threads) {
      thread.join();
    }
  }

 private:
  std::vector<std::thread> threads;
  mutable std::queue<std::packaged_task<void()>> queue;
  mutable std::mutex mutex;
  mutable std::condition_variable condvar;
};
} // hermes::adapter
#endif  // HERMES_ADAPTER_THREAD_POOL_H
