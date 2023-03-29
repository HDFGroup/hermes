//
// Created by lukemartinlogan on 2/26/23.
//

#include "hermes_shm/util/timer.h"
#include "hermes_shm/thread/lock.h"
#include "test_init.h"
#include "basic_test.h"
#include <iostream>

TEST_CASE("TestMutex") {
  Timer t;
  t.Resume();
  int j = 0;
  size_t ops = (1ull << 26);
  hshm::Mutex lock;
  for (size_t i = 0; i < ops; ++i) {
    lock.TryLock();
  }
  t.Pause();
  std::cout << ops / t.GetUsec() << std::endl;
}