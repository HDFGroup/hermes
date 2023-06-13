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

#include "basic_test.h"
#include "test_init.h"
#include "hermes_shm/data_structures/ipc/mpsc_queue.h"
#include "queue.h"

/**
 * TEST MPSC QUEUE
 * */

TEST_CASE("TestMpscQueueInt") {
  Allocator *alloc = alloc_g;
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  ProduceThenConsume<hipc::mpsc_queue<int>, int>(1, 1, 32, 32);
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
}

TEST_CASE("TestMpscQueueString") {
  Allocator *alloc = alloc_g;
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  ProduceThenConsume<hipc::mpsc_queue<hipc::string>, hipc::string>(
    1, 1, 32, 32);
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
}

TEST_CASE("TestMpscQueueIntMultiThreaded") {
  Allocator *alloc = alloc_g;
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  ProduceAndConsume<hipc::mpsc_queue<int>, int>(8, 1, 8192, 32);
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
}

TEST_CASE("TestMpscQueueStringMultiThreaded") {
  Allocator *alloc = alloc_g;
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  ProduceAndConsume<hipc::mpsc_queue<hipc::string>, hipc::string>(
    8, 1, 8192, 32);
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
}
