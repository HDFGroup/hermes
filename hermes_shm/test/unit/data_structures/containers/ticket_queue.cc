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
#include "hermes_shm/data_structures/ipc/ticket_queue.h"
#include "hermes_shm/data_structures/ipc/ticket_stack.h"
#include "hermes_shm/data_structures/ipc/split_ticket_queue.h"
#include "queue.h"

/**
 * TEST TICKET QUEUE
 * */

TEST_CASE("TestTicketStackInt") {
  Allocator *alloc = alloc_g;
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  ProduceThenConsume<hipc::ticket_stack<int>, int>(1, 1, 32, 32);
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
}

TEST_CASE("TestTicketStackIntMultiThreaded") {
  Allocator *alloc = alloc_g;
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  ProduceThenConsume<hipc::ticket_stack<int>, int>(8, 1, 8192, 8192 * 8);
  ProduceAndConsume<hipc::ticket_stack<int>, int>(8, 1, 8192, 64);
  ProduceAndConsume<hipc::ticket_stack<int>, int>(8, 8, 8192, 64);
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
}

TEST_CASE("TestTicketQueueInt") {
  Allocator *alloc = alloc_g;
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  ProduceThenConsume<hipc::ticket_queue<int>, int>(1, 1, 32, 32);
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
}

TEST_CASE("TestTicketQueueIntMultiThreaded") {
  Allocator *alloc = alloc_g;
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  ProduceAndConsume<hipc::ticket_queue<int>, int>(8, 1, 8192, 64);
  ProduceAndConsume<hipc::ticket_queue<int>, int>(8, 8, 8192, 64);
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
}

TEST_CASE("TestSplitTicketQueueInt") {
  Allocator *alloc = alloc_g;
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  ProduceThenConsume<hipc::split_ticket_queue<int>, int>(1, 1, 32, 32);
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
}

TEST_CASE("TestSplitTicketQueueIntMultiThreaded") {
  Allocator *alloc = alloc_g;
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  ProduceAndConsume<hipc::split_ticket_queue<int>, int>(8, 1, 8192, 64);
  ProduceAndConsume<hipc::split_ticket_queue<int>, int>(8, 8, 8192, 64);
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
}
