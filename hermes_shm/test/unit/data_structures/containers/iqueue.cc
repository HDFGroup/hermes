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
#include "iqueue.h"
#include "hermes_shm/data_structures/ipc/iqueue.h"
#include "hermes_shm/data_structures/smart_ptr/smart_ptr_base.h"

using hermes_shm::ipc::mptr;
using hermes_shm::ipc::make_mptr;
using hermes_shm::ipc::iqueue;

template<typename T>
void IqueueTest() {
  Allocator *alloc = alloc_g;
  auto lp = hipc::make_uptr<iqueue<T>>(alloc);
  IqueueTestSuite<T, iqueue<T>> test(*lp, alloc);

  test.EnqueueTest(30);
  test.ForwardIteratorTest();
  test.ConstForwardIteratorTest();
  test.DequeueTest(30);
  test.DequeueMiddleTest();
  test.EraseTest();
}

TEST_CASE("IqueueOfMpPage") {
  Allocator *alloc = alloc_g;
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  IqueueTest<MpPage>();
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
}
