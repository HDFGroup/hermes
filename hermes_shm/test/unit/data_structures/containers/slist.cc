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
#include "list.h"
#include "hermes_shm/data_structures/thread_unsafe/slist.h"
#include "hermes_shm/data_structures/string.h"
#include "hermes_shm/memory/allocator/stack_allocator.h"

using hermes_shm::ipc::slist;

template<typename T>
void SListTest() {
  Allocator *alloc = alloc_g;
  slist<T> lp(alloc);
  ListTestSuite<T, slist<T>> test(lp, alloc);

  test.EmplaceTest(30);
  test.ForwardIteratorTest();
  test.CopyConstructorTest();
  test.CopyAssignmentTest();
  test.MoveConstructorTest();
  test.MoveAssignmentTest();
  test.EmplaceFrontTest();
  test.ModifyEntryCopyIntoTest();
  test.ModifyEntryMoveIntoTest();
  test.EraseTest();
}

TEST_CASE("SListOfInt") {
  Allocator *alloc = alloc_g;
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  SListTest<int>();
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
}

TEST_CASE("SListOfString") {
  Allocator *alloc = alloc_g;
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  SListTest<lipc::string>();
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
}

TEST_CASE("SListOfStdString") {
  Allocator *alloc = alloc_g;
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  SListTest<std::string>();
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
}
