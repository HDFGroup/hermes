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
#include "hermes_shm/data_structures/thread_unsafe/list.h"
#include "hermes_shm/data_structures/string.h"
#include "hermes_shm/memory/allocator/stack_allocator.h"

using hermes_shm::ipc::list;

template<typename T>
void ListTest() {
  Allocator *alloc = alloc_g;
  list<T> lp(alloc);
  ListTestSuite<T, list<T>> test(lp, alloc);

  test.EmplaceTest(30);
  test.ForwardIteratorTest();
  test.ConstForwardIteratorTest();
  test.CopyConstructorTest();
  test.CopyAssignmentTest();
  test.MoveConstructorTest();
  test.MoveAssignmentTest();
  test.EmplaceFrontTest();
  test.ModifyEntryCopyIntoTest();
  test.ModifyEntryMoveIntoTest();
  test.EraseTest();
}

TEST_CASE("ListOfInt") {
  Allocator *alloc = alloc_g;
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  ListTest<int>();
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
}

TEST_CASE("ListOfString") {
  Allocator *alloc = alloc_g;
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  ListTest<lipc::string>();
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
}

TEST_CASE("ListOfStdString") {
  Allocator *alloc = alloc_g;
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  ListTest<std::string>();
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
}
