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


#include "hermes_shm/data_structures/smart_ptr/smart_ptr_base.h"
#include "basic_test.h"
#include "test_init.h"
#include "hermes_shm/data_structures/ipc/string.h"
#include "smart_ptr.h"

using hermes_shm::ipc::string;
using hermes_shm::ipc::uptr;
using hermes_shm::ipc::mptr;
using hermes_shm::ipc::TypedPointer;

template<typename T>
void UniquePtrTest() {
  Allocator *alloc = alloc_g;
  hipc::SmartPtrTestSuite<T, uptr<T>> test;
  CREATE_SET_VAR_TO_INT_OR_STRING(T, num, 25);
  test.ptr_ = hipc::make_uptr<T>(num);
  test.DereferenceTest(num);
  test.MoveConstructorTest(num);
  test.MoveAssignmentTest(num);
  test.SerializeationConstructorTest(num);
  test.SerializeationOperatorTest(num);
}

TEST_CASE("UniquePtrOfInt") {
  Allocator *alloc = alloc_g;
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  UniquePtrTest<int>();
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
}

TEST_CASE("UniquePtrOfString") {
  Allocator *alloc = alloc_g;
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  UniquePtrTest<hipc::string>();
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
}
