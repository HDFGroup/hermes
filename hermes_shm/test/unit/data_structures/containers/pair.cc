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
#include "hermes_shm/data_structures/ipc/pair.h"
#include "hermes_shm/data_structures/ipc/string.h"

template<typename FirstT, typename SecondT>
void PairTest() {
  Allocator *alloc = alloc_g;

  // Construct test
  PAGE_DIVIDE("Construct") {
    CREATE_SET_VAR_TO_INT_OR_STRING(FirstT, first, 124);
    CREATE_SET_VAR_TO_INT_OR_STRING(SecondT, second, 130);
    auto data = hipc::make_uptr<hipc::pair<FirstT, SecondT>>(
      alloc, first, second);
    REQUIRE(*data->first_ == first);
    REQUIRE(*data->second_ == second);
  }

  // Copy constructor test
  PAGE_DIVIDE("Copy constructor") {
    CREATE_SET_VAR_TO_INT_OR_STRING(FirstT, first, 124);
    CREATE_SET_VAR_TO_INT_OR_STRING(SecondT, second, 130);
    auto data = hipc::make_uptr<hipc::pair<FirstT, SecondT>>(
      alloc, first, second);
    auto cpy = hipc::make_uptr<hipc::pair<FirstT, SecondT>>(
      alloc, *data);
    REQUIRE(*cpy->first_ == first);
    REQUIRE(*cpy->second_ == second);
  }

  // Copy assignment test
  PAGE_DIVIDE("Copy assignment operator") {
    CREATE_SET_VAR_TO_INT_OR_STRING(FirstT, first, 124);
    CREATE_SET_VAR_TO_INT_OR_STRING(SecondT, second, 130);
    auto data = hipc::make_uptr<hipc::pair<FirstT, SecondT>>(
      alloc, first, second);
    auto cpy = hipc::make_uptr<hipc::pair<FirstT, SecondT>>(
      alloc);
    *cpy = *data;
    REQUIRE(*cpy->first_ == first);
    REQUIRE(*cpy->second_ == second);
  }

  // Move constructor test
  PAGE_DIVIDE("Move constructor") {
    CREATE_SET_VAR_TO_INT_OR_STRING(FirstT, first, 124);
    CREATE_SET_VAR_TO_INT_OR_STRING(SecondT, second, 130);
    auto data = hipc::make_uptr<hipc::pair<FirstT, SecondT>>(
      alloc, first, second);
    auto cpy = hipc::make_uptr<hipc::pair<FirstT, SecondT>>(
      alloc, std::move(*data));
    REQUIRE(*cpy->first_ == first);
    REQUIRE(*cpy->second_ == second);
  }

  // Move assignment test
  PAGE_DIVIDE("Move assignment operator") {
    CREATE_SET_VAR_TO_INT_OR_STRING(FirstT, first, 124);
    CREATE_SET_VAR_TO_INT_OR_STRING(SecondT, second, 130);
    auto data = hipc::make_uptr<hipc::pair<FirstT, SecondT>>(
      alloc, first, second);
    auto cpy = hipc::make_uptr<hipc::pair<FirstT, SecondT>>(
      alloc);
    *cpy = std::move(*data);
    REQUIRE(*cpy->first_ == first);
    REQUIRE(*cpy->second_ == second);
  }
}

TEST_CASE("PairOfIntInt") {
  Allocator *alloc = alloc_g;
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  PairTest<int, int>();
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
}

TEST_CASE("PairOfIntString") {
  Allocator *alloc = alloc_g;
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  PairTest<int, hipc::string>();
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
}
