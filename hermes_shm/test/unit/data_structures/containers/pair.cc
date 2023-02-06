//
// Created by lukemartinlogan on 1/15/23.
//

#include "basic_test.h"
#include "test_init.h"
#include "hermes_shm/data_structures/pair.h"
#include "hermes_shm/data_structures/string.h"

template<typename FirstT, typename SecondT>
void PairTest() {
  Allocator *alloc = alloc_g;

  // Construct test
  {
    CREATE_SET_VAR_TO_INT_OR_STRING(FirstT, first, 124);
    CREATE_SET_VAR_TO_INT_OR_STRING(SecondT, second, 130);
    lipc::pair<FirstT, SecondT> data(alloc, first, second);
    REQUIRE(*data.first_ == first);
    REQUIRE(*data.second_ == second);
  }

  // Copy test
  {
    CREATE_SET_VAR_TO_INT_OR_STRING(FirstT, first, 124);
    CREATE_SET_VAR_TO_INT_OR_STRING(SecondT, second, 130);
    lipc::pair<FirstT, SecondT> data(alloc, first, second);
    lipc::pair<FirstT, SecondT> cpy(data);
    REQUIRE(*cpy.first_ == first);
    REQUIRE(*cpy.second_ == second);
  }

  // Move test
  {
    CREATE_SET_VAR_TO_INT_OR_STRING(FirstT, first, 124);
    CREATE_SET_VAR_TO_INT_OR_STRING(SecondT, second, 130);
    lipc::pair<FirstT, SecondT> data(alloc, first, second);
    lipc::pair<FirstT, SecondT> cpy(std::move(data));
    REQUIRE(*cpy.first_ == first);
    REQUIRE(*cpy.second_ == second);
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
  PairTest<int, lipc::string>();
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
}