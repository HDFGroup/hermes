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
#include "hermes_shm/data_structures/ipc/string.h"

template<typename FirstT, typename SecondT>
void TupleTest() {
  Allocator *alloc = alloc_g;

  // Construct test
  /*{
    CREATE_SET_VAR_TO_INT_OR_STRING(FirstT, first, 124);
    CREATE_SET_VAR_TO_INT_OR_STRING(SecondT, second, 130);
    hipc::ShmHeader<hipc::ShmStruct<FirstT, SecondT>> hdr;
    hipc::ShmStruct<FirstT, SecondT>
      data(hdr, alloc,
           hshm::make_argpack(first),
           hshm::make_argpack(second));
    REQUIRE(data.template Get<0>() == first);
    REQUIRE(data.template Get<1>() == second);
  }

  // Copy test
  {
    CREATE_SET_VAR_TO_INT_OR_STRING(FirstT, first, 124);
    CREATE_SET_VAR_TO_INT_OR_STRING(SecondT, second, 130);
    hipc::tuple<FirstT, SecondT> data(alloc, first, second);
    hipc::tuple<FirstT, SecondT> cpy(data);
    REQUIRE(cpy.template Get<0>() == first);
    REQUIRE(cpy.template Get<1>() == second);
  }

  // Move test
  {
    CREATE_SET_VAR_TO_INT_OR_STRING(FirstT, first, 124);
    CREATE_SET_VAR_TO_INT_OR_STRING(SecondT, second, 130);
    hipc::tuple<FirstT, SecondT> data(alloc, first, second);
    hipc::tuple<FirstT, SecondT> cpy(std::move(data));
    REQUIRE(cpy.template Get<0>() == first);
    REQUIRE(cpy.template Get<1>() == second);
  }*/
}

#include <functional>

int y() {
  return 0;
}


class Y {
 public:
  Y() = default;
  explicit Y(int x) {}
  explicit Y(int x, int w) {}
};

TEST_CASE("TupleOfIntInt") {
  Allocator *alloc = alloc_g;
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);

  TupleTest<int, int>();
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
}

TEST_CASE("TupleOfIntString") {
  Allocator *alloc = alloc_g;
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  // TupleTest<int, hipc::string>();
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
}
