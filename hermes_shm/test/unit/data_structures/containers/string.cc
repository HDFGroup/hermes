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
#include "hermes_shm/data_structures/string.h"

using hermes_shm::ipc::string;

void TestString() {
  Allocator *alloc = alloc_g;

  auto text1 = string("hello1");
  REQUIRE(text1 == "hello1");
  REQUIRE(text1 != "h");
  REQUIRE(text1 != "asdfklaf");

  auto text2 = string("hello2");
  REQUIRE(text2 == "hello2");

  string text3 = text1 + text2;
  REQUIRE(text3 == "hello1hello2");

  string text4(6);
  memcpy(text4.data_mutable(), "hello4", strlen("hello4"));

  string text5 = text4;
  REQUIRE(text5 == "hello4");
  REQUIRE(text5.header_ != text4.header_);

  string text6 = std::move(text5);
  REQUIRE(text6 == "hello4");
}

TEST_CASE("String") {
  Allocator *alloc = alloc_g;
  REQUIRE(IS_SHM_ARCHIVEABLE(string));
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  TestString();
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
}
