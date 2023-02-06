/*
 * Copyright (C) 2022  SCS Lab <scslab@iit.edu>,
 * Luke Logan <llogan@hawk.iit.edu>,
 * Jaime Cernuda Garcia <jcernudagarcia@hawk.iit.edu>
 * Jay Lofstead <gflofst@sandia.gov>,
 * Anthony Kougkas <akougkas@iit.edu>,
 * Xian-He Sun <sun@iit.edu>
 *
 * This file is part of HermesShm
 *
 * HermesShm is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU General Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 */

#include "basic_test.h"
#include "test_init.h"
#include "hermes_shm/data_structures/string.h"
#include "hermes_shm/memory/allocator/stack_allocator.h"

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
