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
#include "hermes_shm/data_structures/data_structure.h"

TEST_CASE("SerializePod") {
  hipc::ShmSerializer istream;
  Allocator *alloc = HERMES_MEMORY_MANAGER->GetDefaultAllocator();
  int a = 1;
  double b = 2;
  float c = 3;
  size_t size = sizeof(int) + sizeof(double) +
    sizeof(float) + sizeof(allocator_id_t);
  REQUIRE(istream.shm_buf_size(alloc->GetId(), a, b, c) == size);
  char *buf = istream.serialize(alloc, a, b, c);

  hipc::ShmSerializer ostream;
  Allocator *alloc2 = ostream.deserialize(buf);
  REQUIRE(alloc == alloc2);
  auto a2 = ostream.deserialize<int>(alloc2, buf);
  REQUIRE(a2 == a);
  auto b2 = ostream.deserialize<double>(alloc2, buf);
  REQUIRE(b2 == b);
  auto c2 = ostream.deserialize<float>(alloc2, buf);
  REQUIRE(c2 == c);
}

TEST_CASE("SerializeString") {
  hipc::ShmSerializer istream;
  Allocator *alloc = HERMES_MEMORY_MANAGER->GetDefaultAllocator();

  auto a = hipc::make_uptr<hipc::string>(alloc, "h1");
  auto b = hipc::make_uptr<hipc::string>(alloc, "h2");
  int c;
  size_t size = 2 * sizeof(hipc::OffsetPointer) +
    sizeof(int) + sizeof(allocator_id_t);
  REQUIRE(istream.shm_buf_size(alloc->GetId(), *a, *b, c) == size);
  char *buf = istream.serialize(alloc, *a, *b, c);

  hipc::ShmSerializer ostream;
  Allocator *alloc2 = ostream.deserialize(buf);
  REQUIRE(alloc == alloc2);
  hipc::mptr<hipc::string> a2;
  ostream.deserialize<hipc::string>(alloc2, buf, a2);
  REQUIRE(*a2 == *a);
  hipc::mptr<hipc::string> b2;
  ostream.deserialize<hipc::string>(alloc2, buf, b2);
  REQUIRE(*b2 == *b);
  int c2 = ostream.deserialize<int>(alloc2, buf);
  REQUIRE(c2 == c);
}

