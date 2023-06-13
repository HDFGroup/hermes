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
#include "hermes_shm/data_structures/containers/charbuf.h"

using hshm::ipc::string;

void TestCharbuf() {
  Allocator *alloc = alloc_g;

  PAGE_DIVIDE("Construct from allocator") {
    hshm::charbuf data(256);
    memset(data.data(), 0, 256);
    REQUIRE(data.size() == 256);
    REQUIRE(data.GetAllocator() == alloc);
  }

  PAGE_DIVIDE("Construct from malloc") {
    char *ptr = (char*)malloc(256);
    hshm::charbuf data(ptr, 256);
    memset(data.data(), 0, 256);
    REQUIRE(data.size() == 256);
    REQUIRE(data.GetAllocator() == nullptr);
    free(ptr);
  }

  PAGE_DIVIDE("Resize null charbuf to higher value") {
    hshm::charbuf data;
    data.resize(256);
    REQUIRE(data.size() == 256);
    REQUIRE(data.GetAllocator() == alloc);
  }

  PAGE_DIVIDE("Resize null charbuf to 0 value") {
    hshm::charbuf data;
    data.resize(0);
    REQUIRE(data.size() == 0);
    REQUIRE(data.GetAllocator() == nullptr);
  }

  PAGE_DIVIDE("Resize destructable charbuf to 0 value") {
    hshm::charbuf data(8192);
    data.resize(0);
    REQUIRE(data.size() == 0);
    REQUIRE(data.GetAllocator() == alloc);
  }

  PAGE_DIVIDE("Resize destructable charbuf to lower value") {
    hshm::charbuf data(8192);
    data.resize(256);
    REQUIRE(data.size() == 256);
    REQUIRE(data.GetAllocator() == alloc);
  }

  PAGE_DIVIDE("Resize destructable charbuf to higher value") {
    hshm::charbuf data(256);
    data.resize(8192);
    REQUIRE(data.size() == 8192);
    REQUIRE(data.GetAllocator() == alloc);
  }

  PAGE_DIVIDE("Resize indestructable charbuf to higher value") {
    char *ptr = (char*)malloc(256);
    hshm::charbuf data(ptr, 256);
    data.resize(8192);
    REQUIRE(data.size() == 8192);
    free(ptr);
  }

  PAGE_DIVIDE("Resize indestructable charbuf to lower value") {
    char *ptr = (char*)malloc(8192);
    hshm::charbuf data(ptr, 8192);
    data.resize(256);
    REQUIRE(data.size() == 256);
    free(ptr);
  }

  PAGE_DIVIDE("Move construct from destructable") {
    hshm::charbuf data1(8192);
    hshm::charbuf data2(std::move(data1));
    REQUIRE(data2.size() == 8192);
  }

  PAGE_DIVIDE("Move construct from indestructable") {
    char *ptr1 = (char*)malloc(8192);
    hshm::charbuf data1(ptr1, 8192);
    hshm::charbuf data2(std::move(data1));
    REQUIRE(data2.size() == 8192);
    free(ptr1);
  }

  PAGE_DIVIDE("Move assign between two destructables") {
    hshm::charbuf data1(8192);
    hshm::charbuf data2(512);
    data1 = std::move(data2);
    REQUIRE(data1.size() == 512);
  }

  PAGE_DIVIDE("Move assign between two indestructables") {
    char *ptr1 = (char*)malloc(8192);
    hshm::charbuf data1(ptr1, 8192);
    char *ptr2 = (char*)malloc(512);
    hshm::charbuf data2(ptr2, 512);
    data1 = std::move(data2);
    REQUIRE(data1.size() == 512);
    free(ptr1);
    free(ptr2);
  }

  PAGE_DIVIDE("Move assign indestructable -> destructable") {
    hshm::charbuf data1(8192);
    char *ptr2 = (char*)malloc(512);
    hshm::charbuf data2(ptr2, 512);
    data1 = std::move(data2);
    REQUIRE(data1.size() == 512);
    free(ptr2);
  }

  PAGE_DIVIDE("Move assign destructable -> indestructable") {
    char *ptr1 = (char*)malloc(8192);
    hshm::charbuf data1(ptr1, 8192);
    hshm::charbuf data2(512);
    data1 = std::move(data2);
    REQUIRE(data1.size() == 512);
    free(ptr1);
  }

  PAGE_DIVIDE("Move assign to null") {
    hshm::charbuf data1;
    hshm::charbuf data2(512);
    data1 = std::move(data2);
    REQUIRE(data1.size() == 512);
  }

  PAGE_DIVIDE("Copy construct from destructable") {
    hshm::charbuf data1(8192);
    hshm::charbuf data2(data1);
    REQUIRE(data1.size() == 8192);
    REQUIRE(data2.size() == 8192);
    REQUIRE(data1 == data2);
  }

  PAGE_DIVIDE("Copy construct from indestructable") {
    char *ptr1 = (char*)malloc(8192);
    hshm::charbuf data1(ptr1, 8192);
    hshm::charbuf data2(data1);
    REQUIRE(data1.size() == 8192);
    REQUIRE(data2.size() == 8192);
    REQUIRE(data1 == data2);
    free(ptr1);
  }

  PAGE_DIVIDE("Copy assign between two destructables") {
    hshm::charbuf data1(8192);
    hshm::charbuf data2(512);
    data1 = data2;
    REQUIRE(data2.size() == 512);
    REQUIRE(data1.size() == 512);
    REQUIRE(data1 == data2);
  }

  PAGE_DIVIDE("Copy assign between two indestructables") {
    char *ptr1 = (char*)malloc(8192);
    hshm::charbuf data1(ptr1, 8192);
    char *ptr2 = (char*)malloc(512);
    hshm::charbuf data2(ptr2, 512);
    data1 = data2;
    REQUIRE(data2.size() == 512);
    REQUIRE(data1.size() == 512);
    REQUIRE(data1 == data2);
    free(ptr1);
    free(ptr2);
  }

  PAGE_DIVIDE("Copy assign indestructable -> destructable") {
    hshm::charbuf data1(8192);
    char *ptr2 = (char*)malloc(512);
    hshm::charbuf data2(ptr2, 512);
    data1 = data2;
    REQUIRE(data2.size() == 512);
    REQUIRE(data1.size() == 512);
    free(ptr2);
  }

  PAGE_DIVIDE("Copy assign destructable -> indestructable") {
    char *ptr1 = (char*)malloc(8192);
    hshm::charbuf data1(ptr1, 8192);
    hshm::charbuf data2(512);
    data1 = data2;
    REQUIRE(data2.size() == 512);
    REQUIRE(data1.size() == 512);
    free(ptr1);
  }

  PAGE_DIVIDE("Copy assign to null") {
    char *ptr1 = (char*)malloc(8192);
    hshm::charbuf data1;
    hshm::charbuf data2(512);
    data1 = data2;
    REQUIRE(data2.size() == 512);
    REQUIRE(data1.size() == 512);
    free(ptr1);
  }
}

TEST_CASE("Charbuf") {
  Allocator *alloc = alloc_g;
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  TestCharbuf();
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
}
