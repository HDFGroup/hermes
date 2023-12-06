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

#include "stdio_adapter_test.h"
#include "hermes_adapters/mapper/mapper_factory.h"
#include "hermes_adapters/mapper/mapper_factory.h"
#include "hermes_adapters/adapter_constants.h"

using hermes::adapter::MapperFactory;
using hermes::adapter::BlobPlacements;
using hermes::adapter::kMapperType;

TEST_CASE("SingleWrite", "[process=" + std::to_string(TEST_INFO->comm_size_) +
                             "]"
                             "[operation=single_write]"
                             "[request_size=type-fixed][repetition=1]"
                             "[pattern=sequential][file=1]") {
  TEST_INFO->Pretest();
  const size_t kPageSize = MEGABYTES(1);
  SECTION("Map a one request") {
    auto mapper = hermes::adapter::MapperFactory().Get(kMapperType);
    size_t total_size = TEST_INFO->request_size_;
    FILE* fp = fopen(TEST_INFO->new_file_.hermes_.c_str(), "w+");
    REQUIRE(fp != nullptr);
    size_t offset = 0;
    REQUIRE(kPageSize > total_size + offset);
    BlobPlacements mapping;
    mapper->map(offset, total_size, kPageSize, mapping);
    REQUIRE(mapping.size() == 1);
    REQUIRE(mapping[0].bucket_off_ == offset);
    REQUIRE(mapping[0].blob_size_ == total_size);
    REQUIRE(mapping[0].blob_off_ == offset);
    int status = fclose(fp);
    REQUIRE(status == 0);
  }
  SECTION("Map a one big request") {
    auto mapper = MapperFactory().Get(kMapperType);
    size_t total_size = TEST_INFO->request_size_ * TEST_INFO->num_iterations_;
    FILE* fp = fopen(TEST_INFO->new_file_.hermes_.c_str(), "w+");
    REQUIRE(fp != nullptr);
    size_t offset = 0;
    BlobPlacements mapping;
    mapper->map(offset, total_size, kPageSize, mapping);
    REQUIRE(mapping.size() == ceil((double)total_size / kPageSize));
    for (const auto& item : mapping) {
      size_t mapped_size =
          total_size - offset > kPageSize ? kPageSize : total_size - offset;
      REQUIRE(item.bucket_off_ == offset);
      REQUIRE(item.blob_size_ == mapped_size);
      REQUIRE(item.blob_off_ == offset % kPageSize);
      offset += mapped_size;
    }
    int status = fclose(fp);
    REQUIRE(status == 0);
  }
  SECTION("Map a one large unaligned request") {
    auto mapper = MapperFactory().Get(kMapperType);
    size_t total_size = TEST_INFO->request_size_ * TEST_INFO->num_iterations_;
    FILE* fp = fopen(TEST_INFO->new_file_.hermes_.c_str(), "w+");
    REQUIRE(fp != nullptr);
    size_t offset = 1;
    BlobPlacements mapping;
    mapper->map(offset, total_size, kPageSize, mapping);
    bool has_rem = (total_size + offset) % kPageSize != 0;
    if (has_rem) {
      REQUIRE(mapping.size() == ceil((double)total_size / kPageSize) + 1);
    } else {
      REQUIRE(mapping.size() == ceil((double)total_size / kPageSize));
    }

    size_t i = 0;
    size_t current_offset = offset;
    for (const auto& item : mapping) {
      size_t mapped_size = 0;
      if (i == 0) {
        mapped_size = kPageSize - offset;
      } else if (i == mapping.size() - 1) {
        mapped_size = offset;
      } else {
        mapped_size = kPageSize;
      }
      REQUIRE(item.bucket_off_ == current_offset);
      REQUIRE(item.blob_size_ == mapped_size);
      REQUIRE(item.blob_off_ == current_offset % kPageSize);
      current_offset += mapped_size;
      i++;
    }
    int status = fclose(fp);
    REQUIRE(status == 0);
  }
  SECTION("Map a one small unaligned request") {
    auto mapper = MapperFactory().Get(kMapperType);
    size_t total_size = TEST_INFO->request_size_;
    FILE* fp = fopen(TEST_INFO->new_file_.hermes_.c_str(), "w+");
    REQUIRE(fp != nullptr);
    size_t offset = 1;
    REQUIRE(kPageSize > total_size + offset);
    BlobPlacements mapping;
    mapper->map(offset, total_size, kPageSize, mapping);
    REQUIRE(mapping.size() == 1);
    REQUIRE(mapping[0].bucket_off_ == offset);
    REQUIRE(mapping[0].blob_size_ == total_size);
    REQUIRE(mapping[0].blob_off_ == 1);
    int status = fclose(fp);
    REQUIRE(status == 0);
  }
  TEST_INFO->Posttest();
}
