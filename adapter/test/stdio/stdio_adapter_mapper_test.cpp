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

#include <experimental/filesystem>

#include "catch_config.h"
#include "constants.h"
#include "stdio/constants.h"
#include "stdio/datastructures.h"
#include "stdio/mapper/mapper_factory.h"
#include "stdio/metadata_manager.h"

using hermes::adapter::stdio::FileStruct;
using hermes::adapter::stdio::HermesStruct;
using hermes::adapter::stdio::MapperFactory;
using hermes::adapter::stdio::MetadataManager;

namespace fs = std::experimental::filesystem;

namespace hermes::adapter::stdio::test {
struct Arguments {
  std::string filename = "test.dat";
  std::string directory = "/tmp";
  size_t request_size = 65536;
  size_t num_iterations = 1024;
};
struct Info {
  int rank = 0;
  int comm_size = 1;
  std::string new_file;
  std::string existing_file;
  unsigned int offset_seed = 1;
  unsigned int rs_seed = 1;
  size_t total_size;
  size_t stride_size = 1024;
  size_t small_min = 1, small_max = 4 * 1024;
  size_t medium_min = 4 * 1024 + 1, medium_max = 256 * 1024;
  size_t large_min = 256 * 1024 + 1, large_max = 4 * 1024 * 1024;
};
}  // namespace hermes::adapter::stdio::test
hermes::adapter::stdio::test::Arguments args;
hermes::adapter::stdio::test::Info info;

int init(int* argc, char*** argv) {
  MPI_Init(argc, argv);

  return 0;
}
int finalize() {
  MPI_Finalize();

  return 0;
}

int pretest() {
  fs::path fullpath = args.directory;
  fullpath /= args.filename;
  info.new_file = fullpath.string() + "_new";
  info.existing_file = fullpath.string() + "_ext";
  if (fs::exists(info.new_file)) fs::remove(info.new_file);
  if (fs::exists(info.existing_file)) fs::remove(info.existing_file);
  if (!fs::exists(info.existing_file)) {
    std::string cmd = "dd if=/dev/zero of=" + info.existing_file +
                      " bs=1 count=0 seek=" +
                      std::to_string(args.request_size * args.num_iterations) +
                      " > /dev/null 2>&1";
    int status = system(cmd.c_str());
    REQUIRE(status != -1);
    REQUIRE(fs::file_size(info.existing_file) ==
            args.request_size * args.num_iterations);
    info.total_size = fs::file_size(info.existing_file);
  }
  REQUIRE(info.total_size > 0);
  return 0;
}

int posttest() {
  if (fs::exists(info.new_file)) fs::remove(info.new_file);
  if (fs::exists(info.existing_file)) fs::remove(info.existing_file);
  return 0;
}

cl::Parser define_options() {
  return cl::Opt(args.filename, "filename")["-f"]["--filename"](
             "Filename used for performing I/O") |
         cl::Opt(args.directory, "dir")["-d"]["--directory"](
             "Directory used for performing I/O") |
         cl::Opt(args.request_size, "request_size")["-s"]["--request_size"](
             "Request size used for performing I/O") |
         cl::Opt(args.num_iterations, "iterations")["-n"]["--iterations"](
             "Number of iterations of requests");
}

TEST_CASE("SingleWrite", "[process=" + std::to_string(info.comm_size) +
                             "]"
                             "[operation=single_write]"
                             "[request_size=type-fixed][repetition=1]"
                             "[pattern=sequential][file=1]") {
  pretest();
  SECTION("Map a one request") {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto mapper = MapperFactory().Get(kMapperType);
    size_t total_size = args.request_size;
    FILE* fp = fopen(info.new_file.c_str(), "w+");
    REQUIRE(fp != nullptr);
    size_t offset = 0;
    REQUIRE(kPageSize > total_size + offset);
    auto mapping =
        mapper->map(FileStruct(mdm->Convert(fp), offset, total_size));
    REQUIRE(mapping.size() == 1);
    REQUIRE(mapping[0].first.offset_ == offset);
    REQUIRE(mapping[0].first.size_ == total_size);
    REQUIRE(mapping[0].second.offset_ == offset);
    REQUIRE(mapping[0].second.size_ == total_size);

    int status = fclose(fp);
    REQUIRE(status == 0);
  }
  SECTION("Map a one big request") {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto mapper = MapperFactory().Get(kMapperType);
    size_t total_size = args.request_size * args.num_iterations;
    FILE* fp = fopen(info.new_file.c_str(), "w+");
    REQUIRE(fp != nullptr);
    size_t offset = 0;
    auto mapping =
        mapper->map(FileStruct(mdm->Convert(fp), offset, total_size));
    REQUIRE(mapping.size() == ceil((double)total_size / kPageSize));
    for (const auto& item : mapping) {
      size_t mapped_size =
          total_size - offset > kPageSize ? kPageSize : total_size - offset;
      REQUIRE(item.first.offset_ == offset);
      REQUIRE(item.first.size_ == mapped_size);
      REQUIRE(item.second.offset_ == offset % kPageSize);
      REQUIRE(item.second.size_ == mapped_size);
      offset += mapped_size;
    }
    int status = fclose(fp);
    REQUIRE(status == 0);
  }
  SECTION("Map a one large unaligned request") {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto mapper = MapperFactory().Get(kMapperType);
    size_t total_size = args.request_size * args.num_iterations;
    FILE* fp = fopen(info.new_file.c_str(), "w+");
    REQUIRE(fp != nullptr);
    size_t offset = 1;
    auto mapping =
        mapper->map(FileStruct(mdm->Convert(fp), offset, total_size));
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
      REQUIRE(item.first.offset_ == current_offset);
      REQUIRE(item.first.size_ == mapped_size);
      REQUIRE(item.second.offset_ == current_offset % kPageSize);
      REQUIRE(item.second.size_ == mapped_size);
      current_offset += mapped_size;
      i++;
    }
    int status = fclose(fp);
    REQUIRE(status == 0);
  }
  SECTION("Map a one small unaligned request") {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto mapper = MapperFactory().Get(kMapperType);
    size_t total_size = args.request_size;
    FILE* fp = fopen(info.new_file.c_str(), "w+");
    REQUIRE(fp != nullptr);
    size_t offset = 1;
    REQUIRE(kPageSize > total_size + offset);
    auto mapping =
        mapper->map(FileStruct(mdm->Convert(fp), offset, total_size));
    REQUIRE(mapping.size() == 1);
    REQUIRE(mapping[0].first.offset_ == offset);
    REQUIRE(mapping[0].first.size_ == total_size);
    REQUIRE(mapping[0].second.offset_ == 1);
    REQUIRE(mapping[0].second.size_ == total_size);
    int status = fclose(fp);
    REQUIRE(status == 0);
  }
  posttest();
}
