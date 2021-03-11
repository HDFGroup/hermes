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

#include "bucket.h"
#include "hermes.h"
#include "vbucket.h"
#include "test_utils.h"

namespace fs = std::experimental::filesystem;

struct Arguments {
  std::string filename = "test.dat";
  std::string directory = "/tmp";
  std::string config = "./hermes.conf";
  size_t request_size = 16 * 1024;
  size_t iterations = 1024;
};

struct Info {
  int rank;
  int comm_size;
  hermes::api::Blob write_blob;
  const size_t FILE_PAGE = 16 * 1024;
};

Arguments args;
Info info;

cl::Parser define_options() {
  return cl::Opt(args.filename, "filename")["-f"]["--filename"](
             "Filename used for performing I/O") |
         cl::Opt(args.directory, "dir")["-d"]["--directory"](
             "Directory used for performing I/O") |
         cl::Opt(args.config,
                 "config")["-c"]["--config"]("Configuration file for hermes") |
         cl::Opt(args.request_size, "request_size")["-s"]["--request_size"](
             "Request size for each operation") |
         cl::Opt(args.iterations,
                 "iterations")["-z"]["--iterations"]("Number of iterations");
}

int init() {
  MPI_Comm_rank(MPI_COMM_WORLD, &info.rank);
  MPI_Comm_size(MPI_COMM_WORLD, &info.comm_size);
  return 0;
}

int finalize() { return 0; }

std::string gen_random(const int len) {
  std::string tmp_s;
  static const char alphanum[] =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";

  srand((unsigned)time(NULL) * getpid());

  tmp_s.reserve(len);

  for (int i = 0; i < len; ++i)
    tmp_s += alphanum[rand() % (sizeof(alphanum) - 1)];

  return tmp_s;
}

int pretest() {
  args.filename += "_" + std::to_string(getpid());
  auto str = gen_random(args.request_size);
  info.write_blob = hermes::api::Blob(str.begin(), str.end());
  return 0;
}
int posttest() { return 0; }

TEST_CASE("CustomTrait",
          "[module=trait][type=meta][trait=FileBacked]"
          "[process=" +
              std::to_string(info.comm_size) + "]") {
  pretest();
  // REQUIRE(info.comm_size > 1);
  std::shared_ptr<hermes::api::Hermes> hermes_app =
      hermes::api::InitHermes(args.config.c_str(), true);
  hermes::api::Context ctx;
  fs::path fullpath = args.directory;
  fullpath /= args.filename;
  std::string fullpath_str = fullpath.string();
  SECTION("Basic") {
    hermes::api::Bucket file_bucket(args.filename, hermes_app, ctx);
    hermes::api::VBucket file_vbucket(args.filename, hermes_app, true, ctx);
    auto offset_map = std::unordered_map<std::string, hermes::u64>();
    auto blob_cmp = [](std::string a, std::string b) {
      return std::stol(a) < std::stol(b);
    };
    auto blob_names = std::set<std::string, decltype(blob_cmp)>(blob_cmp);
    auto check_write = hermes::api::Blob();
    for (size_t i = 0; i < args.iterations; ++i) {
      hermes::api::Status status =
        file_bucket.Put(std::to_string(i), info.write_blob, ctx);
      Assert(status.Succeeded());
      blob_names.insert(std::to_string(i));
      check_write.insert(check_write.end(), info.write_blob.begin(),
                         info.write_blob.end());
    }
    for (const auto& blob_name : blob_names) {
      file_vbucket.Link(blob_name, args.filename, ctx);
      offset_map.emplace(blob_name, std::stol(blob_name) * info.FILE_PAGE);
    }
    auto trait = hermes::api::FileMappingTrait(fullpath_str, offset_map,
                                               nullptr, NULL, NULL);
    file_vbucket.Attach(&trait, ctx);
    file_vbucket.Delete(ctx);
    file_bucket.Destroy(ctx);
    REQUIRE(fs::exists(fullpath_str));
    REQUIRE(fs::file_size(fullpath_str) == args.iterations * args.request_size);
    auto read_blob =
        hermes::api::Blob(args.iterations * args.request_size, '0');
    FILE* fh = fopen(fullpath_str.c_str(), "r+");
    REQUIRE(fh != nullptr);
    auto read_size =
        fread(read_blob.data(), args.iterations * args.request_size, 1, fh);
    REQUIRE(read_size == 1);
    bool is_same = read_blob == check_write;
    REQUIRE(is_same);
    auto status = fclose(fh);
    REQUIRE(status == 0);
  }
  hermes_app->Finalize(true);
  posttest();
}
