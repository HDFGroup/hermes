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

#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>

#include <filesystem>
#include <iostream>

#include "adapter_test_utils.h"
#include "catch_config.h"
#if HERMES_INTERCEPT == 1
#include "hermes_adapters/stdio/stdio_api.h"
#include "hermes_adapters/stdio/stdio_fs_api.h"
#endif

namespace stdfs = std::filesystem;

namespace hermes::adapter::stdio::test {
struct Arguments {
  std::string filename = "test.dat";
  std::string directory = "/tmp/test_hermes";
  size_t request_size = 65536;
};
struct Info {
  int rank = 0;
  int comm_size = 1;
  std::string write_data;
  std::string read_data;
  std::string new_file;
  std::string existing_file;
  std::string new_file_cmp;
  std::string existing_file_cmp;
  size_t num_iterations = 64;
  unsigned int offset_seed = 1;
  unsigned int rs_seed = 1;
  unsigned int temporal_interval_seed = 5;
  size_t total_size;
  size_t stride_size = 512;
  unsigned int temporal_interval_ms = 1;
  size_t small_min = 1, small_max = 4 * 1024;
  size_t medium_min = 4 * 1024 + 1, medium_max = 256 * 1024;
  size_t large_min = 256 * 1024 + 1, large_max = 3 * 1024 * 1024;
};
}  // namespace hermes::adapter::stdio::test
hermes::adapter::stdio::test::Arguments args;
hermes::adapter::stdio::test::Info info;

int init(int* argc, char*** argv) {
#if HERMES_INTERCEPT == 1
  setenv("HERMES_FLUSH_MODE", "kSync", 1);
  HERMES_CLIENT_CONF.flushing_mode_ = hermes::FlushingMode::kSync;
#endif
  MPI_Init(argc, argv);
  TEST_INFO->write_data_ = GenRandom(TEST_INFO->request_size_);
  TEST_INFO->read_data_ = std::string(TEST_INFO->request_size_, 'r');
  return 0;
}
int finalize() {
  MPI_Finalize();
  return 0;
}

int pretest() {
  stdfs::path fullpath = args.directory;
  fullpath /= args.filename;
  TEST_INFO->new_file_ = fullpath.string() + "_new_" + std::to_string(getpid());
  TEST_INFO->existing_file = fullpath.string() + "_ext_" + std::to_string(getpid());
  TEST_INFO->new_file_cmp =
      fullpath.string() + "_new_cmp" + "_" + std::to_string(getpid());
  TEST_INFO->existing_file_cmp =
      fullpath.string() + "_ext_cmp" + "_" + std::to_string(getpid());
  if (stdfs::exists(TEST_INFO->new_file_)) stdfs::remove(TEST_INFO->new_file_);
  if (stdfs::exists(TEST_INFO->new_file_cmp)) stdfs::remove(TEST_INFO->new_file_cmp);
  if (stdfs::exists(TEST_INFO->existing_file)) stdfs::remove(TEST_INFO->existing_file);
  if (stdfs::exists(TEST_INFO->existing_file_cmp))
    stdfs::remove(TEST_INFO->existing_file_cmp);
  if (!stdfs::exists(TEST_INFO->existing_file)) {
    std::string cmd = "{ tr -dc '[:alnum:]' < /dev/urandom | head -c " +
                      std::to_string(TEST_INFO->request_size_ * TEST_INFO->num_iterations_) +
                      "; } > " + TEST_INFO->existing_file + " 2> /dev/null";
    int status = system(cmd.c_str());
    REQUIRE(status != -1);
    REQUIRE(stdfs::file_size(TEST_INFO->existing_file) ==
            TEST_INFO->request_size_ * TEST_INFO->num_iterations_);
    TEST_INFO->total_size_ = stdfs::file_size(TEST_INFO->existing_file);
  }
  if (!stdfs::exists(TEST_INFO->existing_file_cmp)) {
    std::string cmd = "cp " + TEST_INFO->existing_file + " " + TEST_INFO->existing_file_cmp;
    int status = system(cmd.c_str());
    REQUIRE(status != -1);
    REQUIRE(stdfs::file_size(TEST_INFO->existing_file_cmp) ==
            TEST_INFO->request_size_ * TEST_INFO->num_iterations_);
  }
  REQUIRE(TEST_INFO->total_size_ > 0);
#if HERMES_INTERCEPT == 1
  HERMES_CLIENT_CONF.SetAdapterPathTracking(TEST_INFO->existing_file_cmp, false);
  HERMES_CLIENT_CONF.SetAdapterPathTracking(TEST_INFO->new_file_cmp, false);
#endif
  return 0;
}

void Clear() {
#if HERMES_INTERCEPT == 1
  HERMES->Clear();
#endif
}

int posttest(bool compare_data = true) {
#if HERMES_INTERCEPT == 1
  HERMES_CLIENT_CONF.SetAdapterPathTracking(TEST_INFO->existing_file, false);
  HERMES_CLIENT_CONF.SetAdapterPathTracking(TEST_INFO->new_file_, false);
#endif
  if (compare_data && stdfs::exists(TEST_INFO->new_file_) &&
      stdfs::exists(TEST_INFO->new_file_cmp)) {
    size_t size = stdfs::file_size(TEST_INFO->new_file_);
    REQUIRE(size == stdfs::file_size(TEST_INFO->new_file_cmp));
    if (size > 0) {
      std::vector<unsigned char> d1(size, '0');
      std::vector<unsigned char> d2(size, '1');

      FILE* fh1 = fopen(TEST_INFO->new_file_.hermes_.c_str(), "r");
      REQUIRE(fh1 != nullptr);
      size_t read_d1 = fread(d1.data(), size, sizeof(unsigned char), fh1);
      REQUIRE(read_d1 == sizeof(unsigned char));
      int status = fclose(fh1);
      REQUIRE(status == 0);

      FILE* fh2 = fopen(TEST_INFO->new_file_cmp.c_str(), "r");
      REQUIRE(fh2 != nullptr);
      size_t read_d2 = fread(d2.data(), size, sizeof(unsigned char), fh2);
      REQUIRE(read_d2 == sizeof(unsigned char));
      status = fclose(fh2);
      REQUIRE(status == 0);

      size_t char_mismatch = 0;
      for (size_t pos = 0; pos < size; ++pos) {
        if (d1[pos] != d2[pos]) {
          char_mismatch = pos;
          break;
        }
      }
      REQUIRE(char_mismatch == 0);
    }
  }
  if (compare_data && stdfs::exists(TEST_INFO->existing_file) &&
      stdfs::exists(TEST_INFO->existing_file_cmp)) {
    size_t size = stdfs::file_size(TEST_INFO->existing_file);
    if (size != stdfs::file_size(TEST_INFO->existing_file_cmp)) sleep(1);
    REQUIRE(size == stdfs::file_size(TEST_INFO->existing_file_cmp));
    if (size > 0) {
      std::vector<unsigned char> d1(size, '0');
      std::vector<unsigned char> d2(size, '1');

      FILE* fh1 = fopen(TEST_INFO->existing_file_.hermes_.c_str(), "r");
      REQUIRE(fh1 != nullptr);
      size_t read_d1 = fread(d1.data(), size, sizeof(unsigned char), fh1);
      REQUIRE(read_d1 == sizeof(unsigned char));
      int status = fclose(fh1);
      REQUIRE(status == 0);

      FILE* fh2 = fopen(TEST_INFO->existing_file_cmp.c_str(), "r");
      REQUIRE(fh2 != nullptr);
      size_t read_d2 = fread(d2.data(), size, sizeof(unsigned char), fh2);
      REQUIRE(read_d2 == sizeof(unsigned char));
      status = fclose(fh2);
      REQUIRE(status == 0);
      size_t char_mismatch = 0;
      for (size_t pos = 0; pos < size; ++pos) {
        if (d1[pos] != d2[pos]) char_mismatch++;
      }
      REQUIRE(char_mismatch == 0);
    }
  }
  /* Clean up. */
  if (stdfs::exists(TEST_INFO->new_file_)) stdfs::remove(TEST_INFO->new_file_);
  if (stdfs::exists(TEST_INFO->existing_file)) stdfs::remove(TEST_INFO->existing_file);
  if (stdfs::exists(TEST_INFO->new_file_cmp)) stdfs::remove(TEST_INFO->new_file_cmp);
  if (stdfs::exists(TEST_INFO->existing_file_cmp))
    stdfs::remove(TEST_INFO->existing_file_cmp);
  Clear();

#if HERMES_INTERCEPT == 1
  HERMES_CLIENT_CONF.SetAdapterPathTracking(TEST_INFO->existing_file_cmp, true);
  HERMES_CLIENT_CONF.SetAdapterPathTracking(TEST_INFO->new_file_cmp, true);
  HERMES_CLIENT_CONF.SetAdapterPathTracking(TEST_INFO->new_file_, true);
  HERMES_CLIENT_CONF.SetAdapterPathTracking(TEST_INFO->existing_file, true);
#endif
  return 0;
}

cl::Parser define_options() {
  return cl::Opt(args.filename, "filename")["-f"]["--filename"](
             "Filename used for performing I/O") |
         cl::Opt(args.directory, "dir")["-d"]["--directory"](
             "Directory used for performing I/O") |
         cl::Opt(TEST_INFO->request_size_, "request_size")["-s"]["--request_size"](
             "Request size used for performing I/O");
}

namespace test {
FILE* fh_orig;
FILE* fh_cmp;
int status_orig;
size_t size_read_orig;
size_t size_written_orig;
void test_fopen(const char* path, const char* mode) {
  std::string cmp_path;
  if (strcmp(path, TEST_INFO->new_file_.c_str()) == 0) {
    cmp_path = TEST_INFO->new_file_cmp;
  } else {
    cmp_path = TEST_INFO->existing_file_cmp;
  }
  fh_orig = fopen(path, mode);
  fh_cmp = fopen(cmp_path.c_str(), mode);
  bool is_same = (fh_cmp != nullptr && fh_orig != nullptr) ||
                 (fh_cmp == nullptr && fh_orig == nullptr);
  REQUIRE(is_same);
}
void test_fclose() {
  status_orig = fclose(fh_orig);
  int status = fclose(fh_cmp);
  REQUIRE(status == status_orig);
}
void test_fwrite(const void* ptr, size_t size) {
  size_written_orig = fwrite(ptr, sizeof(char), size, fh_orig);
  size_t size_written = fwrite(ptr, sizeof(char), size, fh_cmp);
  REQUIRE(size_written == size_written_orig);
}
void test_fread(char* ptr, size_t size) {
  size_read_orig = fread(ptr, sizeof(char), size, fh_orig);
  std::vector<unsigned char> read_data(size, 'r');
  size_t size_read = fread(read_data.data(), sizeof(char), size, fh_cmp);
  REQUIRE(size_read == size_read_orig);
  if (size_read > 0) {
    size_t unmatching_chars = 0;
    for (size_t i = 0; i < size; ++i) {
      if (read_data[i] != ptr[i]) {
        unmatching_chars = i;
        break;
      }
    }
    REQUIRE(unmatching_chars == 0);
  }
}
void test_fseek(long offset, int whence) {
  status_orig = fseek(fh_orig, offset, whence);
  int status = fseek(fh_cmp, offset, whence);
  REQUIRE(status == status_orig);
}
}  // namespace test

TEST_CASE("BatchedWriteSequential",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=sequential][file=1]") {
  TEST_INFO->Pretest();
  SECTION("write to new file one big write") {
    TEST_INFO->test_fopen(TEST_INFO->new_file_.hermes_.c_str(), "w+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    auto write_size = TEST_INFO->request_size_ * (TEST_INFO->num_iterations_ + 1);
    TEST_INFO->write_data_ = GenRandom(write_size);
    TEST_INFO->test_fwrite(TEST_INFO->write_data_.data(), write_size);
    REQUIRE(TEST_INFO->size_written_orig_ == write_size);
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
    REQUIRE(stdfs::file_size(TEST_INFO->new_file_) == write_size);
  }
  SECTION("write to new file multiple write") {
    TEST_INFO->test_fopen(TEST_INFO->new_file_.hermes_.c_str(), "w+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    for (size_t i = 0; i <= TEST_INFO->num_iterations_; ++i) {
      TEST_INFO->test_fwrite(TEST_INFO->write_data_.data(), TEST_INFO->request_size_);
      REQUIRE(TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    }
    REQUIRE(TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
    REQUIRE(stdfs::file_size(TEST_INFO->new_file_) ==
            TEST_INFO->request_size_ * (TEST_INFO->num_iterations_ + 1));
  }
  TEST_INFO->Posttest();
}
