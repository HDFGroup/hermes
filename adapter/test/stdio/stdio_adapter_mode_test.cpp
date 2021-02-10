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

#include <catch_config.h>
#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>

#include <experimental/filesystem>
#include <iostream>

#if HERMES_INTERCEPT == 1
#include <hermes/adapter/stdio.h>
#endif

namespace fs = std::experimental::filesystem;

namespace hermes::adapter::stdio::test {
struct Arguments {
  std::string filename = "test.dat";
  std::string directory = "/tmp";
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
std::string gen_random(const int len) {
  std::string tmp_s;
  static const char alphanum[] =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";

  srand(100);

  tmp_s.reserve(len);

  for (int i = 0; i < len; ++i)
    tmp_s += alphanum[rand() % (sizeof(alphanum) - 1)];

  return tmp_s;
}
int init(int* argc, char*** argv) {
  fs::path fullpath = args.directory;
  fullpath /= args.filename;
  info.new_file = fullpath.string() + "_new";
  info.existing_file = fullpath.string() + "_ext";
  info.new_file_cmp = fullpath.string() + "_new_cmp";
  info.existing_file_cmp = fullpath.string() + "_ext_cmp";
  char* set_path = getenv("SET_PATH");
  if (set_path && strcmp(set_path, "1") == 0) {
    auto paths = info.new_file + "," + info.existing_file;
    setenv(kAdapterModeInfo, paths.c_str(), 1);
  }
  MPI_Init(argc, argv);
  info.write_data = gen_random(args.request_size);
  info.read_data = std::string(args.request_size, 'r');
  return 0;
}
int finalize() {
  MPI_Finalize();
  return 0;
}

int pretest() {
  if (fs::exists(info.new_file)) fs::remove(info.new_file);
  if (fs::exists(info.new_file_cmp)) fs::remove(info.new_file_cmp);
  if (fs::exists(info.existing_file)) fs::remove(info.existing_file);
  if (fs::exists(info.existing_file_cmp)) fs::remove(info.existing_file_cmp);
  if (!fs::exists(info.existing_file)) {
    std::string cmd = "{ tr -dc '[:alnum:]' < /dev/urandom | head -c " +
                      std::to_string(args.request_size * info.num_iterations) +
                      "; } > " + info.existing_file + " 2> /dev/null";
    int status = system(cmd.c_str());
    REQUIRE(status != -1);
    REQUIRE(fs::file_size(info.existing_file) ==
            args.request_size * info.num_iterations);
    info.total_size = fs::file_size(info.existing_file);
  }
  if (!fs::exists(info.existing_file_cmp)) {
    std::string cmd = "cp " + info.existing_file + " " + info.existing_file_cmp;
    int status = system(cmd.c_str());
    REQUIRE(status != -1);
    REQUIRE(fs::file_size(info.existing_file_cmp) ==
            args.request_size * info.num_iterations);
  }
  REQUIRE(info.total_size > 0);
#if HERMES_INTERCEPT == 1
  INTERCEPTOR_LIST->hermes_flush_exclusion.insert(info.existing_file_cmp);
  INTERCEPTOR_LIST->hermes_flush_exclusion.insert(info.new_file_cmp);
#endif
  return 0;
}

int posttest(bool compare_data = true) {
#if HERMES_INTERCEPT == 1
  INTERCEPTOR_LIST->hermes_flush_exclusion.insert(info.existing_file);
  INTERCEPTOR_LIST->hermes_flush_exclusion.insert(info.new_file);
#endif
  if (compare_data && fs::exists(info.new_file) &&
      fs::exists(info.new_file_cmp)) {
    size_t size = fs::file_size(info.new_file);
    REQUIRE(size == fs::file_size(info.new_file_cmp));
    if (size > 0) {
      std::vector<unsigned char> d1(size, '0');
      std::vector<unsigned char> d2(size, '1');

      FILE* fh1 = fopen(info.new_file.c_str(), "r");
      REQUIRE(fh1 != nullptr);
      size_t read_d1 = fread(d1.data(), size, sizeof(unsigned char), fh1);
      REQUIRE(read_d1 == sizeof(unsigned char));
      int status = fclose(fh1);
      REQUIRE(status == 0);

      FILE* fh2 = fopen(info.new_file_cmp.c_str(), "r");
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
  if (compare_data && fs::exists(info.existing_file) &&
      fs::exists(info.existing_file_cmp)) {
    size_t size = fs::file_size(info.existing_file);
    if (size != fs::file_size(info.existing_file_cmp)) sleep(1);
    REQUIRE(size == fs::file_size(info.existing_file_cmp));
    if (size > 0) {
      std::vector<unsigned char> d1(size, '0');
      std::vector<unsigned char> d2(size, '1');

      FILE* fh1 = fopen(info.existing_file.c_str(), "r");
      REQUIRE(fh1 != nullptr);
      size_t read_d1 = fread(d1.data(), size, sizeof(unsigned char), fh1);
      REQUIRE(read_d1 == sizeof(unsigned char));
      int status = fclose(fh1);
      REQUIRE(status == 0);

      FILE* fh2 = fopen(info.existing_file_cmp.c_str(), "r");
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
  if (fs::exists(info.new_file)) fs::remove(info.new_file);
  if (fs::exists(info.existing_file)) fs::remove(info.existing_file);
  if (fs::exists(info.new_file_cmp)) fs::remove(info.new_file_cmp);
  if (fs::exists(info.existing_file_cmp)) fs::remove(info.existing_file_cmp);

#if HERMES_INTERCEPT == 1
  INTERCEPTOR_LIST->hermes_flush_exclusion.erase(info.existing_file_cmp);
  INTERCEPTOR_LIST->hermes_flush_exclusion.erase(info.new_file_cmp);
  INTERCEPTOR_LIST->hermes_flush_exclusion.erase(info.new_file);
  INTERCEPTOR_LIST->hermes_flush_exclusion.erase(info.existing_file);
#endif
  return 0;
}

cl::Parser define_options() {
  return cl::Opt(args.filename, "filename")["-f"]["--filename"](
             "Filename used for performing I/O") |
         cl::Opt(args.directory, "dir")["-d"]["--directory"](
             "Directory used for performing I/O") |
         cl::Opt(args.request_size, "request_size")["-s"]["--request_size"](
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
  if (strcmp(path, info.new_file.c_str()) == 0) {
    cmp_path = info.new_file_cmp;
  } else {
    cmp_path = info.existing_file_cmp;
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

TEST_CASE("BatchedWriteSequentialPersistent",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_write]"
              "[hermes_mode=persistent]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=sequential][file=1]") {
  char* adapter_mode = getenv(kAdapterMode);
  REQUIRE(adapter_mode != nullptr);
  bool is_same =
      strcmp(kAdapterDefaultMode, adapter_mode) == 0;
  REQUIRE(is_same);
  pretest();
  SECTION("write to new file always at end") {
    test::test_fopen(info.new_file.c_str(), "w+");
    REQUIRE(test::fh_orig != nullptr);

    for (size_t i = 0; i < info.num_iterations; ++i) {
      test::test_fwrite(info.write_data.c_str(), args.request_size);
      REQUIRE(test::size_written_orig == args.request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
    REQUIRE(fs::file_size(info.new_file) ==
            info.num_iterations * args.request_size);
  }
  posttest();
}

TEST_CASE("BatchedWriteSequentialBypass",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_write]"
              "[hermes_mode=bypass]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=sequential][file=1]") {
  char* adapter_mode = getenv(kAdapterMode);
  REQUIRE(adapter_mode != nullptr);
  bool is_same = strcmp(kAdapterBypassMode, adapter_mode) == 0;
  REQUIRE(is_same);
  pretest();
  SECTION("write to new file always at end") {
    test::test_fopen(info.new_file.c_str(), "w+");
    REQUIRE(test::fh_orig != nullptr);

    for (size_t i = 0; i < info.num_iterations; ++i) {
      test::test_fwrite(info.write_data.c_str(), args.request_size);
      REQUIRE(test::size_written_orig == args.request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
    REQUIRE(fs::file_size(info.new_file) ==
            info.num_iterations * args.request_size);
  }
  posttest();
}

TEST_CASE("BatchedWriteSequentialScratch",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_write]"
              "[hermes_mode=scratch]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=sequential][file=1]") {
  char* adapter_mode = getenv(kAdapterMode);
  REQUIRE(adapter_mode != nullptr);
  bool is_same = strcmp(kAdapterScratchMode, adapter_mode) == 0;
  REQUIRE(is_same);
  pretest();
  SECTION("write to new file always at end") {
    test::test_fopen(info.new_file.c_str(), "w+");
    REQUIRE(test::fh_orig != nullptr);

    for (size_t i = 0; i < info.num_iterations; ++i) {
      test::test_fwrite(info.write_data.c_str(), args.request_size);
      REQUIRE(test::size_written_orig == args.request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
    REQUIRE(fs::file_size(info.new_file) == 0);
  }
  posttest(false);
}
