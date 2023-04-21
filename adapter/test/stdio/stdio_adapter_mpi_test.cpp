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

#include <adapter_test_utils.h>
#include <catch_config.h>
#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>

#include <filesystem>
#include <iostream>
#if HERMES_INTERCEPT == 1
#include "adapter/stdio/stdio_api.h"
#include "adapter/stdio/stdio_fs_api.h"
#endif

namespace stdfs = std::filesystem;

namespace hermes::adapter::stdio::test {
struct Arguments {
  std::string filename = "test.dat";
  std::string directory = "/tmp/test_hermes";
  size_t request_size = 65536;
};
struct Info {
  bool debug = false;
  int rank = 0;
  int comm_size = 1;
  std::string write_data;
  std::string read_data;
  std::string new_file;
  std::string existing_file;
  std::string existing_shared_file;
  std::string new_file_cmp;
  std::string existing_file_cmp;
  std::string existing_shared_file_cmp;
  size_t num_iterations = 64;
  unsigned int offset_seed = 1;
  unsigned int rs_seed = 1;
  unsigned int temporal_interval_seed = 1;
  size_t total_size;
  size_t stride_size = 4 * 1024;
  unsigned int temporal_interval_ms = 5;
  size_t small_min = 1, small_max = 4 * 1024;
  size_t medium_min = 4 * 1024 + 1, medium_max = 512 * 1024;
  size_t large_min = 512 * 1024 + 1, large_max = 3 * 1024 * 1024;
};
}  // namespace hermes::adapter::stdio::test

hermes::adapter::stdio::test::Arguments args;
hermes::adapter::stdio::test::Info info;

int init(int* argc, char*** argv) {
#if HERMES_INTERCEPT == 1
  setenv("HERMES_FLUSH_MODE", "kSync", 1);
  HERMES->client_config_.flushing_mode_ = hermes::FlushingMode::kSync;
#endif
  MPI_Init(argc, argv);
  info.write_data = GenRandom(args.request_size);
  info.read_data = std::string(args.request_size, 'r');
  MPI_Comm_rank(MPI_COMM_WORLD, &info.rank);
  MPI_Comm_size(MPI_COMM_WORLD, &info.comm_size);
  if (info.debug && info.rank == 0) {
    printf("%d ready for attach\n", info.comm_size);
    fflush(stdout);
    sleep(30);
  }
  MPI_Barrier(MPI_COMM_WORLD);
  return 0;
}
int finalize() {
  MPI_Finalize();
  return 0;
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
  } else if (strcmp(path, info.existing_file.c_str()) == 0) {
    cmp_path = info.existing_file_cmp;
  } else {
    cmp_path = info.existing_shared_file_cmp;
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
      if (read_data[i] != ptr[i]) unmatching_chars++;
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

int pretest() {
  stdfs::path fullpath = args.directory;
  fullpath /= args.filename;
  info.new_file = fullpath.string() + "_new_" + std::to_string(info.rank) +
                  "_of_" + std::to_string(info.comm_size) + "_" +
                  std::to_string(getpid());
  info.existing_file = fullpath.string() + "_ext_" + std::to_string(info.rank) +
                       "_of_" + std::to_string(info.comm_size) + "_" +
                       std::to_string(getpid());
  info.new_file_cmp =
      fullpath.string() + "_new_cmp_" + std::to_string(info.rank) + "_of_" +
      std::to_string(info.comm_size) + "_" + std::to_string(getpid());
  info.existing_file_cmp =
      fullpath.string() + "_ext_cmp_" + std::to_string(info.rank) + "_of_" +
      std::to_string(info.comm_size) + "_" + std::to_string(getpid());
  info.existing_shared_file =
      fullpath.string() + "_ext_" + std::to_string(info.comm_size);
  info.existing_shared_file_cmp =
      fullpath.string() + "_ext_cmp_" + std::to_string(info.comm_size);
  if (stdfs::exists(info.new_file)) stdfs::remove(info.new_file);
  if (stdfs::exists(info.existing_file)) stdfs::remove(info.existing_file);
  if (stdfs::exists(info.existing_file)) stdfs::remove(info.existing_file);
  if (stdfs::exists(info.existing_file_cmp))
    stdfs::remove(info.existing_file_cmp);
  if (stdfs::exists(info.existing_shared_file))
    stdfs::remove(info.existing_shared_file);
  if (stdfs::exists(info.existing_shared_file_cmp))
    stdfs::remove(info.existing_shared_file_cmp);
  stdfs::path temp_fullpath = "/tmp";
  temp_fullpath /= args.filename;
  std::string temp_ext_file =
      temp_fullpath.string() + "_temp_" + std::to_string(info.rank) + "_of_" +
      std::to_string(info.comm_size) + "_" + std::to_string(getpid());
  if (stdfs::exists(temp_ext_file)) stdfs::remove(temp_ext_file);
  if (!stdfs::exists(temp_ext_file)) {
    std::string cmd = "{ tr -dc '[:alnum:]' < /dev/urandom | head -c " +
                      std::to_string(args.request_size * info.num_iterations) +
                      "; } > " + temp_ext_file + " 2> /dev/null";
    int status = system(cmd.c_str());
    REQUIRE(status != -1);
    REQUIRE(stdfs::file_size(temp_ext_file) ==
            args.request_size * info.num_iterations);
    info.total_size = stdfs::file_size(temp_ext_file);
  }
  if (info.rank == 0 && !stdfs::exists(info.existing_shared_file)) {
    std::string cmd = "cp " + temp_ext_file + " " + info.existing_shared_file;
    int status = system(cmd.c_str());
    REQUIRE(status != -1);
    REQUIRE(stdfs::file_size(info.existing_shared_file) ==
            args.request_size * info.num_iterations);
  }
  if (info.rank == 0 && !stdfs::exists(info.existing_shared_file_cmp)) {
    std::string cmd =
        "cp " + temp_ext_file + " " + info.existing_shared_file_cmp;
    int status = system(cmd.c_str());
    REQUIRE(status != -1);
    REQUIRE(stdfs::file_size(info.existing_shared_file_cmp) ==
            args.request_size * info.num_iterations);
  }
  if (!stdfs::exists(info.existing_file)) {
    std::string cmd = "cp " + temp_ext_file + " " + info.existing_file;
    int status = system(cmd.c_str());
    REQUIRE(status != -1);
    REQUIRE(stdfs::file_size(info.existing_file) ==
            args.request_size * info.num_iterations);
    info.total_size = stdfs::file_size(info.existing_file);
  }
  if (!stdfs::exists(info.existing_file_cmp)) {
    std::string cmd = "cp " + info.existing_file + " " + info.existing_file_cmp;
    int status = system(cmd.c_str());
    REQUIRE(status != -1);
    REQUIRE(stdfs::file_size(info.existing_file_cmp) ==
            args.request_size * info.num_iterations);
  }
  if (stdfs::exists(temp_ext_file)) stdfs::remove(temp_ext_file);
  REQUIRE(info.total_size > 0);
  MPI_Barrier(MPI_COMM_WORLD);
#if HERMES_INTERCEPT == 1
  HERMES->client_config_.SetAdapterPathTracking(info.existing_file_cmp, false);
  HERMES->client_config_.SetAdapterPathTracking(info.new_file_cmp, false);
  HERMES->client_config_.SetAdapterPathTracking(
      info.existing_shared_file_cmp, false);
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
  HERMES->client_config_.SetAdapterPathTracking(info.existing_file, false);
  HERMES->client_config_.SetAdapterPathTracking(info.new_file, false);
  HERMES->client_config_.SetAdapterPathTracking(
      info.existing_shared_file, false);
#endif
  if (compare_data && stdfs::exists(info.new_file) &&
      stdfs::exists(info.new_file_cmp)) {
    size_t size = stdfs::file_size(info.new_file);
    REQUIRE(size == stdfs::file_size(info.new_file_cmp));
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
  if (compare_data && stdfs::exists(info.existing_file) &&
      stdfs::exists(info.existing_file_cmp)) {
    size_t size = stdfs::file_size(info.existing_file);
    if (size != stdfs::file_size(info.existing_file_cmp)) sleep(1);
    REQUIRE(size == stdfs::file_size(info.existing_file_cmp));
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
  if (compare_data && stdfs::exists(info.existing_shared_file) &&
      stdfs::exists(info.existing_shared_file_cmp)) {
    size_t size = stdfs::file_size(info.existing_shared_file);
    if (size != stdfs::file_size(info.existing_shared_file_cmp)) sleep(1);
    REQUIRE(size == stdfs::file_size(info.existing_shared_file_cmp));
    if (size > 0) {
      std::vector<unsigned char> d1(size, '0');
      std::vector<unsigned char> d2(size, '1');

      FILE* fh1 = fopen(info.existing_shared_file.c_str(), "r");
      REQUIRE(fh1 != nullptr);
      size_t read_d1 = fread(d1.data(), size, sizeof(unsigned char), fh1);
      REQUIRE(read_d1 == sizeof(unsigned char));
      int status = fclose(fh1);
      REQUIRE(status == 0);

      FILE* fh2 = fopen(info.existing_shared_file_cmp.c_str(), "r");
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
  if (stdfs::exists(info.new_file)) stdfs::remove(info.new_file);
  if (stdfs::exists(info.existing_file)) stdfs::remove(info.existing_file);
  if (stdfs::exists(info.new_file_cmp)) stdfs::remove(info.new_file_cmp);
  if (stdfs::exists(info.existing_file_cmp))
    stdfs::remove(info.existing_file_cmp);
  MPI_Barrier(MPI_COMM_WORLD);
  if (info.rank == 0) {
    if (stdfs::exists(info.existing_shared_file))
      stdfs::remove(info.existing_shared_file);
    if (stdfs::exists(info.existing_shared_file_cmp))
      stdfs::remove(info.existing_shared_file_cmp);
  }
  Clear();

#if HERMES_INTERCEPT == 1
  HERMES->client_config_.SetAdapterPathTracking(info.existing_file_cmp, true);
  HERMES->client_config_.SetAdapterPathTracking(info.new_file_cmp, true);
  HERMES->client_config_.SetAdapterPathTracking(info.new_file, true);
  HERMES->client_config_.SetAdapterPathTracking(info.existing_file, true);
  HERMES->client_config_.SetAdapterPathTracking(
      info.existing_shared_file, true);
  HERMES->client_config_.SetAdapterPathTracking(
      info.existing_shared_file_cmp, true);
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

#include "stdio_adapter_basic_test.cpp"
#include "stdio_adapter_func_test.cpp"
#include "stdio_adapter_rs_test.cpp"
// #include "stdio_adapter_shared_test.cpp"
