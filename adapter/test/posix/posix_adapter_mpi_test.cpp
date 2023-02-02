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
#include <stdarg.h>
#include <unistd.h>

#include <experimental/filesystem>
#include <iostream>

#include "catch_config.h"

#ifndef O_TMPFILE
#define O_TMPFILE 0
#endif

#include "adapter_test_utils.h"

#if HERMES_INTERCEPT == 1
#include "posix/real_api.h"
#endif

namespace stdfs = std::experimental::filesystem;

namespace hermes::adapter::fs::test {
struct Arguments {
  std::string filename = "test.dat";
  std::string directory = "/tmp";
  size_t request_size = 65536;
};
struct Info {
  bool debug = false;
  bool supports_tmpfile;
  int rank = 0;
  int comm_size = 1;
  std::vector<char> write_data;
  std::vector<char> read_data;
  std::string new_file;
  std::string existing_file;
  std::string shared_new_file;
  std::string existing_shared_file;
  std::string new_file_cmp;
  std::string existing_file_cmp;
  std::string existing_shared_file_cmp;
  std::string shared_new_file_cmp;
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
}  // namespace hermes::adapter::fs::test

hermes::adapter::fs::test::Arguments args;
hermes::adapter::fs::test::Info info;
std::vector<char> gen_random(const int len) {
  std::vector<char> tmp_s(len);
  static const char alphanum[] =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";

  srand((unsigned)time(NULL) * getpid());

  tmp_s.reserve(len);

  for (int i = 0; i < len; ++i)
    tmp_s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];

  return tmp_s;
}

int init(int* argc, char*** argv) {
  MPI_Init(argc, argv);
  info.write_data = gen_random(args.request_size);
  info.read_data = std::vector<char>(args.request_size, 'r');
  info.supports_tmpfile = FilesystemSupportsTmpfile();
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

int pretest() {
  REQUIRE(info.comm_size > 1);
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
      fullpath.string() + "_shared_ext_" + std::to_string(info.comm_size);
  info.existing_shared_file_cmp =
      fullpath.string() + "_shared_ext_cmp_" + std::to_string(info.comm_size);
  info.shared_new_file =
      fullpath.string() + "_shared_new_" + std::to_string(info.comm_size);
  info.shared_new_file_cmp =
      fullpath.string() + "_shared_new_cmp_" + std::to_string(info.comm_size);
  if (stdfs::exists(info.new_file)) stdfs::remove(info.new_file);
  if (stdfs::exists(info.existing_file)) stdfs::remove(info.existing_file);
  if (stdfs::exists(info.new_file_cmp)) stdfs::remove(info.new_file_cmp);
  if (stdfs::exists(info.existing_file_cmp))
    stdfs::remove(info.existing_file_cmp);
  if (stdfs::exists(info.existing_shared_file))
    stdfs::remove(info.existing_shared_file);
  if (stdfs::exists(info.existing_shared_file_cmp))
    stdfs::remove(info.existing_shared_file_cmp);
  if (stdfs::exists(info.shared_new_file)) stdfs::remove(info.shared_new_file);
  if (stdfs::exists(info.shared_new_file_cmp))
    stdfs::remove(info.shared_new_file_cmp);
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
  INTERCEPTOR_LIST->hermes_flush_exclusion.insert(info.existing_file_cmp);
  INTERCEPTOR_LIST->hermes_flush_exclusion.insert(info.new_file_cmp);
  INTERCEPTOR_LIST->hermes_flush_exclusion.insert(
      info.existing_shared_file_cmp);
#endif
  return 0;
}

int posttest(bool compare_data = true) {
#if HERMES_INTERCEPT == 1
  INTERCEPTOR_LIST->hermes_flush_exclusion.insert(info.existing_file);
  INTERCEPTOR_LIST->hermes_flush_exclusion.insert(info.new_file);
  INTERCEPTOR_LIST->hermes_flush_exclusion.insert(info.existing_shared_file);
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

#if HERMES_INTERCEPT == 1
  INTERCEPTOR_LIST->hermes_flush_exclusion.erase(info.existing_file_cmp);
  INTERCEPTOR_LIST->hermes_flush_exclusion.erase(info.new_file_cmp);
  INTERCEPTOR_LIST->hermes_flush_exclusion.erase(info.new_file);
  INTERCEPTOR_LIST->hermes_flush_exclusion.erase(info.existing_file);
  INTERCEPTOR_LIST->hermes_flush_exclusion.erase(info.existing_shared_file);
  INTERCEPTOR_LIST->hermes_flush_exclusion.erase(info.existing_shared_file_cmp);
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
int fh_orig;
int fh_cmp;
int status_orig;
size_t size_read_orig;
size_t size_written_orig;

void test_open(const char* path, int flags, ...) {
  int mode = 0;
  if (flags & O_CREAT || flags & O_TMPFILE) {
    va_list arg;
    va_start(arg, flags);
    mode = va_arg(arg, int);
    va_end(arg);
  }
  std::string cmp_path;
  if (strcmp(path, info.new_file.c_str()) == 0) {
    cmp_path = info.new_file_cmp;
  } else if (strcmp(path, info.shared_new_file.c_str()) == 0) {
    cmp_path = info.shared_new_file_cmp;
  } else if (strcmp(path, "/tmp") == 0) {
    cmp_path = "/tmp";
  } else {
    cmp_path = info.existing_file_cmp;
  }
  if (flags & O_CREAT || flags & O_TMPFILE) {
    fh_orig = open(path, flags, mode);
    fh_cmp = open(cmp_path.c_str(), flags, mode);
  } else {
    fh_orig = open(path, flags);
    fh_cmp = open(cmp_path.c_str(), flags);
  }
  bool is_same =
      (fh_cmp != -1 && fh_orig != -1) || (fh_cmp == -1 && fh_orig == -1);
  REQUIRE(is_same);
}
void test_close() {
  status_orig = close(fh_orig);
  int status = close(fh_cmp);
  REQUIRE(status == status_orig);
}
void test_write(const void* ptr, size_t size) {
  size_written_orig = write(fh_orig, ptr, size);
  size_t size_written = write(fh_cmp, ptr, size);
  REQUIRE(size_written == size_written_orig);
}
void test_read(char* ptr, size_t size) {
  size_read_orig = read(fh_orig, ptr, size);
  std::vector<unsigned char> read_data(size, 'r');
  size_t size_read = read(fh_cmp, read_data.data(), size);
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
void test_seek(long offset, int whence) {
  status_orig = lseek(fh_orig, offset, whence);
  int status = lseek(fh_cmp, offset, whence);
  REQUIRE(status == status_orig);
}
}  // namespace test

#include "posix_adapter_basic_test.cpp"
#include "posix_adapter_rs_test.cpp"
// TODO(chogan): Disabling until issue #302 is fixed
// #include "posix_adapter_shared_test.cpp"
