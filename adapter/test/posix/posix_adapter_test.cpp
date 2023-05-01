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

#include <filesystem>
#include <iostream>
#include "hermes.h"
#include <stdio.h>

#include "catch_config.h"
#include "adapter/posix/posix_api.h"

#if HERMES_INTERCEPT == 1
#include "adapter/posix/posix_fs_api.h"
#endif

#include "adapter_test_utils.h"

namespace stdfs = std::filesystem;

namespace hermes::adapter::fs::test {
struct Arguments {
  std::string filename = "test.dat";
  std::string directory = "/tmp/test_hermes";
  size_t request_size = 65536;
};
struct Info {
  int rank = 0;
  int comm_size = 1;
  bool supports_tmpfile;
  std::vector<char> write_data;
  std::vector<char> read_data;
  std::string new_file;       // Tracked by Hermes
  std::string existing_file;  // Tracked by Hermes
  std::string new_file_cmp;   // NOT tracked by Hermes
  std::string existing_file_cmp;  // NOT tracekd by Hermes
  size_t num_iterations = 64;
  unsigned int offset_seed = 1;
  unsigned int rs_seed = 1;
  unsigned int temporal_interval_seed = 5;
  size_t total_size;    // The size of the EXISTING file
  size_t stride_size = 1024;
  unsigned int temporal_interval_ms = 1;
  size_t small_min = 1, small_max = 4 * 1024;
  size_t medium_min = 4 * 1024 + 1, medium_max = 256 * 1024;
  size_t large_min = 256 * 1024 + 1, large_max = 3 * 1024 * 1024;
};
}  // namespace hermes::adapter::fs::test
hermes::adapter::fs::test::Arguments args;
hermes::adapter::fs::test::Info info;
std::vector<char> gen_random(const int len) {
  auto tmp_s = std::vector<char>(len);
  static const char alphanum[] =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";

  srand(100);
  for (int i = 0; i < len; ++i)
    tmp_s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
  return tmp_s;
}

int init(int* argc, char*** argv) {
#if HERMES_INTERCEPT == 1
  setenv("HERMES_FLUSH_MODE", "kSync", 1);
  HERMES->client_config_.flushing_mode_ = hermes::FlushingMode::kSync;
#endif
  MPI_Init(argc, argv);
  info.write_data = gen_random(args.request_size);
  info.read_data = std::vector<char>(args.request_size, 'r');
  info.supports_tmpfile = FilesystemSupportsTmpfile();
  return 0;
}

int finalize() {
  MPI_Finalize();
  return 0;
}

void IgnoreAllFiles() {
#if HERMES_INTERCEPT == 1
  HERMES->client_config_.SetAdapterPathTracking(info.existing_file_cmp, false);
  HERMES->client_config_.SetAdapterPathTracking(info.new_file_cmp, false);
  HERMES->client_config_.SetAdapterPathTracking(info.new_file, false);
  HERMES->client_config_.SetAdapterPathTracking(info.existing_file, false);
#endif
}

void TrackFiles() {
#if HERMES_INTERCEPT == 1
  HERMES->client_config_.SetAdapterPathTracking(info.new_file, true);
  HERMES->client_config_.SetAdapterPathTracking(info.existing_file, true);
#endif
}

void RemoveFile(const std::string &path) {
  stdfs::remove(path);
  if (stdfs::exists(path)) {
    HELOG(kFatal, "Failed to remove: {}", path)
  }
}

void RemoveFiles() {
  RemoveFile(info.new_file);
  RemoveFile(info.new_file_cmp);
  RemoveFile(info.existing_file);
  RemoveFile(info.existing_file_cmp);
}

int pretest() {
  // Initialize path names
  stdfs::path fullpath = args.directory;
  fullpath /= args.filename;
  info.new_file = fullpath.string() + "_new_" + std::to_string(getpid());
  info.existing_file = fullpath.string() + "_ext_" + std::to_string(getpid());
  info.new_file_cmp =
      fullpath.string() + "_new_cmp" + "_" + std::to_string(getpid());
  info.existing_file_cmp =
      fullpath.string() + "_ext_cmp" + "_" + std::to_string(getpid());

  // Ignore all files
  IgnoreAllFiles();

  // Remove existing files from the FS
  RemoveFiles();

  // Create the file which is untracked by Hermes
  if (true) {
    std::string cmd = "{ tr -dc '[:alnum:]' < /dev/urandom | head -c " +
                      std::to_string(args.request_size * info.num_iterations) +
                      "; } > " + info.existing_file_cmp + " 2> /dev/null";
    int status = system(cmd.c_str());
    REQUIRE(status != -1);
    REQUIRE(stdfs::file_size(info.existing_file_cmp) ==
            args.request_size * info.num_iterations);
    info.total_size = stdfs::file_size(info.existing_file_cmp);
  }

  // Create the file that is being tracks by Hermes
  if (true) {
    std::string cmd = "cp " + info.existing_file_cmp + " " + info.existing_file;
    int status = system(cmd.c_str());
    REQUIRE(status != -1);
    auto check = stdfs::file_size(info.existing_file) ==
                 args.request_size * info.num_iterations;
    if (!check) {
      HELOG(kFatal, "File sizes weren't equivalent after copy")
    }
    REQUIRE(check);
  }
  REQUIRE(info.total_size > 0);

  // Begin tracking the Hermes files
  TrackFiles();
  return 0;
}

void Clear() {
#if HERMES_INTERCEPT == 1
  HERMES->Clear();
#endif
}

int posttest(bool compare_data = true) {
  IgnoreAllFiles();
  if (compare_data && stdfs::exists(info.new_file) &&
      stdfs::exists(info.new_file_cmp)) {
    // Verify the NEW file is the same in Hermes + the backend
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
      std::vector<unsigned char> d1(size, 'r');
      std::vector<unsigned char> d2(size, 'w');

      FILE* fh1 = fopen(info.existing_file.c_str(), "r");
      REQUIRE(fh1 != nullptr);
      size_t read_d1 = fread(d1.data(), sizeof(unsigned char), size, fh1);
      REQUIRE(read_d1 == size);
      int status = fclose(fh1);
      REQUIRE(status == 0);

      FILE* fh2 = fopen(info.existing_file_cmp.c_str(), "r");
      REQUIRE(fh2 != nullptr);
      size_t read_d2 = fread(d2.data(), sizeof(unsigned char), size, fh2);
      REQUIRE(read_d2 == size);
      status = fclose(fh2);
      REQUIRE(status == 0);
      size_t char_mismatch = 0;
      for (size_t pos = 0; pos < size; ++pos) {
        if (d1[pos] != d2[pos]) {
          char_mismatch = pos;
          break;
        }
      }
      if (char_mismatch != 0) {
        std::cout << "The files " <<  info.existing_file
                  << " and " << info.existing_file_cmp
                  << " had mismatched characters" << std::endl;
      }
      REQUIRE(char_mismatch == 0);
    }
  }
  /* Delete the files from both Hermes and the backend. */
  TrackFiles();
  RemoveFiles();
  Clear();
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
bool is_scase_ = false;
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
    if (unmatching_chars != 0) {
      std::cerr << "There were unmatching chars" << std::endl;
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
