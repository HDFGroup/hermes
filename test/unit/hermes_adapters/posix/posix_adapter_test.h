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

#ifndef HERMES_TEST_UNIT_HERMES_ADAPTERS_POSIX_POSIX_ADAPTER_BASE_TEST_H_
#define HERMES_TEST_UNIT_HERMES_ADAPTERS_POSIX_POSIX_ADAPTER_BASE_TEST_H_

#include "binary_file_tests.h"

namespace hermes::adapter::fs::test {
template<bool WITH_MPI>
class PosixTest : public BinaryFileTests {
 public:
  FileInfo new_file_;
  FileInfo existing_file_;
  FileInfo shared_new_file_;
  FileInfo shared_existing_file_;
  FileInfo tmp_file_;
  unsigned int offset_seed_ = 1;
  unsigned int rs_seed_ = 1;
  unsigned int temporal_interval_seed_ = 5;
  size_t stride_size_ = 1024;
  unsigned int temporal_interval_ms_ = 1;
  size_t small_min_ = 1;
  size_t small_max_ = 4 * 1024;
  size_t medium_min_ = 4 * 1024 + 1;
  size_t medium_max_ = 256 * 1024;
  size_t large_min_ = 256 * 1024 + 1;
  size_t large_max_ = 3 * 1024 * 1024;

  int fh_orig_;
  int fh_cmp_;
  int status_orig_;
  size_t size_read_orig_;
  size_t size_written_orig_;
  bool is_scase_ = false;

 public:
  void RegisterFiles() override {
    RegisterPath("new", 0, new_file_);
    RegisterPath("ext", TEST_DO_CREATE, existing_file_);
    if constexpr(WITH_MPI) {
      RegisterPath("shared_new", TEST_FILE_SHARED, shared_new_file_);
      RegisterPath("shared_ext", TEST_DO_CREATE | TEST_FILE_SHARED, shared_existing_file_);
    }
    RegisterTmpPath(tmp_file_);
  }
  
  void test_open(FileInfo &info, int flags, ...) {
    int mode = 0;
    if (flags & O_CREAT || flags & O_TMPFILE) {
      va_list arg;
      va_start(arg, flags);
      mode = va_arg(arg, int);
      va_end(arg);
    }
    std::string cmp_path;
    if (flags & O_CREAT || flags & O_TMPFILE) {
      fh_orig_ = open(info.hermes_.c_str(), flags, mode);
      fh_cmp_ = open(info.cmp_.c_str(), flags, mode);
    } else {
      fh_orig_ = open(info.hermes_.c_str(), flags);
      fh_cmp_ = open(info.cmp_.c_str(), flags);
    }
    bool is_same =
        (fh_cmp_ != -1 && fh_orig_ != -1) || (fh_cmp_ == -1 && fh_orig_ == -1);
    REQUIRE(is_same);
  }

  void test_close() {
    status_orig_ = close(fh_orig_);
    int status = close(fh_cmp_);
    REQUIRE(status == status_orig_);
  }

  void test_write(const void* ptr, size_t size) {
    size_written_orig_ = write(fh_orig_, ptr, size);
    size_t size_written = write(fh_cmp_, ptr, size);
    REQUIRE(size_written == size_written_orig_);
  }

  void test_read(char* ptr, size_t size) {
    size_read_orig_ = read(fh_orig_, ptr, size);
    std::vector<unsigned char> read_data(size, 'r');
    size_t size_read = read(fh_cmp_, read_data.data(), size);
    REQUIRE(size_read == size_read_orig_);
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
    status_orig_ = lseek(fh_orig_, offset, whence);
    int status = lseek(fh_cmp_, offset, whence);
    REQUIRE(status == status_orig_);
  }
};

}  // namespace hermes::adapter::fs::test

#if defined(HERMES_MPI_TESTS)
#define TEST_INFO \
  hshm::EasySingleton<hermes::adapter::fs::test::PosixTest<HERMES_MPI_TESTS>>::GetInstance()
#else
#define TEST_INFO \
  hshm::EasySingleton<hermes::adapter::fs::test::PosixTest<false>>::GetInstance()
#endif

#endif  // HERMES_TEST_UNIT_HERMES_ADAPTERS_POSIX_POSIX_ADAPTER_BASE_TEST_H_
