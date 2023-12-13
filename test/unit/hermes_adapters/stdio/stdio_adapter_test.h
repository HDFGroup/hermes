//
// Created by lukemartinlogan on 11/4/23.
//

#ifndef HERMES_TEST_UNIT_HERMES_ADAPTERS_STDIO_STDIO_ADAPTER_TEST_H_
#define HERMES_TEST_UNIT_HERMES_ADAPTERS_STDIO_STDIO_ADAPTER_TEST_H_

#include "binary_file_tests.h"

namespace hermes::adapter::test {
template<bool WITH_MPI>
class StdioTest : public BinaryFileTests {
 public:
  FileInfo new_file_;
  FileInfo existing_file_;
  FileInfo shared_new_file_;
  FileInfo shared_existing_file_;
  FileInfo tmp_file_;
  unsigned int offset_seed_ = 1;
  unsigned int rs_seed_ = 1;
  unsigned int temporal_interval_seed_ = 5;
  size_t stride_size_ = 512;
  unsigned int temporal_interval_ms_ = 1;
  size_t small_min_ = 1;
  size_t small_max_ = 4 * 1024;
  size_t medium_min_ = 4 * 1024 + 1;
  size_t medium_max_ = 256 * 1024;
  size_t large_min_ = 256 * 1024 + 1;
  size_t large_max_ = 3 * 1024 * 1024;

  FILE* fh_orig_;
  FILE* fh_cmp_;
  int status_orig_;
  size_t size_read_orig_;
  size_t size_written_orig_;
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

  void test_fopen(FileInfo &info, const char* mode) {
    fh_orig_ = fopen(info.hermes_.c_str(), mode);
    fh_cmp_ = fopen(info.cmp_.c_str(), mode);
    bool is_same = (fh_cmp_ != nullptr && fh_orig_ != nullptr) ||
        (fh_cmp_ == nullptr && fh_orig_ == nullptr);
    REQUIRE(is_same);
  }
  void test_fclose() {
    status_orig_ = fclose(fh_orig_);
    int status = fclose(fh_cmp_);
    REQUIRE(status == status_orig_);
  }
  void test_fwrite(const void* ptr, size_t size) {
    size_written_orig_ = fwrite(ptr, sizeof(char), size, fh_orig_);
    size_t size_written = fwrite(ptr, sizeof(char), size, fh_cmp_);
    REQUIRE(size_written == size_written_orig_);
  }
  void test_fread(char* ptr, size_t size) {
    size_read_orig_ = fread(ptr, sizeof(char), size, fh_orig_);
    std::vector<unsigned char> read_data(size, 'r');
    size_t size_read = fread(read_data.data(), sizeof(char), size, fh_cmp_);
    REQUIRE(size_read == size_read_orig_);
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
    status_orig_ = fseek(fh_orig_, offset, whence);
    int status = fseek(fh_cmp_, offset, whence);
    REQUIRE(status == status_orig_);
  }
};

}  // namespace hermes::adapter::test

#if defined(HERMES_MPI_TESTS)
#define TEST_INFO \
  hshm::EasySingleton<hermes::adapter::test::StdioTest<HERMES_MPI_TESTS>>::GetInstance()
#else
#define TEST_INFO \
  hshm::EasySingleton<hermes::adapter::test::StdioTest<false>>::GetInstance()
#endif

#endif //HERMES_TEST_UNIT_HERMES_ADAPTERS_STDIO_STDIO_ADAPTER_TEST_H_
