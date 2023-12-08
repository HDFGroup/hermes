//
// Created by lukemartinlogan on 12/7/23.
//

#ifndef HERMES_TEST_UNIT_HERMES_ADAPTERS_BINARY_FILE_TESTS_H_
#define HERMES_TEST_UNIT_HERMES_ADAPTERS_BINARY_FILE_TESTS_H_

#include "filesystem_tests.h"

namespace hermes::adapter::fs::test {

class BinaryFileTests : public FilesystemTests<char> {
 public:
  std::vector<char> GenerateData() override {
    return GenRandom(total_size_, 200);
  }

  void CreateFile(const std::string &path,
                  std::vector<char> &data) override {
    int fd = open(path.c_str(), O_CREAT | O_TRUNC | O_RDWR, 0666);
    if (fd == -1) {
      HELOG(kFatal, "Failed to open file: {}", path);
    }
    int ret = write(fd, data.data(), data.size());
    if (ret != data.size()) {
      return;
    }
    close(fd);
    REQUIRE(stdfs::file_size(path) == data.size());
    HILOG(kInfo, "Created file {}", path);
  }

  void CompareFiles(FileInfo &info) override {
    size_t cmp_size = stdfs::file_size(info.cmp_);
    size_t hermes_size = stdfs::file_size(info.hermes_);
    REQUIRE(cmp_size == hermes_size);
    std::vector<char> d1(cmp_size, '0');
    std::vector<char> d2(hermes_size, '1');
    LoadFile(info.cmp_, d1);
    LoadFile(info.hermes_, d2);
    CompareBuffers(d1, d2);
  }

 private:
  void LoadFile(const std::string &path, std::vector<char> &data) {
    FILE* fh = fopen(path.c_str(), "r");
    if (fh == nullptr) {
      HELOG(kFatal, "Failed to open file: {}", path);
    }
    REQUIRE(fh != nullptr);
    size_t load_size = fread(data.data(), 1, data.size(), fh);
    REQUIRE(load_size == data.size());
    int status = fclose(fh);
    REQUIRE(status == 0);
  }

  void CompareBuffers(const std::vector<char> &d1, const std::vector<char> &d2) {
    size_t char_mismatch = 0;
    for (size_t pos = 0; pos < d1.size(); ++pos) {
      if (d1[pos] != d2[pos]) char_mismatch++;
    }
    REQUIRE(char_mismatch == 0);
  }
};

}  // namespace hermes::adapter::fs::test

#endif  // HERMES_TEST_UNIT_HERMES_ADAPTERS_BINARY_FILE_TESTS_H_
