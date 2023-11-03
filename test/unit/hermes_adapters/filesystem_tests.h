//
// Created by lukemartinlogan on 11/3/23.
//

#ifndef HERMES_TEST_UNIT_HERMES_ADAPTERS_FILESYSTEM_TESTS_H_
#define HERMES_TEST_UNIT_HERMES_ADAPTERS_FILESYSTEM_TESTS_H_


#include <fcntl.h>
#include <stdarg.h>
#include <unistd.h>

#include <filesystem>
#include <iostream>
#include "hermes/hermes.h"
#include <stdio.h>

#include "catch_config.h"
#include "adapter_test_utils.h"

namespace hermes::adapter::test {

struct FileInfo {
  std::string path;
  bool tracked_;
};

class FilesystemTests {
 public:
  bool supports_tmpfile;
  std::vector<char> write_data;
  std::vector<char> read_data;
  std::list<FileInfo> files_;

 public:
  FilesystemTests(size_t request_size) {
#if HERMES_INTERCEPT == 1
    setenv("HERMES_FLUSH_MODE", "kSync", 1);
  HERMES_CLIENT_CONF.flushing_mode_ = hermes::FlushingMode::kSync;
#endif
    write_data = gen_random(request_size);
    read_data = std::vector<char>(request_size, 'r');
    supports_tmpfile = FilesystemSupportsTmpfile();
  }

  void RegisterPath(const std::string &path, bool track, std::string &path_var) {
    files_.push_back({path, track});
    path_var = path;
  }

  void IgnoreAllFiles() {
    for (const FileInfo &paths : files_) {
      HERMES_CLIENT_CONF.SetAdapterPathTracking(paths.path, false);
    }
  }

  void TrackAllFiles() {
    for (const FileInfo &paths : files_) {
      if (paths.tracked_) {
        HERMES_CLIENT_CONF.SetAdapterPathTracking(paths.path, true);
      }
    }
  }

  void RemoveAllFiles() {
    for (const FileInfo &paths : files_) {
      std::filesystem::remove(paths.path);
    }
  }

  void CreateFiles(const std::vector<std::string> &paths, size_t file_size) {
    std::vector<char> data = gen_random(file_size, 200);
    for (const std::string &path : paths) {
      CreateFile(path, data);
    }
  }

  void CreateFile(const std::string &path, const std::vector<char> &data) {
    int fd = open(path.c_str(), O_CREAT | O_RDWR, 0666);
    if (fd == -1) {
      HELOG(kFatal, "Failed to open file");
    }
    write(fd, data.data(), data.size());
    close(fd);
  }

  void Clear() {
#if HERMES_INTERCEPT == 1
    // HERMES->Clear();
  RemoveAllFiles();
  HRUN_ADMIN->FlushRoot(DomainId::GetGlobal());
#endif
  }

  void LoadFile(const std::string &path, std::vector<char> &data) {
    FILE* fh2 = fopen(path.c_str(), "r");
    REQUIRE(fh2 != nullptr);
    size_t read_d2 = fread(data.data(), data.size(), sizeof(unsigned char), fh2);
    REQUIRE(read_d2 == sizeof(unsigned char));
    int status = fclose(fh2);
    REQUIRE(status == 0);
  }

  void VerifyFile(const std::vector<char> &d1, const std::vector<char> &d2) {
    size_t char_mismatch = 0;
    for (size_t pos = 0; pos < d1.size(); ++pos) {
      if (d1[pos] != d2[pos]) char_mismatch++;
    }
    REQUIRE(char_mismatch == 0);
  }

  virtual void PreTest() = 0;

  virtual void PostTest() = 0;

  virtual void RegisterFiles() = 0;

  std::vector<char> gen_random(const int len, int seed = 100) {
    auto tmp_s = std::vector<char>(len);
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

    srand(seed);
    for (int i = 0; i < len; ++i)
      tmp_s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
    return tmp_s;
  }
};

}

#endif  // HERMES_TEST_UNIT_HERMES_ADAPTERS_FILESYSTEM_TESTS_H_
