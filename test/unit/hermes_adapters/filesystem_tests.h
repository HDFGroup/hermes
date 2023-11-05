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
#include <stdio.h>

#include <cmath>
#include <cstdio>
#include <string>
#include <basic_test.h>

#include "hermes_shm/util/singleton.h"
#include "hermes/hermes.h"

#define CATCH_CONFIG_RUNNER
#include <mpi.h>
#include <catch2/catch_all.hpp>

namespace hermes::adapter::fs::test {

/** Pre-create the Hermes file */
#define TEST_DO_CREATE BIT_OPT(u32, 0)
/** Allow this file to be buffered with Hermes */
#define TEST_WITH_HERMES BIT_OPT(u32, 1)
/** Make this file shared across processes */
#define TEST_FILE_SHARED BIT_OPT(u32, 2)

struct FileInfo {
  std::string hermes_;  /** The file produced by Hermes */
  std::string cmp_;     /** The file to verify against */
  bitfield32_t flags_;           /** Various flags */
};

class FilesystemTests {
 public:
  bool supports_tmpfile;
  std::vector<char> write_data_;
  std::vector<char> read_data_;
  std::list<FileInfo> files_;
  int pid_;
  std::string pid_str_;

  std::string filename_ = "test.dat";
  std::string dir_ = "/tmp/test_hermes";
  size_t request_size_ = KILOBYTES(64);
  size_t num_iterations_ = 64;
  size_t total_size_;    // The size of the EXISTING file
  int rank_ = 0;
  int comm_size_ = 1;

 public:
  cl::Parser DefineOptions() {
    return cl::Opt(filename_, "filename")["-f"]["--filename"](
        "Filename used for performing I/O") |
        cl::Opt(dir_, "dir")["-d"]["--directory"](
            "Directory used for performing I/O") |
        cl::Opt(request_size_, "request_size")["-s"]["--request_size"](
            "Request size used for performing I/O");
  }

  int Init(int argc, char **argv) {
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank_);
    MPI_Comm_size(MPI_COMM_WORLD, &comm_size_);
    Catch::Session session;
    auto cli = session.cli() | DefineOptions();
    session.cli(cli);
    int rc = session.applyCommandLine(argc, argv);
    if (rc != 0) return rc;

    total_size_ = request_size_ * num_iterations_;
    write_data_ = GenRandom(request_size_);
    read_data_ = std::vector<char>(request_size_, 'r');
    supports_tmpfile = FilesystemSupportsTmpfile();
    pid_ = getpid();
    pid_str_ = std::to_string(pid_);
    RegisterFiles();

#if HERMES_INTERCEPT == 1
    setenv("HERMES_FLUSH_MODE", "kSync", 1);
  HERMES_CLIENT_CONF.flushing_mode_ = hermes::FlushingMode::kSync;
#endif
    rc = session.run();
    MPI_Finalize();
    return rc;
  }

  void RegisterPath(const std::string &basename,
                    u32 flags,
                    FileInfo &info) {
    info.hermes_ = dir_ + "/" + filename_ + "_" + basename + "_";
    info.cmp_ = info.hermes_ + "cmp_";
    if (!info.flags_.Any(TEST_FILE_SHARED)) {
      info.hermes_ += pid_str_;
      info.cmp_ += pid_str_;
    }
    info.flags_.SetBits(flags | TEST_WITH_HERMES);
    files_.push_back(info);
  }

  void RegisterTmpPath(FileInfo &info) {
    info.hermes_ = "/tmp";
    info.cmp_ = "/tmp";
    info.flags_.SetBits(0);
    files_.push_back(info);
  }

  void IgnoreAllFiles() {
    for (const FileInfo &info : files_) {
      HERMES_CLIENT_CONF.SetAdapterPathTracking(info.hermes_, false);
      HERMES_CLIENT_CONF.SetAdapterPathTracking(info.cmp_, false);
    }
  }

  void TrackAllFiles() {
    for (const FileInfo &info : files_) {
      HERMES_CLIENT_CONF.SetAdapterPathTracking(info.hermes_, 
                                                info.flags_.Any(TEST_WITH_HERMES));
    }
  }

  void RemoveAllFiles() {
    for (const FileInfo &info : files_) {
      if (info.flags_.Any(TEST_WITH_HERMES)) {
        RemoveFile(info.hermes_);
        RemoveFile(info.cmp_);
        // HILOG(kInfo, "Removing files: {} {}", info.hermes_, info.cmp_);
      }
    }
  }

  void CreateFiles() {
    std::vector<char> data = GenRandom(total_size_, 200);
    for (const FileInfo &info : files_) {
      if (!info.flags_.Any(TEST_WITH_HERMES)) {
        continue;
      }
      if (info.flags_.Any(TEST_DO_CREATE)) {
        if (info.flags_.Any(TEST_FILE_SHARED) && rank_ != 0) {
          continue;
        }
        CreateFile(info.cmp_, data);
        CreateFile(info.hermes_, data);
      }
    }
  }

  void Flush() {
#if HERMES_INTERCEPT == 1
    // HERMES->Clear();
  HRUN_ADMIN->FlushRoot(DomainId::GetGlobal());
#endif
  }

  void Pretest() {
    MPI_Barrier(MPI_COMM_WORLD);
    IgnoreAllFiles();
    RemoveAllFiles();
    CreateFiles();
    TrackAllFiles();
    MPI_Barrier(MPI_COMM_WORLD);
  }

  void Posttest(bool compare_data = true) {
    Flush();
    IgnoreAllFiles();
    if (compare_data) {
      for (FileInfo &info : files_) {
        if (!info.flags_.Any(TEST_WITH_HERMES)) {
          continue;
        }
        if (stdfs::exists(info.hermes_) && stdfs::exists(info.cmp_)) {
          size_t cmp_size = stdfs::file_size(info.cmp_);
          size_t hermes_size = stdfs::file_size(info.hermes_);
          REQUIRE(cmp_size == hermes_size);
          std::vector<char> d1(cmp_size, '0');
          std::vector<char> d2(hermes_size, '1');
          LoadFile(info.cmp_, d1);
          LoadFile(info.hermes_, d2);
          CompareBuffers(d1, d2);
        }
      }
    }
    /* Delete the files from both Hermes and the backend. */
    TrackAllFiles();
    RemoveAllFiles();
    Flush();
  }

  virtual void RegisterFiles() = 0;

  static size_t GetRandomOffset(
      size_t i, unsigned int offset_seed,
      size_t stride, size_t total_size) {
    return abs((int)(((i * rand_r(&offset_seed)) % stride) % total_size));
  }

 private:
  void LoadFile(const std::string &path, std::vector<char> &data) {
    FILE* fh = fopen(path.c_str(), "r");
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

  std::vector<char> GenRandom(const size_t len, int seed = 100) {
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

  void CreateFile(const std::string &path,
                  const std::vector<char> &data) {
    int fd = open(path.c_str(), O_CREAT | O_TRUNC | O_RDWR, 0666);
    if (fd == -1) {
      HELOG(kFatal, "Failed to open file: {}", path);
    }
    write(fd, data.data(), data.size());
    close(fd);
    REQUIRE(stdfs::file_size(path) == data.size());
  }

  void RemoveFile(const std::string &path) {
    stdfs::remove(path);
    if (stdfs::exists(path)) {
      HELOG(kFatal, "Failed to remove: {}", path)
    }
  }

  bool FilesystemSupportsTmpfile() {
    bool result = false;

#if O_TMPFILE
    // NOTE(chogan): Even if O_TMPFILE is defined, the underlying filesystem might
    // not support it.
    int tmp_fd = open("/tmp", O_WRONLY | O_TMPFILE, 0600);
    if (tmp_fd > 0) {
      result = true;
      close(tmp_fd);
    }
#endif

    return result;
  }
};

}

#endif  // HERMES_TEST_UNIT_HERMES_ADAPTERS_FILESYSTEM_TESTS_H_
