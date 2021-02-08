#include <catch_config.h>
#include <fcntl.h>
#include <unistd.h>

#include <experimental/filesystem>
#include <iostream>
namespace fs = std::experimental::filesystem;

namespace hermes::adapter::posix::test {
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
  std::string shared_new_file;
  std::string shared_existing_file;
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
}  // namespace hermes::adapter::posix::test

hermes::adapter::posix::test::Arguments args;
hermes::adapter::posix::test::Info info;

int init(int* argc, char*** argv) {
  MPI_Init(argc, argv);
  info.write_data = std::string(args.request_size, 'w');
  info.read_data = std::string(args.request_size, 'r');
  MPI_Comm_rank(MPI_COMM_WORLD, &info.rank);
  MPI_Comm_size(MPI_COMM_WORLD, &info.comm_size);
  return 0;
}
int finalize() {
  MPI_Finalize();
  return 0;
}

int pretest() {
  REQUIRE(info.comm_size > 1);
  fs::path fullpath = args.directory;
  fullpath /= args.filename;
  info.shared_new_file =
      fullpath.string() + "_new_" + std::to_string(info.comm_size);
  info.shared_existing_file =
      fullpath.string() + "_ext_" + std::to_string(info.comm_size);
  info.new_file = fullpath.string() + "_new_" + std::to_string(info.rank + 1) +
                  "_of_" + std::to_string(info.comm_size) + "_" +
                  std::to_string(getpid());
  info.existing_file =
      fullpath.string() + "_ext_" + std::to_string(info.rank + 1) + "_of_" +
      std::to_string(info.comm_size) + "_" + std::to_string(getpid());
  if (info.rank == 0) {
    if (fs::exists(info.shared_new_file)) fs::remove(info.shared_new_file);
    if (fs::exists(info.shared_existing_file))
      fs::remove(info.shared_existing_file);
    if (!fs::exists(info.shared_existing_file)) {
      std::string cmd =
          "dd if=/dev/zero of=" + info.shared_existing_file +
          " bs=" + std::to_string(args.request_size * info.num_iterations) +
          " count=1  > /dev/null 2>&1";
      int status = system(cmd.c_str());
      REQUIRE(status != -1);
      REQUIRE(fs::file_size(info.shared_existing_file) ==
              args.request_size * info.num_iterations);
    }
  }
  MPI_Barrier(MPI_COMM_WORLD);
  if (fs::exists(info.new_file)) fs::remove(info.new_file);
  if (fs::exists(info.existing_file)) fs::remove(info.existing_file);
  if (!fs::exists(info.existing_file)) {
    std::string cmd = "dd if=/dev/zero of=" + info.existing_file + " bs=" +
                      std::to_string(args.request_size * info.num_iterations) +
                      " count=1  > /dev/null 2>&1";
    int status = system(cmd.c_str());
    REQUIRE(status != -1);
    REQUIRE(fs::file_size(info.existing_file) ==
            args.request_size * info.num_iterations);
  }

  info.total_size = fs::file_size(info.existing_file);
  REQUIRE(info.total_size > 0);
  return 0;
}

int posttest() {
  if (fs::exists(info.new_file)) fs::remove(info.new_file);
  if (fs::exists(info.existing_file)) fs::remove(info.existing_file);
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

#include "posix_adapter_basic_test.cpp"
#include "posix_adapter_rs_test.cpp"
#include "posix_adapter_shared_test.cpp"
