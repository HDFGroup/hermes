#include <catch_config.h>
#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>

#include <experimental/filesystem>
#include <iostream>
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

int init() {
  info.write_data = std::string(args.request_size, 'w');
  info.read_data = std::string(args.request_size, 'r');
  MPI_Comm_rank(MPI_COMM_WORLD, &info.rank);
  MPI_Comm_size(MPI_COMM_WORLD, &info.comm_size);
  if (info.rank == 0) {
    printf("%d ready for attach\n", info.comm_size);
    getchar();
  }
  MPI_Barrier(MPI_COMM_WORLD);
  return 0;
}
int finalize() { return 0; }

int pretest() {
  REQUIRE(info.comm_size > 1);
  fs::path fullpath = args.directory;
  fullpath /= args.filename;
  info.new_file = fullpath.string() + "_new_" + std::to_string(info.rank) +
                  "_of_" + std::to_string(info.comm_size);
  info.existing_file = fullpath.string() + "_ext_" + std::to_string(info.rank) +
                       "_of_" + std::to_string(info.comm_size);
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
    info.total_size = fs::file_size(info.existing_file);
  }
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

#include "stdio_adapter_basic_test.cpp"
#include "stdio_adapter_func_test.cpp"
#include "stdio_adapter_rs_test.cpp"
