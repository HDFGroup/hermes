#include <iostream>
#include <experimental/filesystem>
#include <stdio.h>
#include <catch_config.h>
namespace fs = std::experimental::filesystem;

namespace hermes::adapter::stdio::test {
struct Arguments {
    std::string filename = "test.dat";
    std::string directory = "/tmp";
    long request_size = 65536;
};
struct Info {
    int rank = 0;
    int comm_size = 1;
    std::string write_data;
    std::string read_data;
    std::string new_file;
    std::string existing_file;
    long num_iterations = 1024;
    unsigned int offset_seed = 1;
    unsigned int rs_seed = 1;
    unsigned int temporal_interval_seed = 5;
    long total_size;
    long stride_size = 1024;
    unsigned int temporal_interval_ms = 1;
    long small_min = 1, small_max = 4 * 1024;
    long medium_min = 4 * 1024 + 1,
            medium_max = 256 * 1024;
    long large_min = 256 * 1024 + 1,
                large_max = 4 * 1024 * 1024;
};
}  // namespace hermes::adapter::stdio::test
hermes::adapter::stdio::test::Arguments args;
hermes::adapter::stdio::test::Info info;

int init() {
    info.write_data = std::string(args.request_size, 'w');
    info.read_data = std::string(args.request_size, 'r');
    return 0;
}
int finalize() {
    return 0;
}

int pretest() {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    info.new_file = fullpath.string() + "_new";
    info.existing_file = fullpath.string() + "_ext";
    if (fs::exists(info.new_file)) fs::remove(info.new_file);
    if (fs::exists(info.existing_file)) fs::remove(info.existing_file);
    if (!fs::exists(info.existing_file)) {
        std::string cmd = "dd if=/dev/zero of="+info.existing_file+
                          " bs=1 count=0 seek="+
                          std::to_string(args.request_size
                          * info.num_iterations)
                          + " > /dev/null 2>&1";
        system(cmd.c_str());
        REQUIRE(fs::file_size(info.existing_file)
                == args.request_size * info.num_iterations);
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
    return cl::Opt(args.filename, "filename")["-f"]["--filename"]
                   ("Filename used for performing I/O")
           | cl::Opt(args.directory, "dir")["-d"]["--directory"]
                   ("Directory used for performing I/O")
           | cl::Opt(args.request_size, "request_size")["-s"]["--request_size"]
                   ("Request size used for performing I/O");
}

#include "stdio_adapter_basic_test.cpp"
#include "stdio_adapter_rs_test.cpp"
