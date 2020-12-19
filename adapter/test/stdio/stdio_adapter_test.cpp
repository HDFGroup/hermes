#include <iostream>
#include <experimental/filesystem>
#include <stdio.h>
#include <catch_config.h>

namespace fs = std::experimental::filesystem;

namespace hermes::adapter::stdio::test {
struct Arguments {
    std::string filename = "test.dat";
    std::string directory = "/tmp";
    long request_size = 16384;
};
struct Info {
    std::string data;
};
}
hermes::adapter::stdio::test::Arguments args;
hermes::adapter::stdio::test::Info info;

int init() {
    info.data = std::string(args.request_size, 'x');
    return 0;
}
int finalize() {
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
