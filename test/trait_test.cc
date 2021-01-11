#include "catch_config.h"

struct Arguments {
  std::string filename = "test.dat";
  std::string directory = "/tmp";
  long request_size = 65536;
};

Arguments args;

cl::Parser define_options() {
  return cl::Opt(args.filename, "filename")["-f"]["--filename"](
             "Filename used for performing I/O") |
         cl::Opt(args.directory, "dir")["-d"]["--directory"](
             "Directory used for performing I/O") |
         cl::Opt(args.request_size, "request_size")["-s"]["--request_size"](
             "Request size used for performing I/O");
}

int init() { return 0;}

int finalize() {return 0;}

TEST_CASE("CustomTrait", "[module=trait][type=meta]") {
  REQUIRE(1 == 1);
  SECTION("Basic") {
    REQUIRE(1 == 1);
    REQUIRE(1 == 1);
  }
  REQUIRE(1 == 1);
}