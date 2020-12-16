#include <catch_config.h>
#include <iostream>
#include <experimental/filesystem>
#include <stdio.h>

namespace fs = std::experimental::filesystem;

namespace hermes::adapter::stdio::test {
struct Arguments {
    std::string filename = "test.dat";
    std::string directory = "/tmp";
    long request_size = 4096;
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

TEST_CASE("Open", "[process=1][operation=single_open][repetition=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string new_file = fullpath.string() + "_new";
    std::string existing_file = fullpath.string() + "_ext";
    if (fs::exists(new_file)) fs::remove(new_file);
    if (fs::exists(existing_file)) fs::remove(existing_file);
    if (!fs::exists(existing_file)) {
        std::ofstream ofs(existing_file);
        ofs.close();
    }

    SECTION("open non-existant file") {
        FILE* fd = fopen(new_file.c_str(), "r");
        REQUIRE(fd == nullptr);
        fd = fopen(new_file.c_str(), "r+");
        REQUIRE(fd == nullptr);
    }

    SECTION("truncate existing file and write-only") {
        FILE* fd = fopen(existing_file.c_str(), "w");
        REQUIRE(fd != nullptr);
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    SECTION("truncate existing file and read/write") {
        FILE* fd = fopen(existing_file.c_str(), "w+");
        REQUIRE(fd != nullptr);
        int status = fclose(fd);
        REQUIRE(status == 0);
    }

    SECTION("open existing file") {
        FILE* fd = fopen(existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        int status = fclose(fd);
        REQUIRE(status == 0);
        fd = fopen(existing_file.c_str(), "r");
        REQUIRE(fd != nullptr);
        status = fclose(fd);
        REQUIRE(status == 0);
    }

    SECTION("append write new file") {
        FILE* fd = fopen(existing_file.c_str(), "a");
        REQUIRE(fd != nullptr);
        int status = fclose(fd);
        REQUIRE(status == 0);
    }

    SECTION("append write and read new file") {
        FILE* fd = fopen(existing_file.c_str(), "a+");
        REQUIRE(fd != nullptr);
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(fullpath);
}

TEST_CASE("SingleWrite",
          "[process=1][operation=single_write]"
          "[request_size=type-fixed][repetition=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string new_file = fullpath.string() + "_new";
    std::string existing_file = fullpath.string() + "_ext";
    if (fs::exists(new_file)) fs::remove(new_file);
    if (fs::exists(existing_file)) fs::remove(existing_file);
    if (!fs::exists(existing_file)) {
        std::ofstream ofs(existing_file);
        ofs.close();
    }

    SECTION("write to existing file") {
        FILE* fd = fopen(existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        long offset = fseek(fd, 0, SEEK_SET);
        long size_written = fwrite(info.data.c_str(),
                                   sizeof(char), args.request_size, fd);
        REQUIRE(size_written == args.request_size);
        int status = fclose(fd);
        REQUIRE(status == 0);
        REQUIRE(fs::file_size(existing_file) == size_written);
    }

    SECTION("write to new  file") {
        FILE* fd = fopen(new_file.c_str(), "w+");
        REQUIRE(fd != nullptr);
        long size_written = fwrite(info.data.c_str(), sizeof(char),
                                   args.request_size, fd);
        REQUIRE(size_written == args.request_size);
        int status = fclose(fd);
        REQUIRE(status == 0);
        REQUIRE(fs::file_size(new_file) == size_written);
    }

    SECTION("write to existing file with truncate") {
        FILE* fd = fopen(existing_file.c_str(), "w");
        REQUIRE(fd != nullptr);
        long size_written = fwrite(info.data.c_str(), sizeof(char),
                                   args.request_size, fd);
        REQUIRE(size_written == args.request_size);
        int status = fclose(fd);
        REQUIRE(status == 0);
        REQUIRE(fs::file_size(existing_file) == size_written);
    }

    SECTION("append to existing file") {
        auto existing_size = fs::file_size(existing_file);
        FILE* fd = fopen(existing_file.c_str(), "a+");
        REQUIRE(fd != nullptr);
        long size_written = fwrite(info.data.c_str(),
                                   sizeof(char), args.request_size, fd);
        REQUIRE(size_written == args.request_size);
        int status = fclose(fd);
        REQUIRE(status == 0);
        REQUIRE(fs::file_size(existing_file) == existing_size + size_written);
    }

    SECTION("append to new file") {
        FILE* fd = fopen(new_file.c_str(), "w+");
        REQUIRE(fd != nullptr);
        long size_written = fwrite(info.data.c_str(), sizeof(char),
                                   args.request_size, fd);
        REQUIRE(size_written == args.request_size);
        int status = fclose(fd);
        REQUIRE(status == 0);
        REQUIRE(fs::file_size(new_file) == size_written);
    }
    fs::remove(fullpath);
}

TEST_CASE("BatchedWrite",
          "[process=1][operation=batched_write]"
          "[request_size=type-fixed][repetition=100][pattern=sequential]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string new_file = fullpath.string() + "_new";
    if (fs::exists(new_file)) fs::remove(new_file);
    long num_iterations = 100;

    SECTION("write to existing file") {
        FILE* fd = fopen(new_file.c_str(), "w+");
        REQUIRE(fd != nullptr);

        for (int i = 0; i < num_iterations; ++i) {
            long size_written = fwrite(info.data.c_str(),
                                       sizeof(char), args.request_size, fd);
            REQUIRE(size_written == args.request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
        REQUIRE(fs::file_size(new_file) == num_iterations * args.request_size);
    }
    fs::remove(new_file);
}


TEST_CASE("BatchedReadSequential",
          "[process=1][operation=batched_read]"
          "[request_size=type-fixed][repetition=100][pattern=sequential]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 100;
    if (!fs::exists(existing_file)) {
        std::string cmd = "dd if=/dev/zero of="+existing_file+
                        " bs=1 count=0 seek="+
                        std::to_string(args.request_size*num_iterations)
                        + " > /dev/null 2>&1";
        system(cmd.c_str());
        REQUIRE(fs::file_size(existing_file)
                == num_iterations * args.request_size);
    }

    SECTION("read from existing file") {
        FILE* fd = fopen(existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        std::string data(args.request_size,'1');
        for (int i = 0; i < num_iterations; ++i) {
            long size_read = fread(data.data(),
                                       sizeof(char), args.request_size, fd);
            REQUIRE(size_read == args.request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedReadRandom",
          "[process=1][operation=batched_read]"
          "[request_size=type-fixed][repetition=100][pattern=random]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 100;
    unsigned int seed = 100;

    if (!fs::exists(existing_file)) {
        std::string cmd = "dd if=/dev/zero of="+existing_file+
                          " bs=1 count=0 seek="+
                          std::to_string(args.request_size*num_iterations)
                          + " > /dev/null 2>&1";
        system(cmd.c_str());
        REQUIRE(fs::file_size(existing_file)
                == num_iterations * args.request_size);
    }
    long total_size = fs::file_size(existing_file);

    SECTION("read from existing file") {
        FILE* fd = fopen(existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        std::string data(args.request_size, '1');
        for (int i = 0; i < num_iterations; ++i) {
            auto offset = rand_r(&seed) % (total_size - args.request_size);
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long size_read = fread(data.data(),
                                   sizeof(char), args.request_size, fd);
            REQUIRE(size_read == args.request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedUpdateRandom",
          "[process=1][operation=batched_write]"
          "[request_size=type-fixed][repetition=100][pattern=random]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 100;
    unsigned int seed = 100;

    if (!fs::exists(existing_file)) {
        std::string cmd = "dd if=/dev/zero of="+existing_file+
                          " bs=1 count=0 seek="+
                          std::to_string(args.request_size*num_iterations)
                          + " > /dev/null 2>&1";
        system(cmd.c_str());
        REQUIRE(fs::file_size(existing_file)
                == num_iterations * args.request_size);
    }
    long total_size = fs::file_size(existing_file);

    SECTION("read from existing file") {
        FILE* fd = fopen(existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        std::string data(args.request_size,'1');
        for (int i = 0; i < num_iterations; ++i) {
            auto offset = rand_r(&seed) % (total_size - args.request_size);
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long size_read = fwrite(data.data(),
                                   sizeof(char), args.request_size, fd);
            REQUIRE(size_read == args.request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}
