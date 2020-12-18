#include <catch_config.h>
#include <iostream>
#include <experimental/filesystem>
#include <stdio.h>

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

TEST_CASE("Open", "[process=1][operation=single_open]"
                  "[repetition=1][file=1]") {
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
          "[request_size=type-fixed][repetition=1]"
          "[file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string new_file = fullpath.string() + "_new";
    std::string existing_file = fullpath.string() + "_ext";
    if (fs::exists(new_file)) fs::remove(new_file);
    if (fs::exists(existing_file)) fs::remove(existing_file);
    if (!fs::exists(existing_file)) {
        std::string cmd = "dd if=/dev/zero of="+existing_file+
                          " bs=1 count=0 seek="+
                          std::to_string(args.request_size)
                          + " > /dev/null 2>&1";
        system(cmd.c_str());
        REQUIRE(fs::file_size(existing_file)
                == args.request_size);
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

    SECTION("write to existing file at the end") {
        FILE* fd = fopen(existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        int status = fseek(fd, 0, SEEK_END);
        REQUIRE(status == 0);
        long offset = ftell(fd);
        REQUIRE(offset == args.request_size);
        long size_written = fwrite(info.data.c_str(), sizeof(char),
                                   args.request_size, fd);
        REQUIRE(size_written == args.request_size);
        status = fclose(fd);
        REQUIRE(status == 0);
        REQUIRE(fs::file_size(existing_file) == size_written + offset);
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

TEST_CASE("SingleRead",
          "[process=1][operation=single_read]"
          "[request_size=type-fixed][repetition=1]"
          "[file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string new_file = fullpath.string() + "_new";
    std::string existing_file = fullpath.string() + "_ext";
    if (fs::exists(new_file)) fs::remove(new_file);
    if (fs::exists(existing_file)) fs::remove(existing_file);
    if (!fs::exists(existing_file)) {
        std::string cmd = "dd if=/dev/zero of="+existing_file+
                          " bs=1 count=0 seek="+
                          std::to_string(args.request_size)
                          + " > /dev/null 2>&1";
        system(cmd.c_str());
        REQUIRE(fs::file_size(existing_file)
                == args.request_size);
    }
    SECTION("read from non-existing file") {
        FILE* fd = fopen(new_file.c_str(), "r");
        REQUIRE(fd == nullptr);
    }

    SECTION("read from existing file") {
        FILE* fd = fopen(existing_file.c_str(), "r");
        REQUIRE(fd != nullptr);
        long offset = ftell(fd);
        REQUIRE(offset == 0);
        long size_read = fread(info.data.data(), sizeof(char),
                                   args.request_size, fd);
        REQUIRE(size_read == args.request_size);
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    SECTION("read at the end of existing file") {
        FILE* fd = fopen(existing_file.c_str(), "r");
        REQUIRE(fd != nullptr);
        int status = fseek(fd, 0, SEEK_END);
        REQUIRE(status == 0);
        long offset = ftell(fd);
        REQUIRE(offset == args.request_size);
        long size_read = fread(info.data.data(), sizeof(char),
                               args.request_size, fd);
        REQUIRE(size_read == 0);
        status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedWrite",
          "[process=1][operation=batched_write]"
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=sequential][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string new_file = fullpath.string() + "_new";
    if (fs::exists(new_file)) fs::remove(new_file);
    long num_iterations = 1024;

    SECTION("write to existing file") {
        FILE* fd = fopen(new_file.c_str(), "w+");
        REQUIRE(fd != nullptr);

        for (int i = 0; i < num_iterations; ++i) {
            int status = fseek(fd, 0, SEEK_SET);
            REQUIRE(status == 0);
            long offset = ftell(fd);
            REQUIRE(offset == 0);
            long size_written = fwrite(info.data.c_str(),
                                       sizeof(char), args.request_size, fd);
            REQUIRE(size_written == args.request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
        REQUIRE(fs::file_size(new_file) == args.request_size);
    }

    SECTION("write to new file always at start") {
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
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=sequential][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
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
        std::string data(args.request_size, '1');
        for (int i = 0; i < num_iterations; ++i) {
            long size_read = fread(data.data(),
                                       sizeof(char), args.request_size, fd);
            REQUIRE(size_read == args.request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }

    SECTION("read from existing file always at start") {
        FILE* fd = fopen(existing_file.c_str(), "w+");
        REQUIRE(fd != nullptr);

        for (int i = 0; i < num_iterations; ++i) {
            int status = fseek(fd, 0, SEEK_SET);
            REQUIRE(status == 0);
            long offset = ftell(fd);
            REQUIRE(offset == 0);
            long size_written = fwrite(info.data.c_str(),
                                       sizeof(char), args.request_size, fd);
            REQUIRE(size_written == args.request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedReadRandom",
          "[process=1][operation=batched_read]"
          "[request_size=type-fixed]"
          "[repetition=1024][pattern=random][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
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
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=random][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
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
            long size_read = fwrite(data.data(),
                                   sizeof(char), args.request_size, fd);
            REQUIRE(size_read == args.request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedReadStrideFixed",
          "[process=1][operation=batched_read]"
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=stride_fixed][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    long stride_size = 4*1024;

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
            auto offset = (i * stride_size) % total_size;
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

TEST_CASE("BatchedUpdateStrideFixed",
          "[process=1][operation=batched_write]"
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=stride_fixed][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    long stride_size = 4*1024;

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
            auto offset = (i * stride_size) % total_size;
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

TEST_CASE("BatchedReadStrideDynamic",
          "[process=1][operation=batched_read]"
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=stride_dynamic][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    unsigned int seed = 100;
    long stride_size = 4*1024;

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
            auto offset = abs(((i * rand_r(&seed)) % stride_size) % total_size);
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

TEST_CASE("BatchedUpdateStrideDynamic",
          "[process=1][operation=batched_write]"
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=stride_dynamic][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    long stride_size = 4*1024;
    unsigned int seed = 1024;

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
    REQUIRE(total_size > 0);
    SECTION("read from existing file") {
        FILE* fd = fopen(existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        std::string data(args.request_size, '1');
        for (int i = 0; i < num_iterations; ++i) {
            auto offset = abs(((i * rand_r(&seed)) % stride_size) % total_size);
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

TEST_CASE("BatchedWriteRSVariable",
          "[process=1][operation=batched_write]"
          "[request_size=type-variable][repetition=1024]"
          "[pattern=sequential][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string new_file = fullpath.string() + "_new";
    if (fs::exists(new_file)) fs::remove(new_file);
    long num_iterations = 1024;
    unsigned int seed = 1024;

    SECTION("write to new file always at the start") {
        FILE* fd = fopen(new_file.c_str(), "w+");
        REQUIRE(fd != nullptr);
        long biggest_size_written = 0;
        for (int i = 0; i < num_iterations; ++i) {
            int status = fseek(fd, 0, SEEK_SET);
            REQUIRE(status == 0);
            long offset = ftell(fd);
            REQUIRE(offset == 0);
            long request_size = args.request_size +
                                (rand_r(&seed) % args.request_size);
            std::string data(request_size, '1');
            long size_written = fwrite(data.c_str(),
                                       sizeof(char), request_size, fd);
            REQUIRE(size_written == request_size);
            if (biggest_size_written < request_size)
                biggest_size_written = request_size;
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
        REQUIRE(fs::file_size(new_file) == biggest_size_written);
    }

    SECTION("write to new file") {
        FILE* fd = fopen(new_file.c_str(), "w+");
        REQUIRE(fd != nullptr);
        long total_size_written = 0;
        for (int i = 0; i < num_iterations; ++i) {
            long request_size = args.request_size +
                                (rand_r(&seed) % args.request_size);
            std::string data(request_size, '1');
            long size_written = fwrite(data.c_str(),
                                       sizeof(char), request_size, fd);
            REQUIRE(size_written == request_size);
            total_size_written += size_written;
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
        REQUIRE(fs::file_size(new_file) == total_size_written);
    }
    fs::remove(new_file);
}


TEST_CASE("BatchedReadSequentialRSVariable",
          "[process=1][operation=batched_read]"
          "[request_size=type-variable][repetition=1024]"
          "[pattern=sequential][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    unsigned int seed = 1024;

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
        FILE* fd = fopen(existing_file.c_str(), "r");
        REQUIRE(fd != nullptr);
        std::string data(args.request_size, '1');
        long current_offset = 0;
        for (int i = 0; i < num_iterations; ++i) {
            long request_size = (args.request_size +
                                (rand_r(&seed) % args.request_size))
                                        % (total_size - current_offset);
            std::string data(request_size, '1');
            long size_read = fread(data.data(),
                                   sizeof(char), request_size, fd);
            REQUIRE(size_read == request_size);
            current_offset += size_read;
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }

    SECTION("read from existing file always at start") {
        FILE* fd = fopen(existing_file.c_str(), "r");
        REQUIRE(fd != nullptr);

        for (int i = 0; i < num_iterations; ++i) {
            int status = fseek(fd, 0, SEEK_SET);
            REQUIRE(status == 0);
            long offset = ftell(fd);
            REQUIRE(offset == 0);
            long request_size = args.request_size +
                                (rand_r(&seed) % args.request_size);
            std::string data(request_size, '1');
            long size_read = fread(data.data(),
                                   sizeof(char), request_size, fd);
            REQUIRE(size_read == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedReadRandomRSVariable",
          "[process=1][operation=batched_read]"
          "[request_size=type-variable]"
          "[repetition=1024][pattern=random][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    unsigned int seed = 1024, request_size_seed = 200;

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
            long request_size = (args.request_size +
                                (rand_r(&request_size_seed)
                                % args.request_size))
                                % (total_size - offset);
            std::string data(request_size, '1');
            long size_read = fread(data.data(),
                                   sizeof(char), request_size, fd);
            REQUIRE(size_read == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedUpdateRandomRSVariable",
          "[process=1][operation=batched_write]"
          "[request_size=type-variable][repetition=1024]"
          "[pattern=random][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    unsigned int seed = 1024, request_size_seed = 200;

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
            long request_size = args.request_size +
                                (rand_r(&request_size_seed)
                                % args.request_size);
            std::string data(request_size, '1');
            long size_written = fwrite(data.c_str(),
                                       sizeof(char), request_size, fd);
            REQUIRE(size_written == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedReadStrideFixedRSVariable",
          "[process=1][operation=batched_read]"
          "[request_size=type-variable][repetition=1024]"
          "[pattern=stride_fixed][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    long stride_size = 4*1024;
    unsigned int request_size_seed = 200;

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
        for (int i = 0; i < num_iterations; ++i) {
            auto offset = (i * stride_size) % total_size;
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = (args.request_size +
                                (rand_r(&request_size_seed)
                                % args.request_size))
                                        % (total_size - offset);
            std::string data(request_size, '1');
            long size_read = fread(data.data(),
                                   sizeof(char), request_size, fd);
            REQUIRE(size_read == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedUpdateStrideFixedRSVariable",
          "[process=1][operation=batched_write]"
          "[request_size=type-variable][repetition=1024]"
          "[pattern=stride_fixed][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    long stride_size = 4*1024;
    unsigned int request_size_seed = 200;

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

    SECTION("write to existing file") {
        FILE* fd = fopen(existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        std::string data(args.request_size, '1');
        for (int i = 0; i < num_iterations; ++i) {
            auto offset = (i * stride_size) % total_size;
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = args.request_size +
                                (rand_r(&request_size_seed)
                                % args.request_size);
            std::string data(request_size, '1');
            long size_written = fwrite(data.c_str(),
                                       sizeof(char), request_size, fd);
            REQUIRE(size_written == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedReadStrideDynamicRSVariable",
          "[process=1][operation=batched_read]"
          "[request_size=type-variable][repetition=1024]"
          "[pattern=stride_dynamic][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    unsigned int seed = 1024, request_size_seed = 200;;
    long stride_size = 4*1024;

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
        for (int i = 0; i < num_iterations; ++i) {
            auto offset = abs(((i * rand_r(&seed)) % stride_size) % total_size);
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = args.request_size +
                                (rand_r(&request_size_seed)
                                % args.request_size);
            std::string data(request_size, '1');
            long size_read = fread(data.data(),
                                   sizeof(char), request_size, fd);
            REQUIRE(size_read == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedUpdateStrideDynamicRSVariable",
          "[process=1][operation=batched_write]"
          "[request_size=type-variable][repetition=1024]"
          "[pattern=stride_dynamic][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    long stride_size = 4*1024;
    unsigned int seed = 1024, request_size_seed = 200;

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
    REQUIRE(total_size > 0);
    SECTION("read from existing file") {
        FILE* fd = fopen(existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        for (int i = 0; i < num_iterations; ++i) {
            auto offset = abs(((i * rand_r(&seed)) % stride_size) % total_size);
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = args.request_size +
                                (rand_r(&request_size_seed)
                                 % args.request_size);
            std::string data(request_size, '1');
            long size_written = fwrite(data.c_str(),
                                       sizeof(char), request_size, fd);
            REQUIRE(size_written == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedReadStrideNegative",
          "[process=1][operation=batched_read]"
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=stride_negative][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    long stride_size = 4*1024;

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
        long prev_offset = total_size + 1;
        for (int i = 0; i < num_iterations; ++i) {
            auto stride_offset = total_size - i * stride_size;
            REQUIRE(prev_offset > stride_offset);
            prev_offset = stride_offset;
            auto offset = (stride_offset)
                        % (total_size - args.request_size);
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

TEST_CASE("BatchedUpdateStrideNegative",
          "[process=1][operation=batched_write]"
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=stride_negative][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    long stride_size = 4*1024;

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
            auto offset = total_size - ((i * stride_size) % total_size);
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

TEST_CASE("BatchedReadStrideNegativeRSVariable",
          "[process=1][operation=batched_read]"
          "[request_size=type-variable][repetition=1024]"
          "[pattern=stride_negative][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    long stride_size = 4*1024;
    unsigned int request_size_seed = 200;

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
        for (int i = 0; i < num_iterations; ++i) {
            auto offset = (total_size - i * stride_size)
                        % (total_size - 2*args.request_size);
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = (args.request_size +
                                 (rand_r(&request_size_seed)
                                  % args.request_size))
                                % (total_size - offset);
            std::string data(request_size, '1');
            long size_read = fread(data.data(),
                                   sizeof(char), request_size, fd);
            REQUIRE(size_read == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedUpdateStrideNegativeRSVariable",
          "[process=1][operation=batched_write]"
          "[request_size=type-variable][repetition=1024]"
          "[pattern=stride_negative][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    long stride_size = 4*1024;
    unsigned int request_size_seed = 200;

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

    SECTION("write to existing file") {
        FILE* fd = fopen(existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        std::string data(args.request_size, '1');
        for (int i = 0; i < num_iterations; ++i) {
            auto offset = total_size - ((i * stride_size) % total_size);
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = args.request_size +
                                (rand_r(&request_size_seed)
                                 % args.request_size);
            std::string data(request_size, '1');
            long size_written = fwrite(data.c_str(),
                                       sizeof(char), request_size, fd);
            REQUIRE(size_written == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedReadStride2D",
          "[process=1][operation=batched_read]"
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=stride_2d][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    long stride_size = 4*1024;

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
    long rows = sqrt(total_size);
    long cols = rows;
    REQUIRE(rows * cols == total_size);
    long cell_size = 128;
    long cell_stride = rows * cols/ cell_size / num_iterations;
    SECTION("read from existing file") {
        FILE* fd = fopen(existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        std::string data(args.request_size, '1');
        long prev_cell_col = 0, prev_cell_row = 0;
        for (int i = 0; i < num_iterations; ++i) {
            long current_cell_col = (prev_cell_col +  cell_stride) % cols;
            long current_cell_row = prev_cell_col + cell_stride > cols ?
                    prev_cell_row + 1: prev_cell_row;
            prev_cell_row = current_cell_row;
            auto offset = (current_cell_col*cell_stride + prev_cell_row*cols)
                          % (total_size - args.request_size);
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

TEST_CASE("BatchedUpdateStride2D",
          "[process=1][operation=batched_write]"
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=stride_2d][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    long stride_size = 4*1024;

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
    long rows = sqrt(total_size);
    long cols = rows;
    REQUIRE(rows * cols == total_size);
    long cell_size = 128;
    long cell_stride = rows * cols/ cell_size / num_iterations;
    SECTION("read from existing file") {
        FILE* fd = fopen(existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        std::string data(args.request_size, '1');
        long prev_cell_col = 0, prev_cell_row = 0;
        for (int i = 0; i < num_iterations; ++i) {
            long current_cell_col = (prev_cell_col +  cell_stride) % cols;
            long current_cell_row = prev_cell_col + cell_stride > cols ?
                                    prev_cell_row + 1: prev_cell_row;
            prev_cell_row = current_cell_row;
            auto offset = (current_cell_col*cell_stride + prev_cell_row*cols)
                          % (total_size - args.request_size);
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

TEST_CASE("BatchedReadStride2DRSVariable",
          "[process=1][operation=batched_read]"
          "[request_size=type-variable][repetition=1024]"
          "[pattern=stride_2d][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    long stride_size = 4*1024;
    unsigned int request_size_seed = 200;

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
    long rows = sqrt(total_size);
    long cols = rows;
    REQUIRE(rows * cols == total_size);
    long cell_size = 128;
    long cell_stride = rows * cols/ cell_size / num_iterations;
    SECTION("read from existing file") {
        FILE* fd = fopen(existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        long prev_cell_col = 0, prev_cell_row = 0;
        for (int i = 0; i < num_iterations; ++i) {
            long current_cell_col = (prev_cell_col +  cell_stride) % cols;
            long current_cell_row = prev_cell_col + cell_stride > cols ?
                                    prev_cell_row + 1: prev_cell_row;
            prev_cell_row = current_cell_row;
            auto offset = (current_cell_col*cell_stride + prev_cell_row*cols)
                          % (total_size - 2*args.request_size);
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = (args.request_size +
                                 (rand_r(&request_size_seed)
                                  % args.request_size))
                                % (total_size - offset);
            std::string data(request_size, '1');
            long size_read = fread(data.data(),
                                   sizeof(char), request_size, fd);
            REQUIRE(size_read == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedUpdateStride2DRSVariable",
          "[process=1][operation=batched_write]"
          "[request_size=type-variable][repetition=1024]"
          "[pattern=stride_2d][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    long stride_size = 4*1024;
    unsigned int request_size_seed = 200;

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
    long rows = sqrt(total_size);
    long cols = rows;
    REQUIRE(rows * cols == total_size);
    long cell_size = 128;
    long cell_stride = rows * cols/ cell_size / num_iterations;
    SECTION("write to existing file") {
        FILE* fd = fopen(existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        std::string data(args.request_size, '1');
        long prev_cell_col = 0, prev_cell_row = 0;
        for (int i = 0; i < num_iterations; ++i) {
            long current_cell_col = (prev_cell_col +  cell_stride) % cols;
            long current_cell_row = prev_cell_col + cell_stride > cols ?
                                    prev_cell_row + 1: prev_cell_row;
            prev_cell_row = current_cell_row;
            auto offset = (current_cell_col*cell_stride + prev_cell_row*cols)
                          % (total_size - 2*args.request_size);
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = args.request_size +
                                (rand_r(&request_size_seed)
                                 % args.request_size);
            std::string data(request_size, '1');
            long size_written = fwrite(data.c_str(),
                                       sizeof(char), request_size, fd);
            REQUIRE(size_written == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}
/**
 * RS Range
 */

TEST_CASE("BatchedWriteRSRangeSmall",
          "[process=1][operation=batched_write]"
          "[request_size=range-small][repetition=1024]"
          "[pattern=sequential][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string new_file = fullpath.string() + "_new";
    if (fs::exists(new_file)) fs::remove(new_file);
    long num_iterations = 1024;
    unsigned int request_size_seed = 1024;
    long min_size = 1, max_size = 4096;
    SECTION("write to new file always at the start") {
        FILE* fd = fopen(new_file.c_str(), "w+");
        REQUIRE(fd != nullptr);
        long biggest_size_written = 0;
        for (int i = 0; i < num_iterations; ++i) {
            int status = fseek(fd, 0, SEEK_SET);
            REQUIRE(status == 0);
            long offset = ftell(fd);
            REQUIRE(offset == 0);
            long request_size = min_size +
                                (rand_r(&request_size_seed) % max_size);
            std::string data(request_size, '1');
            long size_written = fwrite(data.c_str(),
                                       sizeof(char), request_size, fd);
            REQUIRE(size_written == request_size);
            if (biggest_size_written < request_size)
                biggest_size_written = request_size;
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
        REQUIRE(fs::file_size(new_file) == biggest_size_written);
    }

    SECTION("write to new file") {
        FILE* fd = fopen(new_file.c_str(), "w+");
        REQUIRE(fd != nullptr);
        long total_size_written = 0;
        for (int i = 0; i < num_iterations; ++i) {
            long request_size = min_size +
                                (rand_r(&request_size_seed) % max_size);
            std::string data(request_size, '1');
            long size_written = fwrite(data.c_str(),
                                       sizeof(char), request_size, fd);
            REQUIRE(size_written == request_size);
            total_size_written += size_written;
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
        REQUIRE(fs::file_size(new_file) == total_size_written);
    }
    fs::remove(new_file);
}


TEST_CASE("BatchedReadSequentialRSRangeSmall",
          "[process=1][operation=batched_read]"
          "[request_size=range-small][repetition=1024]"
          "[pattern=sequential][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    unsigned int request_size_seed = 1024;
    long min_size = 1, max_size = 4096;

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
        FILE* fd = fopen(existing_file.c_str(), "r");
        REQUIRE(fd != nullptr);
        std::string data(args.request_size, '1');
        long current_offset = 0;
        for (int i = 0; i < num_iterations; ++i) {
            long request_size = min_size +
                                 (rand_r(&request_size_seed) % max_size);
            std::string data(request_size, '1');
            long size_read = fread(data.data(),
                                   sizeof(char), request_size, fd);
            REQUIRE(size_read == request_size);
            current_offset += size_read;
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }

    SECTION("read from existing file always at start") {
        FILE* fd = fopen(existing_file.c_str(), "r");
        REQUIRE(fd != nullptr);

        for (int i = 0; i < num_iterations; ++i) {
            int status = fseek(fd, 0, SEEK_SET);
            REQUIRE(status == 0);
            long offset = ftell(fd);
            REQUIRE(offset == 0);
            long request_size = min_size +
                                (rand_r(&request_size_seed) % max_size);
            std::string data(request_size, '1');
            long size_read = fread(data.data(),
                                   sizeof(char), request_size, fd);
            REQUIRE(size_read == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedReadRandomRSRangeSmall",
          "[process=1][operation=batched_read]"
          "[request_size=range-small]"
          "[repetition=1024][pattern=random][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    unsigned int seed = 1024, request_size_seed = 200;
    long min_size = 1, max_size = 4096;
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
        for (int i = 0; i < num_iterations; ++i) {
            auto offset = rand_r(&seed) % (total_size - max_size);
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = min_size +
                                 (rand_r(&request_size_seed)
                                  % max_size);
            std::string data(request_size, '1');
            long size_read = fread(data.data(),
                                   sizeof(char), request_size, fd);
            REQUIRE(size_read == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedUpdateRandomRSRangeSmall",
          "[process=1][operation=batched_write]"
          "[request_size=range-small][repetition=1024]"
          "[pattern=random][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    unsigned int seed = 1024, request_size_seed = 200;
    long min_size = 1, max_size = 4096;

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
        for (int i = 0; i < num_iterations; ++i) {
            auto offset = rand_r(&seed) % (total_size - max_size);
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = min_size +
                                (rand_r(&request_size_seed) % max_size);
            std::string data(request_size, '1');
            long size_written = fwrite(data.c_str(),
                                       sizeof(char), request_size, fd);
            REQUIRE(size_written == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedReadStrideFixedRSRangeSmall",
          "[process=1][operation=batched_read]"
          "[request_size=range-small][repetition=1024]"
          "[pattern=stride_fixed][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    long stride_size = 4*1024;
    unsigned int request_size_seed = 200;
    long min_size = 1, max_size = 4096;
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
        for (int i = 0; i < num_iterations; ++i) {
            auto offset = (i * stride_size) % (total_size - max_size);
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = min_size +
                                 (rand_r(&request_size_seed)
                                  % max_size);
            std::string data(request_size, '1');
            long size_read = fread(data.data(),
                                   sizeof(char), request_size, fd);
            REQUIRE(size_read == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedUpdateStrideFixedRSRangeSmall",
          "[process=1][operation=batched_write]"
          "[request_size=range-small][repetition=1024]"
          "[pattern=stride_fixed][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    long stride_size = 4*1024;
    unsigned int request_size_seed = 200;
    long min_size = 1, max_size = 4096;

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

    SECTION("write to existing file") {
        FILE* fd = fopen(existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        for (int i = 0; i < num_iterations; ++i) {
            auto offset = (i * stride_size) % (total_size - max_size);
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = min_size +
                                (rand_r(&request_size_seed) % max_size);
            std::string data(request_size, '1');
            long size_written = fwrite(data.c_str(),
                                       sizeof(char), request_size, fd);
            REQUIRE(size_written == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedReadStrideDynamicRSRangeSmall",
          "[process=1][operation=batched_read]"
          "[request_size=range-small][repetition=1024]"
          "[pattern=stride_dynamic][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    unsigned int seed = 1024, request_size_seed = 200;;
    long stride_size = 4*1024;
    long min_size = 1, max_size = 4096;

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
        for (int i = 0; i < num_iterations; ++i) {
            auto offset = abs(((i * rand_r(&seed)) % stride_size)
                            % (total_size- max_size));
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = min_size +
                                (rand_r(&request_size_seed) % max_size);
            std::string data(request_size, '1');
            long size_read = fread(data.data(),
                                   sizeof(char), request_size, fd);
            REQUIRE(size_read == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedUpdateStrideDynamicRSRangeSmall",
          "[process=1][operation=batched_write]"
          "[request_size=range-small][repetition=1024]"
          "[pattern=stride_dynamic][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    long stride_size = 4*1024;
    unsigned int seed = 1024, request_size_seed = 200;
    long min_size = 1, max_size = 4096;

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
    REQUIRE(total_size > 0);
    SECTION("read from existing file") {
        FILE* fd = fopen(existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        for (int i = 0; i < num_iterations; ++i) {
            auto offset = abs(((i * rand_r(&seed)) % stride_size)
                    % (total_size - max_size));
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = min_size +
                                (rand_r(&request_size_seed) % max_size);
            std::string data(request_size, '1');
            long size_written = fwrite(data.c_str(),
                                       sizeof(char), request_size, fd);
            REQUIRE(size_written == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedReadStrideNegativeRSRangeSmall",
          "[process=1][operation=batched_read]"
          "[request_size=range-small][repetition=1024]"
          "[pattern=stride_negative][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    long stride_size = 4*1024;
    unsigned int request_size_seed = 200;
    long min_size = 1, max_size = 4096;

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
        for (int i = 0; i < num_iterations; ++i) {
            auto offset = (total_size - i * stride_size)
                          % (total_size - max_size);
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = min_size +
                                 (rand_r(&request_size_seed) % max_size);
            std::string data(request_size, '1');
            long size_read = fread(data.data(),
                                   sizeof(char), request_size, fd);
            REQUIRE(size_read == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedUpdateStrideNegativeRSRangeSmall",
          "[process=1][operation=batched_write]"
          "[request_size=range-small][repetition=1024]"
          "[pattern=stride_negative][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    long stride_size = 4*1024;
    unsigned int request_size_seed = 200;
    long min_size = 1, max_size = 4096;

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

    SECTION("write to existing file") {
        FILE* fd = fopen(existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        for (int i = 0; i < num_iterations; ++i) {
            auto offset = total_size - ((i * stride_size)
                    % (total_size - max_size));
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = min_size +
                                (rand_r(&request_size_seed) % max_size);
            std::string data(request_size, '1');
            long size_written = fwrite(data.c_str(),
                                       sizeof(char), request_size, fd);
            REQUIRE(size_written == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedReadStride2DRSRangeSmall",
          "[process=1][operation=batched_read]"
          "[request_size=range-small][repetition=1024]"
          "[pattern=stride_2d][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    long stride_size = 4*1024;
    unsigned int request_size_seed = 200;
    long min_size = 1, max_size = 4096;

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
    long rows = sqrt(total_size);
    long cols = rows;
    REQUIRE(rows * cols == total_size);
    long cell_size = 128;
    long cell_stride = rows * cols/ cell_size / num_iterations;
    SECTION("read from existing file") {
        FILE* fd = fopen(existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        long prev_cell_col = 0, prev_cell_row = 0;
        for (int i = 0; i < num_iterations; ++i) {
            long current_cell_col = (prev_cell_col +  cell_stride) % cols;
            long current_cell_row = prev_cell_col + cell_stride > cols ?
                                    prev_cell_row + 1: prev_cell_row;
            prev_cell_row = current_cell_row;
            auto offset = (current_cell_col*cell_stride + prev_cell_row*cols)
                          % (total_size - max_size);
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = min_size +
                                 (rand_r(&request_size_seed)
                                  % max_size);
            std::string data(request_size, '1');
            long size_read = fread(data.data(),
                                   sizeof(char), request_size, fd);
            REQUIRE(size_read == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedUpdateStride2DRSRangeSmall",
          "[process=1][operation=batched_write]"
          "[request_size=range-small][repetition=1024]"
          "[pattern=stride_2d][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    long stride_size = 4*1024;
    unsigned int request_size_seed = 200;
    long min_size = 1, max_size = 4096;

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
    long rows = sqrt(total_size);
    long cols = rows;
    REQUIRE(rows * cols == total_size);
    long cell_size = 128;
    long cell_stride = rows * cols/ cell_size / num_iterations;
    SECTION("write to existing file") {
        FILE* fd = fopen(existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        long prev_cell_col = 0, prev_cell_row = 0;
        for (int i = 0; i < num_iterations; ++i) {
            long current_cell_col = (prev_cell_col +  cell_stride) % cols;
            long current_cell_row = prev_cell_col + cell_stride > cols ?
                                    prev_cell_row + 1: prev_cell_row;
            prev_cell_row = current_cell_row;
            auto offset = (current_cell_col*cell_stride + prev_cell_row*cols)
                          % (total_size - max_size);
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = min_size +
                                (rand_r(&request_size_seed)
                                 % max_size);
            std::string data(request_size, '1');
            long size_written = fwrite(data.c_str(),
                                       sizeof(char), request_size, fd);
            REQUIRE(size_written == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

/**
 * RS Range Medium
 */

TEST_CASE("BatchedWriteRSRangeMedium",
          "[process=1][operation=batched_write]"
          "[request_size=range-medium][repetition=1024]"
          "[pattern=sequential][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string new_file = fullpath.string() + "_new";
    if (fs::exists(new_file)) fs::remove(new_file);
    long num_iterations = 1024;
    unsigned int request_size_seed = 1024;
    long min_size = 4096 + 1, max_size = 512*1024;
    SECTION("write to new file always at the start") {
        FILE* fd = fopen(new_file.c_str(), "w+");
        REQUIRE(fd != nullptr);
        long biggest_size_written = 0;
        for (int i = 0; i < num_iterations; ++i) {
            int status = fseek(fd, 0, SEEK_SET);
            REQUIRE(status == 0);
            long offset = ftell(fd);
            REQUIRE(offset == 0);
            long request_size = min_size +
                                (rand_r(&request_size_seed) % max_size);
            std::string data(request_size, '1');
            long size_written = fwrite(data.c_str(),
                                       sizeof(char), request_size, fd);
            REQUIRE(size_written == request_size);
            if (biggest_size_written < request_size)
                biggest_size_written = request_size;
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
        REQUIRE(fs::file_size(new_file) == biggest_size_written);
    }

    SECTION("write to new file") {
        FILE* fd = fopen(new_file.c_str(), "w+");
        REQUIRE(fd != nullptr);
        long total_size_written = 0;
        for (int i = 0; i < num_iterations; ++i) {
            long request_size = min_size +
                                (rand_r(&request_size_seed) % max_size);
            std::string data(request_size, '1');
            long size_written = fwrite(data.c_str(),
                                       sizeof(char), request_size, fd);
            REQUIRE(size_written == request_size);
            total_size_written += size_written;
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
        REQUIRE(fs::file_size(new_file) == total_size_written);
    }
    fs::remove(new_file);
}


TEST_CASE("BatchedReadSequentialRSRangeMedium",
          "[process=1][operation=batched_read]"
          "[request_size=range-medium][repetition=1024]"
          "[pattern=sequential][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    unsigned int request_size_seed = 1024;
    long min_size = 4096 + 1, max_size = 512*1024;

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
        FILE* fd = fopen(existing_file.c_str(), "r");
        REQUIRE(fd != nullptr);
        long current_offset = 0;
        for (int i = 0; i < num_iterations; ++i) {
            long request_size = min_size +
                                (rand_r(&request_size_seed) % max_size);
            if (request_size > total_size - current_offset)
                request_size = total_size - current_offset;
            std::string data(request_size, '1');
            long size_read = fread(data.data(),
                                   sizeof(char), request_size, fd);
            REQUIRE(size_read == request_size);
            current_offset += size_read;
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }

    SECTION("read from existing file always at start") {
        FILE* fd = fopen(existing_file.c_str(), "r");
        REQUIRE(fd != nullptr);

        for (int i = 0; i < num_iterations; ++i) {
            int status = fseek(fd, 0, SEEK_SET);
            REQUIRE(status == 0);
            long offset = ftell(fd);
            REQUIRE(offset == 0);
            long request_size = min_size +
                                (rand_r(&request_size_seed) % max_size);
            std::string data(request_size, '1');
            long size_read = fread(data.data(),
                                   sizeof(char), request_size, fd);
            REQUIRE(size_read == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedReadRandomRSRangeMedium",
          "[process=1][operation=batched_read]"
          "[request_size=range-medium]"
          "[repetition=1024][pattern=random][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    unsigned int seed = 1024, request_size_seed = 200;
    long min_size = 4096 + 1, max_size = 512*1024;
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
        for (int i = 0; i < num_iterations; ++i) {
            auto offset = rand_r(&seed) % (total_size - max_size);
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = min_size +
                                (rand_r(&request_size_seed)
                                 % max_size);
            std::string data(request_size, '1');
            long size_read = fread(data.data(),
                                   sizeof(char), request_size, fd);
            REQUIRE(size_read == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedUpdateRandomRSRangeMedium",
          "[process=1][operation=batched_write]"
          "[request_size=range-medium][repetition=1024]"
          "[pattern=random][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    unsigned int seed = 1024, request_size_seed = 200;
    long min_size = 4096 + 1, max_size = 512*1024;

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
        for (int i = 0; i < num_iterations; ++i) {
            auto offset = rand_r(&seed) % (total_size - max_size);
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = min_size +
                                (rand_r(&request_size_seed) % max_size);
            std::string data(request_size, '1');
            long size_written = fwrite(data.c_str(),
                                       sizeof(char), request_size, fd);
            REQUIRE(size_written == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedReadStrideFixedRSRangeMedium",
          "[process=1][operation=batched_read]"
          "[request_size=range-medium][repetition=1024]"
          "[pattern=stride_fixed][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    long stride_size = 4*1024;
    unsigned int request_size_seed = 200;
    long min_size = 4096 + 1, max_size = 512*1024;
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
        for (int i = 0; i < num_iterations; ++i) {
            auto offset = (i * stride_size) % (total_size - max_size);
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = min_size +
                                (rand_r(&request_size_seed)
                                 % max_size);
            std::string data(request_size, '1');
            long size_read = fread(data.data(),
                                   sizeof(char), request_size, fd);
            REQUIRE(size_read == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedUpdateStrideFixedRSRangeMedium",
          "[process=1][operation=batched_write]"
          "[request_size=range-medium][repetition=1024]"
          "[pattern=stride_fixed][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    long stride_size = 4*1024;
    unsigned int request_size_seed = 200;
    long min_size = 4096 + 1, max_size = 512*1024;

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

    SECTION("write to existing file") {
        FILE* fd = fopen(existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        for (int i = 0; i < num_iterations; ++i) {
            auto offset = (i * stride_size) % (total_size - max_size);
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = min_size +
                                (rand_r(&request_size_seed) % max_size);
            std::string data(request_size, '1');
            long size_written = fwrite(data.c_str(),
                                       sizeof(char), request_size, fd);
            REQUIRE(size_written == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedReadStrideDynamicRSRangeMedium",
          "[process=1][operation=batched_read]"
          "[request_size=range-medium][repetition=1024]"
          "[pattern=stride_dynamic][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    unsigned int seed = 1024, request_size_seed = 200;;
    long stride_size = 4*1024;
    long min_size = 4096 + 1, max_size = 512*1024;

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
        for (int i = 0; i < num_iterations; ++i) {
            auto offset = abs(((i * rand_r(&seed)) % stride_size)
                              % (total_size- max_size));
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = min_size +
                                (rand_r(&request_size_seed) % max_size);
            std::string data(request_size, '1');
            long size_read = fread(data.data(),
                                   sizeof(char), request_size, fd);
            REQUIRE(size_read == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedUpdateStrideDynamicRSRangeMedium",
          "[process=1][operation=batched_write]"
          "[request_size=range-medium][repetition=1024]"
          "[pattern=stride_dynamic][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    long stride_size = 4*1024;
    unsigned int seed = 1024, request_size_seed = 200;
    long min_size = 4096 + 1, max_size = 512*1024;

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
    REQUIRE(total_size > 0);
    SECTION("read from existing file") {
        FILE* fd = fopen(existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        for (int i = 0; i < num_iterations; ++i) {
            auto offset = abs(((i * rand_r(&seed)) % stride_size)
                              % (total_size - max_size));
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = min_size +
                                (rand_r(&request_size_seed) % max_size);
            std::string data(request_size, '1');
            long size_written = fwrite(data.c_str(),
                                       sizeof(char), request_size, fd);
            REQUIRE(size_written == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedReadStrideNegativeRSRangeMedium",
          "[process=1][operation=batched_read]"
          "[request_size=range-medium][repetition=1024]"
          "[pattern=stride_negative][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    long stride_size = 4*1024;
    unsigned int request_size_seed = 200;
    long min_size = 4096 + 1, max_size = 512*1024;

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
        for (int i = 0; i < num_iterations; ++i) {
            auto offset = (total_size - i * stride_size)
                          % (total_size - max_size);
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = min_size +
                                (rand_r(&request_size_seed) % max_size);
            std::string data(request_size, '1');
            long size_read = fread(data.data(),
                                   sizeof(char), request_size, fd);
            REQUIRE(size_read == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedUpdateStrideNegativeRSRangeMedium",
          "[process=1][operation=batched_write]"
          "[request_size=range-medium][repetition=1024]"
          "[pattern=stride_negative][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    long stride_size = 4*1024;
    unsigned int request_size_seed = 200;
    long min_size = 4096 + 1, max_size = 512*1024;

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

    SECTION("write to existing file") {
        FILE* fd = fopen(existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        for (int i = 0; i < num_iterations; ++i) {
            auto offset = total_size - ((i * stride_size)
                                        % (total_size - max_size));
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = min_size +
                                (rand_r(&request_size_seed) % max_size);
            std::string data(request_size, '1');
            long size_written = fwrite(data.c_str(),
                                       sizeof(char), request_size, fd);
            REQUIRE(size_written == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedReadStride2DRSRangeMedium",
          "[process=1][operation=batched_read]"
          "[request_size=range-medium][repetition=1024]"
          "[pattern=stride_2d][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    long stride_size = 4*1024;
    unsigned int request_size_seed = 200;
    long min_size = 4096 + 1, max_size = 512*1024;

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
    long rows = sqrt(total_size);
    long cols = rows;
    REQUIRE(rows * cols == total_size);
    long cell_size = 128;
    long cell_stride = rows * cols/ cell_size / num_iterations;
    SECTION("read from existing file") {
        FILE* fd = fopen(existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        long prev_cell_col = 0, prev_cell_row = 0;
        for (int i = 0; i < num_iterations; ++i) {
            long current_cell_col = (prev_cell_col +  cell_stride) % cols;
            long current_cell_row = prev_cell_col + cell_stride > cols ?
                                    prev_cell_row + 1: prev_cell_row;
            prev_cell_row = current_cell_row;
            auto offset = (current_cell_col*cell_stride + prev_cell_row*cols)
                          % (total_size - max_size);
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = min_size +
                                (rand_r(&request_size_seed)
                                 % max_size);
            std::string data(request_size, '1');
            long size_read = fread(data.data(),
                                   sizeof(char), request_size, fd);
            REQUIRE(size_read == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedUpdateStride2DRSRangeMedium",
          "[process=1][operation=batched_write]"
          "[request_size=range-medium][repetition=1024]"
          "[pattern=stride_2d][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    long stride_size = 4*1024;
    unsigned int request_size_seed = 200;
    long min_size = 4096 + 1, max_size = 512*1024;

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
    long rows = sqrt(total_size);
    long cols = rows;
    REQUIRE(rows * cols == total_size);
    long cell_size = 128;
    long cell_stride = rows * cols/ cell_size / num_iterations;
    SECTION("write to existing file") {
        FILE* fd = fopen(existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        long prev_cell_col = 0, prev_cell_row = 0;
        for (int i = 0; i < num_iterations; ++i) {
            long current_cell_col = (prev_cell_col +  cell_stride) % cols;
            long current_cell_row = prev_cell_col + cell_stride > cols ?
                                    prev_cell_row + 1: prev_cell_row;
            prev_cell_row = current_cell_row;
            auto offset = (current_cell_col*cell_stride + prev_cell_row*cols)
                          % (total_size - max_size);
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = min_size +
                                (rand_r(&request_size_seed)
                                 % max_size);
            std::string data(request_size, '1');
            long size_written = fwrite(data.c_str(),
                                       sizeof(char), request_size, fd);
            REQUIRE(size_written == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

/**
 * RS Range Large
 */

TEST_CASE("BatchedWriteRSRangeLarge",
          "[process=1][operation=batched_write]"
          "[request_size=range-large][repetition=1024]"
          "[pattern=sequential][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string new_file = fullpath.string() + "_new";
    if (fs::exists(new_file)) fs::remove(new_file);
    long num_iterations = 1024;
    unsigned int request_size_seed = 1024;
    long min_size = 512*1024 + 1, max_size = 4*1024*1024;
    SECTION("write to new file always at the start") {
        FILE* fd = fopen(new_file.c_str(), "w+");
        REQUIRE(fd != nullptr);
        long biggest_size_written = 0;
        for (int i = 0; i < num_iterations; ++i) {
            int status = fseek(fd, 0, SEEK_SET);
            REQUIRE(status == 0);
            long offset = ftell(fd);
            REQUIRE(offset == 0);
            long request_size = min_size +
                                (rand_r(&request_size_seed) % max_size);
            std::string data(request_size, '1');
            long size_written = fwrite(data.c_str(),
                                       sizeof(char), request_size, fd);
            REQUIRE(size_written == request_size);
            if (biggest_size_written < request_size)
                biggest_size_written = request_size;
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
        REQUIRE(fs::file_size(new_file) == biggest_size_written);
    }

    SECTION("write to new file") {
        FILE* fd = fopen(new_file.c_str(), "w+");
        REQUIRE(fd != nullptr);
        long total_size_written = 0;
        for (int i = 0; i < num_iterations; ++i) {
            long request_size = min_size +
                                (rand_r(&request_size_seed) % max_size);
            std::string data(request_size, '1');
            long size_written = fwrite(data.c_str(),
                                       sizeof(char), request_size, fd);
            REQUIRE(size_written == request_size);
            total_size_written += size_written;
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
        REQUIRE(fs::file_size(new_file) == total_size_written);
    }
    fs::remove(new_file);
}


TEST_CASE("BatchedReadSequentialRSRangeLarge",
          "[process=1][operation=batched_read]"
          "[request_size=range-large][repetition=1024]"
          "[pattern=sequential][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    unsigned int request_size_seed = 1024;
    long min_size = 512*1024 + 1, max_size = 4*1024*1024;

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
        FILE* fd = fopen(existing_file.c_str(), "r");
        REQUIRE(fd != nullptr);
        long current_offset = 0;
        for (int i = 0; i < num_iterations; ++i) {
            long request_size = min_size +
                                (rand_r(&request_size_seed) % max_size);
            if (request_size > total_size - current_offset)
                request_size = total_size - current_offset;
            std::string data(request_size, '1');
            long size_read = fread(data.data(),
                                   sizeof(char), request_size, fd);
            REQUIRE(size_read == request_size);
            current_offset += size_read;
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }

    SECTION("read from existing file always at start") {
        FILE* fd = fopen(existing_file.c_str(), "r");
        REQUIRE(fd != nullptr);

        for (int i = 0; i < num_iterations; ++i) {
            int status = fseek(fd, 0, SEEK_SET);
            REQUIRE(status == 0);
            long offset = ftell(fd);
            REQUIRE(offset == 0);
            long request_size = min_size +
                                (rand_r(&request_size_seed) % max_size);
            if (request_size > total_size - offset)
                total_size = total_size - offset;
            std::string data(request_size, '1');
            long size_read = fread(data.data(),
                                   sizeof(char), request_size, fd);
            REQUIRE(size_read == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedReadRandomRSRangeLarge",
          "[process=1][operation=batched_read]"
          "[request_size=range-large]"
          "[repetition=1024][pattern=random][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    unsigned int seed = 1024, request_size_seed = 200;
    long min_size = 512*1024 + 1, max_size = 4*1024*1024;
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
        for (int i = 0; i < num_iterations; ++i) {
            auto offset = rand_r(&seed) % (total_size - max_size);
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = min_size +
                                (rand_r(&request_size_seed)
                                 % max_size);
            if (request_size > total_size - offset)
                request_size = total_size - offset;
            std::string data(request_size, '1');
            long size_read = fread(data.data(),
                                   sizeof(char), request_size, fd);
            REQUIRE(size_read == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedUpdateRandomRSRangeLarge",
          "[process=1][operation=batched_write]"
          "[request_size=range-large][repetition=1024]"
          "[pattern=random][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    unsigned int seed = 1024, request_size_seed = 200;
    long min_size = 512*1024 + 1, max_size = 4*1024*1024;

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
        for (int i = 0; i < num_iterations; ++i) {
            auto offset = rand_r(&seed) % (total_size - max_size);
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = min_size +
                                (rand_r(&request_size_seed) % max_size);
            std::string data(request_size, '1');
            long size_written = fwrite(data.c_str(),
                                       sizeof(char), request_size, fd);
            REQUIRE(size_written == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedReadStrideFixedRSRangeLarge",
          "[process=1][operation=batched_read]"
          "[request_size=range-large][repetition=1024]"
          "[pattern=stride_fixed][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    long stride_size = 4*1024;
    unsigned int request_size_seed = 200;
    long min_size = 512*1024 + 1, max_size = 4*1024*1024;
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
        for (int i = 0; i < num_iterations; ++i) {
            auto offset = (i * stride_size) % (total_size - max_size);
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = min_size +
                                (rand_r(&request_size_seed)
                                 % max_size);
            std::string data(request_size, '1');
            long size_read = fread(data.data(),
                                   sizeof(char), request_size, fd);
            REQUIRE(size_read == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedUpdateStrideFixedRSRangeLarge",
          "[process=1][operation=batched_write]"
          "[request_size=range-large][repetition=1024]"
          "[pattern=stride_fixed][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    long stride_size = 4*1024;
    unsigned int request_size_seed = 200;
    long min_size = 512*1024 + 1, max_size = 4*1024*1024;

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

    SECTION("write to existing file") {
        FILE* fd = fopen(existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        for (int i = 0; i < num_iterations; ++i) {
            auto offset = (i * stride_size) % (total_size - max_size);
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = min_size +
                                (rand_r(&request_size_seed) % max_size);
            std::string data(request_size, '1');
            long size_written = fwrite(data.c_str(),
                                       sizeof(char), request_size, fd);
            REQUIRE(size_written == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedReadStrideDynamicRSRangeLarge",
          "[process=1][operation=batched_read]"
          "[request_size=range-large][repetition=1024]"
          "[pattern=stride_dynamic][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    unsigned int seed = 1024, request_size_seed = 200;;
    long stride_size = 4*1024;
    long min_size = 512*1024 + 1, max_size = 4*1024*1024;

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
        for (int i = 0; i < num_iterations; ++i) {
            auto offset = abs(((i * rand_r(&seed)) % stride_size)
                              % (total_size- max_size));
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = min_size +
                                (rand_r(&request_size_seed) % max_size);

            if (request_size > total_size - offset)
                request_size = total_size - offset;
            std::string data(request_size, '1');
            long size_read = fread(data.data(),
                                   sizeof(char), request_size, fd);
            REQUIRE(size_read == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedUpdateStrideDynamicRSRangeLarge",
          "[process=1][operation=batched_write]"
          "[request_size=range-large][repetition=1024]"
          "[pattern=stride_dynamic][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    long stride_size = 4*1024;
    unsigned int seed = 1024, request_size_seed = 200;
    long min_size = 512*1024 + 1, max_size = 4*1024*1024;

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
    REQUIRE(total_size > 0);
    SECTION("read from existing file") {
        FILE* fd = fopen(existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        for (int i = 0; i < num_iterations; ++i) {
            auto offset = abs(((i * rand_r(&seed)) % stride_size)
                              % (total_size - max_size));
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = min_size +
                                (rand_r(&request_size_seed) % max_size);
            std::string data(request_size, '1');
            long size_written = fwrite(data.c_str(),
                                       sizeof(char), request_size, fd);
            REQUIRE(size_written == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedReadStrideNegativeRSRangeLarge",
          "[process=1][operation=batched_read]"
          "[request_size=range-large][repetition=1024]"
          "[pattern=stride_negative][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    long stride_size = 4*1024;
    unsigned int request_size_seed = 200;
    long min_size = 512*1024 + 1, max_size = 4*1024*1024;

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
        for (int i = 0; i < num_iterations; ++i) {
            auto offset = (total_size - i * stride_size)
                          % (total_size - max_size);
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = min_size +
                                (rand_r(&request_size_seed) % max_size);

            if (request_size > total_size - offset)
                request_size = total_size - offset;
            std::string data(request_size, '1');
            long size_read = fread(data.data(),
                                   sizeof(char), request_size, fd);
            REQUIRE(size_read == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedUpdateStrideNegativeRSRangeLarge",
          "[process=1][operation=batched_write]"
          "[request_size=range-large][repetition=1024]"
          "[pattern=stride_negative][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    long stride_size = 4*1024;
    unsigned int request_size_seed = 200;
    long min_size = 512*1024 + 1, max_size = 4*1024*1024;

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

    SECTION("write to existing file") {
        FILE* fd = fopen(existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        for (int i = 0; i < num_iterations; ++i) {
            auto offset = total_size - ((i * stride_size)
                                        % (total_size - max_size));
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = min_size +
                                (rand_r(&request_size_seed) % max_size);
            std::string data(request_size, '1');
            long size_written = fwrite(data.c_str(),
                                       sizeof(char), request_size, fd);
            REQUIRE(size_written == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedReadStride2DRSRangeLarge",
          "[process=1][operation=batched_read]"
          "[request_size=range-large][repetition=1024]"
          "[pattern=stride_2d][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    long stride_size = 4*1024;
    unsigned int request_size_seed = 200;
    long min_size = 512*1024 + 1, max_size = 4*1024*1024;

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
    long rows = sqrt(total_size);
    long cols = rows;
    REQUIRE(rows * cols == total_size);
    long cell_size = 128;
    long cell_stride = rows * cols/ cell_size / num_iterations;
    SECTION("read from existing file") {
        FILE* fd = fopen(existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        long prev_cell_col = 0, prev_cell_row = 0;
        for (int i = 0; i < num_iterations; ++i) {
            long current_cell_col = (prev_cell_col +  cell_stride) % cols;
            long current_cell_row = prev_cell_col + cell_stride > cols ?
                                    prev_cell_row + 1: prev_cell_row;
            prev_cell_row = current_cell_row;
            auto offset = (current_cell_col*cell_stride + prev_cell_row*cols)
                          % (total_size - max_size);
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = min_size +
                                (rand_r(&request_size_seed)
                                 % max_size);
            if (request_size > total_size - offset)
                request_size = total_size - offset;
            std::string data(request_size, '1');
            long size_read = fread(data.data(),
                                   sizeof(char), request_size, fd);
            REQUIRE(size_read == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedUpdateStride2DRSRangeRequest",
          "[process=1][operation=batched_write]"
          "[request_size=range-large][repetition=1024]"
          "[pattern=stride_2d][file=1]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    long stride_size = 4*1024;
    unsigned int request_size_seed = 200;
    long min_size = 512*1024 + 1, max_size = 4*1024*1024;

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
    long rows = sqrt(total_size);
    long cols = rows;
    REQUIRE(rows * cols == total_size);
    long cell_size = 128;
    long cell_stride = rows * cols/ cell_size / num_iterations;
    SECTION("write to existing file") {
        FILE* fd = fopen(existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        long prev_cell_col = 0, prev_cell_row = 0;
        for (int i = 0; i < num_iterations; ++i) {
            long current_cell_col = (prev_cell_col +  cell_stride) % cols;
            long current_cell_row = prev_cell_col + cell_stride > cols ?
                                    prev_cell_row + 1: prev_cell_row;
            prev_cell_row = current_cell_row;
            auto offset = (current_cell_col*cell_stride + prev_cell_row*cols)
                          % (total_size - max_size);
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = min_size +
                                (rand_r(&request_size_seed)
                                 % max_size);
            std::string data(request_size, '1');
            long size_written = fwrite(data.c_str(),
                                       sizeof(char), request_size, fd);
            REQUIRE(size_written == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

/**
 * Temporal Fixed
 */

TEST_CASE("BatchedWriteTemporalFixed",
          "[process=1][operation=batched_write]"
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=sequential][file=1][temporal=fixed]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string new_file = fullpath.string() + "_new";
    if (fs::exists(new_file)) fs::remove(new_file);
    long num_iterations = 1024;
    long temporal_interval_ms = 100;

    SECTION("write to existing file") {
        FILE* fd = fopen(new_file.c_str(), "w+");
        REQUIRE(fd != nullptr);

        for (int i = 0; i < num_iterations; ++i) {
            usleep(temporal_interval_ms * 1000);
            int status = fseek(fd, 0, SEEK_SET);
            REQUIRE(status == 0);
            long offset = ftell(fd);
            REQUIRE(offset == 0);
            long size_written = fwrite(info.data.c_str(),
                                       sizeof(char), args.request_size, fd);
            REQUIRE(size_written == args.request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
        REQUIRE(fs::file_size(new_file) == args.request_size);
    }

    SECTION("write to new file always at start") {
        FILE* fd = fopen(new_file.c_str(), "w+");
        REQUIRE(fd != nullptr);

        for (int i = 0; i < num_iterations; ++i) {
            usleep(temporal_interval_ms * 1000);
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

TEST_CASE("BatchedReadSequentialTemporalFixed",
          "[process=1][operation=batched_read]"
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=sequential][file=1][temporal=fixed]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    long temporal_interval_ms = 100;
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
        std::string data(args.request_size, '1');
        for (int i = 0; i < num_iterations; ++i) {
            usleep(temporal_interval_ms * 1000);
            long size_read = fread(data.data(),
                                   sizeof(char), args.request_size, fd);
            REQUIRE(size_read == args.request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }

    SECTION("read from existing file always at start") {
        FILE* fd = fopen(existing_file.c_str(), "w+");
        REQUIRE(fd != nullptr);

        for (int i = 0; i < num_iterations; ++i) {
            usleep(temporal_interval_ms * 1000);
            int status = fseek(fd, 0, SEEK_SET);
            REQUIRE(status == 0);
            long offset = ftell(fd);
            REQUIRE(offset == 0);
            long size_written = fwrite(info.data.c_str(),
                                       sizeof(char), args.request_size, fd);
            REQUIRE(size_written == args.request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}

TEST_CASE("BatchedWriteTemporalVariable",
          "[process=1][operation=batched_write]"
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=sequential][file=1][temporal=variable]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string new_file = fullpath.string() + "_new";
    if (fs::exists(new_file)) fs::remove(new_file);
    long num_iterations = 1024;

    unsigned int temporal_interval_seed = 100;

    SECTION("write to existing file") {
        FILE* fd = fopen(new_file.c_str(), "w+");
        REQUIRE(fd != nullptr);

        for (int i = 0; i < num_iterations; ++i) {
            long temporal_interval_ms = rand_r(&temporal_interval_seed)
                    % 100 + 1;
            usleep(temporal_interval_ms * 1000);
            int status = fseek(fd, 0, SEEK_SET);
            REQUIRE(status == 0);
            long offset = ftell(fd);
            REQUIRE(offset == 0);
            long size_written = fwrite(info.data.c_str(),
                                       sizeof(char), args.request_size, fd);
            REQUIRE(size_written == args.request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
        REQUIRE(fs::file_size(new_file) == args.request_size);
    }

    SECTION("write to new file always at start") {
        FILE* fd = fopen(new_file.c_str(), "w+");
        REQUIRE(fd != nullptr);

        for (int i = 0; i < num_iterations; ++i) {
            long temporal_interval_ms = rand_r(&temporal_interval_seed)
                                        % 100 + 1;
            usleep(temporal_interval_ms * 1000);
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

TEST_CASE("BatchedReadSequentialTemporalVariable",
          "[process=1][operation=batched_read]"
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=sequential][file=1][temporal=variable]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string existing_file = fullpath.string();
    if (fs::exists(existing_file)) fs::remove(existing_file);
    long num_iterations = 1024;
    unsigned int temporal_interval_seed = 100;
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
        std::string data(args.request_size, '1');
        for (int i = 0; i < num_iterations; ++i) {
            long temporal_interval_ms = rand_r(&temporal_interval_seed)
                                        % 100 + 1;
            usleep(temporal_interval_ms * 1000);
            long size_read = fread(data.data(),
                                   sizeof(char), args.request_size, fd);
            REQUIRE(size_read == args.request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }

    SECTION("read from existing file always at start") {
        FILE* fd = fopen(existing_file.c_str(), "w+");
        REQUIRE(fd != nullptr);

        for (int i = 0; i < num_iterations; ++i) {
            long temporal_interval_ms = rand_r(&temporal_interval_seed)
                                        % 100 + 1;
            usleep(temporal_interval_ms * 1000);
            int status = fseek(fd, 0, SEEK_SET);
            REQUIRE(status == 0);
            long offset = ftell(fd);
            REQUIRE(offset == 0);
            long size_written = fwrite(info.data.c_str(),
                                       sizeof(char), args.request_size, fd);
            REQUIRE(size_written == args.request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    fs::remove(existing_file);
}