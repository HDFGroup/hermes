TEST_CASE("Open", "[process="+std::to_string(info.comm_size)+"]"
                  "[operation=single_open]"
                  "[repetition=1][file=1]") {
    pretest();
    SECTION("open non-existant file") {
        int fd = open(info.new_file.c_str(), O_WRONLY);
        REQUIRE(fd == -1);
        fd = open(info.new_file.c_str(), O_RDONLY);
        REQUIRE(fd == -1);
        fd = open(info.new_file.c_str(), O_RDWR);
        REQUIRE(fd == -1);
    }

    SECTION("truncate existing file and write-only") {
        int fd = open(info.existing_file.c_str(),
                      O_WRONLY | O_TRUNC);
        REQUIRE(fd != -1);
        int status = close(fd);
        REQUIRE(status == 0);
    }
    SECTION("truncate existing file and read/write") {
        int fd = open(info.existing_file.c_str(),
                      O_RDWR | O_TRUNC);
        REQUIRE(fd != -1);
        int status = close(fd);
        REQUIRE(status == 0);
    }

    SECTION("open existing file") {
        int fd = open(info.existing_file.c_str(), O_WRONLY);
        REQUIRE(fd != -1);
        int status = close(fd);
        REQUIRE(status == 0);
        fd = open(info.existing_file.c_str(), O_RDONLY);
        REQUIRE(fd != -1);
        status = close(fd);
        REQUIRE(status == 0);
        fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        status = close(fd);
        REQUIRE(status == 0);
    }

    SECTION("append write new file") {
        int fd = open(info.existing_file.c_str(), O_APPEND);
        REQUIRE(fd != -1);
        int status = close(fd);
        REQUIRE(status == 0);
    }

    SECTION("create a new file") {
        int fd = open(info.new_file.c_str(), O_WRONLY | O_CREAT);
        REQUIRE(fd != -1);
        int status = close(fd);
        REQUIRE(status == 0);
        fs::remove(info.new_file);

        fd = open(info.new_file.c_str(), O_RDONLY  | O_CREAT);
        REQUIRE(fd != -1);
        status = close(fd);
        REQUIRE(status == 0);
        fs::remove(info.new_file);

        fd = open(info.new_file.c_str(), O_RDWR | O_CREAT);
        REQUIRE(fd != -1);
        status = close(fd);
        REQUIRE(status == 0);
    }
    SECTION("create a existing file") {
        int fd = open(info.existing_file.c_str(), O_WRONLY | O_CREAT);
        REQUIRE(fd != -1);
        int status = close(fd);
        REQUIRE(status == 0);
        fd = open(info.existing_file.c_str(), O_RDONLY  | O_CREAT);
        REQUIRE(fd != -1);
        status = close(fd);
        REQUIRE(status == 0);
        fd = open(info.existing_file.c_str(), O_RDWR | O_CREAT);
        REQUIRE(fd != -1);
        status = close(fd);
        REQUIRE(status == 0);

        fd = open(info.existing_file.c_str(),
                      O_WRONLY | O_CREAT | O_EXCL);
        REQUIRE(fd == -1);
        fd = open(info.existing_file.c_str(),
                  O_RDONLY | O_CREAT | O_EXCL);
        REQUIRE(fd == -1);
        fd = open(info.existing_file.c_str(),
                  O_RDWR | O_CREAT | O_EXCL);
        REQUIRE(fd == -1);
    }
    SECTION("Async I/O") {
        int fd = open(info.existing_file.c_str(), O_WRONLY | O_ASYNC);
        REQUIRE(fd != -1);
        int status = close(fd);
        REQUIRE(status == 0);
        fd = open(info.existing_file.c_str(), O_RDONLY | O_ASYNC);
        REQUIRE(fd != -1);
        status = close(fd);
        REQUIRE(status == 0);
        fd = open(info.existing_file.c_str(), O_RDWR | O_ASYNC);
        REQUIRE(fd != -1);
        status = close(fd);
        REQUIRE(status == 0);
        fd = open(info.existing_file.c_str(), O_APPEND | O_ASYNC);
        REQUIRE(fd != -1);
        status = close(fd);
        REQUIRE(status == 0);

        fd = open(info.existing_file.c_str(), O_WRONLY | O_NONBLOCK);
        REQUIRE(fd != -1);
        status = close(fd);
        REQUIRE(status == 0);
        fd = open(info.existing_file.c_str(), O_RDONLY | O_NONBLOCK);
        REQUIRE(fd != -1);
        status = close(fd);
        REQUIRE(status == 0);
        fd = open(info.existing_file.c_str(), O_RDWR | O_NONBLOCK);
        REQUIRE(fd != -1);
        status = close(fd);
        REQUIRE(status == 0);
        fd = open(info.existing_file.c_str(), O_APPEND | O_NONBLOCK);
        REQUIRE(fd != -1);
        status = close(fd);
        REQUIRE(status == 0);

        fd = open(info.existing_file.c_str(), O_WRONLY | O_NDELAY);
        REQUIRE(fd != -1);
        status = close(fd);
        REQUIRE(status == 0);
        fd = open(info.existing_file.c_str(), O_RDONLY | O_NDELAY);
        REQUIRE(fd != -1);
        status = close(fd);
        REQUIRE(status == 0);
        fd = open(info.existing_file.c_str(), O_RDWR | O_NDELAY);
        REQUIRE(fd != -1);
        status = close(fd);
        REQUIRE(status == 0);
        fd = open(info.existing_file.c_str(), O_APPEND | O_NDELAY);
        REQUIRE(fd != -1);
        status = close(fd);
        REQUIRE(status == 0);
    }
    SECTION("Async I/O") {
        int fd = open(info.existing_file.c_str(), O_WRONLY | O_DIRECT);
        REQUIRE(fd != -1);
        int status = close(fd);
        REQUIRE(status == 0);
        fd = open(info.existing_file.c_str(), O_RDONLY | O_DIRECT);
        REQUIRE(fd != -1);
        status = close(fd);
        REQUIRE(status == 0);
        fd = open(info.existing_file.c_str(), O_RDWR | O_DIRECT);
        REQUIRE(fd != -1);
        status = close(fd);
        REQUIRE(status == 0);
        fd = open(info.existing_file.c_str(), O_APPEND | O_DIRECT);
        REQUIRE(fd != -1);
        status = close(fd);
        REQUIRE(status == 0);
    }
    SECTION("Write Synchronize") {
        /* File synchronicity */
        int fd = open(info.existing_file.c_str(), O_WRONLY | O_DSYNC);
        REQUIRE(fd != -1);
        int status = close(fd);
        REQUIRE(status == 0);
        fd = open(info.existing_file.c_str(), O_RDONLY | O_DSYNC);
        REQUIRE(fd != -1);
        status = close(fd);
        REQUIRE(status == 0);
        fd = open(info.existing_file.c_str(), O_RDWR | O_DSYNC);
        REQUIRE(fd != -1);
        status = close(fd);
        REQUIRE(status == 0);
        fd = open(info.existing_file.c_str(), O_APPEND | O_DSYNC);
        REQUIRE(fd != -1);
        status = close(fd);
        REQUIRE(status == 0);

        /* Write synchronicity */
        fd = open(info.existing_file.c_str(), O_WRONLY | O_SYNC);
        REQUIRE(fd != -1);
        status = close(fd);
        REQUIRE(status == 0);
        fd = open(info.existing_file.c_str(), O_RDONLY | O_SYNC);
        REQUIRE(fd != -1);
        status = close(fd);
        REQUIRE(status == 0);
        fd = open(info.existing_file.c_str(), O_RDWR | O_SYNC);
        REQUIRE(fd != -1);
        status = close(fd);
        REQUIRE(status == 0);
        fd = open(info.existing_file.c_str(), O_APPEND | O_SYNC);
        REQUIRE(fd != -1);
        status = close(fd);
        REQUIRE(status == 0);
    }
    SECTION("Temporary file") {
        int fd = open("/tmp", O_WRONLY | O_TMPFILE);
        REQUIRE(fd != -1);
        int status = close(fd);
        REQUIRE(status == 0);
        fd = open(info.new_file.c_str(), O_RDONLY | O_TMPFILE);
        REQUIRE(fd == -1);
        fd = open(info.new_file.c_str(), O_RDWR | O_TMPFILE);
        REQUIRE(fd == -1);
        fd = open(info.new_file.c_str(), O_APPEND | O_TMPFILE);
        REQUIRE(fd == -1);

        fd = open(info.existing_file.c_str(), O_WRONLY | O_TMPFILE);
        REQUIRE(fd == -1);
        fd = open(info.existing_file.c_str(), O_RDONLY | O_TMPFILE);
        REQUIRE(fd == -1);
        fd = open(info.existing_file.c_str(), O_RDWR | O_TMPFILE);
        REQUIRE(fd == -1);
    }
    posttest();
}

TEST_CASE("SingleWrite",
          "[process="+std::to_string(info.comm_size)+"]"
          "[operation=single_write]"
          "[request_size=type-fixed][repetition=1]"
          "[file=1]") {
    pretest();
    SECTION("write to existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        long offset = lseek(fd, 0, SEEK_SET);
        long size_written = write(fd, info.write_data.c_str(),
                                  args.request_size);
        REQUIRE(size_written == args.request_size);
        int status = close(fd);
        REQUIRE(status == 0);
    }

    SECTION("write to new  file") {
        int fd = open(info.new_file.c_str(), O_WRONLY | O_CREAT | O_EXCL);
        REQUIRE(fd != -1);
        long size_written = write(fd, info.write_data.c_str(),
                                  args.request_size);
        REQUIRE(size_written == args.request_size);
        int status = close(fd);
        REQUIRE(status == 0);
        REQUIRE(fs::file_size(info.new_file) == size_written);
    }

    SECTION("write to existing file with truncate") {
        int fd = open(info.existing_file.c_str(), O_WRONLY | O_TRUNC);
        REQUIRE(fd != -1);
        long size_written = write(fd, info.write_data.c_str(),
                                  args.request_size);
        REQUIRE(size_written == args.request_size);
        int status = close(fd);
        REQUIRE(status == 0);
        REQUIRE(fs::file_size(info.existing_file) == size_written);
    }

    SECTION("write to existing file at the end") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        long offset = lseek(fd, 0, SEEK_END);
        REQUIRE(offset == args.request_size * info.num_iterations);
        long size_written = write(fd, info.write_data.c_str(),
                                  args.request_size);
        REQUIRE(size_written == args.request_size);
        int status = close(fd);
        REQUIRE(status == 0);
        REQUIRE(fs::file_size(info.existing_file) == size_written + offset);
    }

    SECTION("append to existing file") {
        auto existing_size = fs::file_size(info.existing_file);
        int fd = open(info.existing_file.c_str(), O_RDWR | O_APPEND);
        REQUIRE(fd != -1);
        long size_written = write(fd, info.write_data.c_str(),
                                  args.request_size);
        REQUIRE(size_written == args.request_size);
        int status = close(fd);
        REQUIRE(status == 0);
        REQUIRE(fs::file_size(info.existing_file)
                == existing_size + size_written);
    }

    SECTION("append to new file") {
        int fd = open(info.new_file.c_str(), O_WRONLY | O_CREAT | O_EXCL);
        REQUIRE(fd != -1);
        long size_written = write(fd, info.write_data.c_str(),
                                  args.request_size);
        REQUIRE(size_written == args.request_size);
        int status = close(fd);
        REQUIRE(status == 0);
        REQUIRE(fs::file_size(info.new_file) == size_written);
    }
    posttest();
}

TEST_CASE("SingleRead",
          "[process="+std::to_string(info.comm_size)+"]"
          "[operation=single_read]"
          "[request_size=type-fixed][repetition=1]"
          "[file=1]") {
    pretest();
    SECTION("read from non-existing file") {
        int fd = open(info.new_file.c_str(), O_RDONLY);
        REQUIRE(fd == -1);
    }

    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDONLY);
        REQUIRE(fd != -1);
        long offset = lseek(fd, 0, SEEK_CUR);
        REQUIRE(offset == 0);
        long size_read = read(fd, info.read_data.data(),
                                   args.request_size);
        REQUIRE(size_read == args.request_size);
        int status = close(fd);
        REQUIRE(status == 0);
    }
    SECTION("read at the end of existing file") {
        int fd = open(info.existing_file.c_str(), O_RDONLY);
        REQUIRE(fd != -1);
        int offset = lseek(fd, 0, SEEK_END);
        REQUIRE(offset == args.request_size * info.num_iterations);
        long size_read = read(fd, info.read_data.data(),
                                   args.request_size);
        REQUIRE(size_read == 0);
        int status = close(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedWriteSequential",
          "[process="+std::to_string(info.comm_size)+"]"
          "[operation=batched_write]"
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=sequential][file=1]") {
    pretest();
    SECTION("write to existing file") {
        int fd = open(info.new_file.c_str(), O_WRONLY | O_CREAT | O_EXCL);
        REQUIRE(fd != -1);

        for (int i = 0; i < info.num_iterations; ++i) {
            int offset = lseek(fd, 0, SEEK_SET);
            REQUIRE(offset == 0);
            long size_written = write(fd, info.write_data.c_str(),
                                      args.request_size);
            REQUIRE(size_written == args.request_size);
        }
        int status = close(fd);
        REQUIRE(status == 0);
        REQUIRE(fs::file_size(info.new_file) == args.request_size);
    }

    SECTION("write to new file always at start") {
        int fd = open(info.new_file.c_str(), O_WRONLY | O_CREAT | O_EXCL);
        REQUIRE(fd != -1);

        for (int i = 0; i < info.num_iterations; ++i) {
            long size_written = write(fd, info.write_data.c_str(),
                                      args.request_size);
            REQUIRE(size_written == args.request_size);
        }
        int status = close(fd);
        REQUIRE(status == 0);
        REQUIRE(fs::file_size(info.new_file) ==
        info.num_iterations * args.request_size);
    }
    posttest();
}


TEST_CASE("BatchedReadSequential",
          "[process="+std::to_string(info.comm_size)+"]"
          "[operation=batched_read]"
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=sequential][file=1]") {
    pretest();

    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        std::string data(args.request_size, '1');
        for (int i = 0; i < info.num_iterations; ++i) {
            long size_read = read(fd, data.data(),
                                       args.request_size);
            REQUIRE(size_read == args.request_size);
        }
        int status = close(fd);
        REQUIRE(status == 0);
    }

    SECTION("read from existing file always at start") {
        int fd = open(info.existing_file.c_str(), O_WRONLY);
        REQUIRE(fd != -1);

        for (int i = 0; i < info.num_iterations; ++i) {
            int offset = lseek(fd, 0, SEEK_SET);
            REQUIRE(offset == 0);
            long size_written = write(fd, info.write_data.c_str(),
                                      args.request_size);
            REQUIRE(size_written == args.request_size);
        }
        int status = close(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedReadRandom",
          "[process="+std::to_string(info.comm_size)+"]"
          "[operation=batched_read]"
          "[request_size=type-fixed]"
          "[repetition=1024][pattern=random][file=1]") {
    pretest();
    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        std::string data(args.request_size, '1');
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = rand_r(&info.offset_seed)
                    % (info.total_size - args.request_size);
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long size_read = read(fd, data.data(),
                                  args.request_size);
            REQUIRE(size_read == args.request_size);
        }
        int status = close(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedUpdateRandom",
          "[process="+std::to_string(info.comm_size)+"]"
          "[operation=batched_write]"
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=random][file=1]") {
    pretest();
    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        std::string data(args.request_size, '1');
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = rand_r(&info.offset_seed)
                    % (info.total_size - args.request_size);
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long size_read = write(fd, data.data(),
                                   args.request_size);
            REQUIRE(size_read == args.request_size);
        }
        int status = close(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedReadStrideFixed",
          "[process="+std::to_string(info.comm_size)+"]"
          "[operation=batched_read]"
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=stride_fixed][file=1]") {
    pretest();

    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        std::string data(args.request_size, '1');
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = (i * info.stride_size) % info.total_size;
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long size_read = read(fd, data.data(),
                                  args.request_size);
            REQUIRE(size_read == args.request_size);
        }
        int status = close(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedUpdateStrideFixed",
          "[process="+std::to_string(info.comm_size)+"]"
          "[operation=batched_write]"
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=stride_fixed][file=1]") {
    pretest();

    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        std::string data(args.request_size, '1');
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = (i * info.stride_size) % info.total_size;
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long size_read = write(fd, data.data(),
                                   args.request_size);
            REQUIRE(size_read == args.request_size);
        }
        int status = close(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedReadStrideDynamic",
          "[process="+std::to_string(info.comm_size)+"]"
          "[operation=batched_read]"
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=stride_dynamic][file=1]") {
    pretest();

    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        std::string data(args.request_size, '1');
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = abs(((i * rand_r(&info.offset_seed))
                    % info.stride_size) % info.total_size);
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long size_read = read(fd, data.data(),
                                  args.request_size);
            REQUIRE(size_read == args.request_size);
        }
        int status = close(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedUpdateStrideDynamic",
          "[process="+std::to_string(info.comm_size)+"]"
          "[operation=batched_write]"
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=stride_dynamic][file=1]") {
    pretest();
    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        std::string data(args.request_size, '1');
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = abs(((i * rand_r(&info.offset_seed))
                    % info.stride_size) % info.total_size);
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long size_read = write(fd, data.data(),
                                   args.request_size);
            REQUIRE(size_read == args.request_size);
        }
        int status = close(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedWriteRSVariable",
          "[process="+std::to_string(info.comm_size)+"]"
          "[operation=batched_write]"
          "[request_size=type-variable][repetition=1024]"
          "[pattern=sequential][file=1]") {
    pretest();

    SECTION("write to new file always at the start") {
        int fd = open(info.new_file.c_str(), O_WRONLY | O_CREAT | O_EXCL);
        REQUIRE(fd != -1);
        long biggest_size_written = 0;
        for (int i = 0; i < info.num_iterations; ++i) {
            int offset = lseek(fd, 0, SEEK_SET);
            REQUIRE(offset == 0);
            long request_size = args.request_size +
                                (rand_r(&info.offset_seed) % args.request_size);
            std::string data(request_size, '1');
            long size_written = write(fd, data.c_str(),
                                      request_size);
            REQUIRE(size_written == request_size);
            if (biggest_size_written < request_size)
                biggest_size_written = request_size;
        }
        int status = close(fd);
        REQUIRE(status == 0);
        REQUIRE(fs::file_size(info.new_file) == biggest_size_written);
    }

    SECTION("write to new file") {
        int fd = open(info.new_file.c_str(), O_WRONLY | O_CREAT | O_EXCL);
        REQUIRE(fd != -1);
        long total_size_written = 0;
        for (int i = 0; i < info.num_iterations; ++i) {
            long request_size = args.request_size +
                                (rand_r(&info.offset_seed) % args.request_size);
            std::string data(request_size, '1');
            long size_written = write(fd, data.c_str(),
                                      request_size);
            REQUIRE(size_written == request_size);
            total_size_written += size_written;
        }
        int status = close(fd);
        REQUIRE(status == 0);
        REQUIRE(fs::file_size(info.new_file) == total_size_written);
    }
    posttest();
}


TEST_CASE("BatchedReadSequentialRSVariable",
          "[process="+std::to_string(info.comm_size)+"]"
          "[operation=batched_read]"
          "[request_size=type-variable][repetition=1024]"
          "[pattern=sequential][file=1]") {
    pretest();
    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDONLY);
        REQUIRE(fd != -1);
        std::string data(args.request_size, '1');
        long current_offset = 0;
        for (int i = 0; i < info.num_iterations; ++i) {
            long request_size = (args.request_size +
                                (rand_r(&info.offset_seed) % args.request_size))
                                        % (info.total_size - current_offset);
            std::string data(request_size, '1');
            long size_read = read(fd, data.data(),
                                  request_size);
            REQUIRE(size_read == request_size);
            current_offset += size_read;
        }
        int status = close(fd);
        REQUIRE(status == 0);
    }

    SECTION("read from existing file always at start") {
        int fd = open(info.existing_file.c_str(), O_RDONLY);
        REQUIRE(fd != -1);

        for (int i = 0; i < info.num_iterations; ++i) {
            int offset = lseek(fd, 0, SEEK_SET);
            REQUIRE(offset == 0);
            long request_size = args.request_size +
                                (rand_r(&info.offset_seed) % args.request_size);
            std::string data(request_size, '1');
            long size_read = read(fd, data.data(),
                                  request_size);
            REQUIRE(size_read == request_size);
        }
        int status = close(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedReadRandomRSVariable",
          "[process="+std::to_string(info.comm_size)+"]"
          "[operation=batched_read]"
          "[request_size=type-variable]"
          "[repetition=1024][pattern=random][file=1]") {
    pretest();

    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        std::string data(args.request_size, '1');
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = rand_r(&info.offset_seed)
                    % (info.total_size - args.request_size);
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long request_size = (args.request_size +
                                (rand_r(&info.rs_seed)
                                % args.request_size))
                                % (info.total_size - offset);
            std::string data(request_size, '1');
            long size_read = read(fd, data.data(),
                                  request_size);
            REQUIRE(size_read == request_size);
        }
        int status = close(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedUpdateRandomRSVariable",
          "[process="+std::to_string(info.comm_size)+"]"
          "[operation=batched_write]"
          "[request_size=type-variable][repetition=1024]"
          "[pattern=random][file=1]") {
    pretest();

    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        std::string data(args.request_size, '1');
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = rand_r(&info.offset_seed)
                    % (info.total_size - args.request_size);
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long request_size = args.request_size +
                                (rand_r(&info.rs_seed)
                                % args.request_size);
            std::string data(request_size, '1');
            long size_written = write(fd, data.c_str(),
                                      request_size);
            REQUIRE(size_written == request_size);
        }
        int status = close(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedReadStrideFixedRSVariable",
          "[process="+std::to_string(info.comm_size)+"]"
          "[operation=batched_read]"
          "[request_size=type-variable][repetition=1024]"
          "[pattern=stride_fixed][file=1]") {
    pretest();

    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = (i * info.stride_size) % info.total_size;
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long request_size = (args.request_size +
                                (rand_r(&info.rs_seed)
                                % args.request_size))
                                        % (info.total_size - offset);
            std::string data(request_size, '1');
            long size_read = read(fd, data.data(),
                                  request_size);
            REQUIRE(size_read == request_size);
        }
        int status = close(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedUpdateStrideFixedRSVariable",
          "[process="+std::to_string(info.comm_size)+"]"
          "[operation=batched_write]"
          "[request_size=type-variable][repetition=1024]"
          "[pattern=stride_fixed][file=1]") {
    pretest();

    SECTION("write to existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        std::string data(args.request_size, '1');
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = (i * info.stride_size) % info.total_size;
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long request_size = args.request_size +
                                (rand_r(&info.rs_seed)
                                % args.request_size);
            std::string data(request_size, '1');
            long size_written = write(fd, data.c_str(),
                                      request_size);
            REQUIRE(size_written == request_size);
        }
        int status = close(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedReadStrideDynamicRSVariable",
          "[process="+std::to_string(info.comm_size)+"]"
          "[operation=batched_read]"
          "[request_size=type-variable][repetition=1024]"
          "[pattern=stride_dynamic][file=1]") {
    pretest();

    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = abs(((i * rand_r(&info.offset_seed))
                    % info.stride_size) % info.total_size);
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long request_size = args.request_size +
                                (rand_r(&info.rs_seed)
                                % args.request_size);
            std::string data(request_size, '1');
            long size_read = read(fd, data.data(),
                                  request_size);
            REQUIRE(size_read == request_size);
        }
        int status = close(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedUpdateStrideDynamicRSVariable",
          "[process="+std::to_string(info.comm_size)+"]"
          "[operation=batched_write]"
          "[request_size=type-variable][repetition=1024]"
          "[pattern=stride_dynamic][file=1]") {
    pretest();
    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = abs(((i * rand_r(&info.offset_seed))
                    % info.stride_size) % info.total_size);
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long request_size = args.request_size +
                                (rand_r(&info.rs_seed)
                                 % args.request_size);
            std::string data(request_size, '1');
            long size_written = write(fd, data.c_str(),
                                      request_size);
            REQUIRE(size_written == request_size);
        }
        int status = close(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedReadStrideNegative",
          "[process="+std::to_string(info.comm_size)+"]"
          "[operation=batched_read]"
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=stride_negative][file=1]") {
    pretest();
    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        std::string data(args.request_size, '1');
        long prev_offset = info.total_size + 1;
        for (int i = 0; i < info.num_iterations; ++i) {
            auto stride_offset = info.total_size - i * info.stride_size;
            REQUIRE(prev_offset > stride_offset);
            prev_offset = stride_offset;
            auto offset = (stride_offset)
                        % (info.total_size - args.request_size);
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long size_read = read(fd, data.data(),
                                  args.request_size);
            REQUIRE(size_read == args.request_size);
        }
        int status = close(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedUpdateStrideNegative",
          "[process="+std::to_string(info.comm_size)+"]"
          "[operation=batched_write]"
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=stride_negative][file=1]") {
    pretest();
    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        std::string data(args.request_size, '1');
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = info.total_size - ((i * info.stride_size)
                    % info.total_size);
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long size_read = write(fd, data.data(),
                                   args.request_size);
            REQUIRE(size_read == args.request_size);
        }
        int status = close(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedReadStrideNegativeRSVariable",
          "[process="+std::to_string(info.comm_size)+"]"
          "[operation=batched_read]"
          "[request_size=type-variable][repetition=1024]"
          "[pattern=stride_negative][file=1]") {
    pretest();

    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = (info.total_size - i * info.stride_size)
                        % (info.total_size - 2*args.request_size);
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long request_size = (args.request_size +
                                 (rand_r(&info.rs_seed)
                                  % args.request_size))
                                % (info.total_size - offset);
            std::string data(request_size, '1');
            long size_read = read(fd, data.data(),
                                  request_size);
            REQUIRE(size_read == request_size);
        }
        int status = close(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedUpdateStrideNegativeRSVariable",
          "[process="+std::to_string(info.comm_size)+"]"
          "[operation=batched_write]"
          "[request_size=type-variable][repetition=1024]"
          "[pattern=stride_negative][file=1]") {
    pretest();

    SECTION("write to existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        std::string data(args.request_size, '1');
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = info.total_size - ((i * info.stride_size)
                    % info.total_size);
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long request_size = args.request_size +
                                (rand_r(&info.rs_seed)
                                 % args.request_size);
            std::string data(request_size, '1');
            long size_written = write(fd, data.c_str(),
                                      request_size);
            REQUIRE(size_written == request_size);
        }
        int status = close(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedReadStride2D",
          "[process="+std::to_string(info.comm_size)+"]"
          "[operation=batched_read]"
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=stride_2d][file=1]") {
    pretest();
    long rows = sqrt(info.total_size);
    long cols = rows;
    REQUIRE(rows * cols == info.total_size);
    long cell_size = 128;
    long cell_stride = rows * cols/ cell_size / info.num_iterations;
    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        std::string data(args.request_size, '1');
        long prev_cell_col = 0, prev_cell_row = 0;
        for (int i = 0; i < info.num_iterations; ++i) {
            long current_cell_col = (prev_cell_col +  cell_stride) % cols;
            long current_cell_row = prev_cell_col + cell_stride > cols ?
                    prev_cell_row + 1: prev_cell_row;
            prev_cell_row = current_cell_row;
            auto offset = (current_cell_col*cell_stride + prev_cell_row*cols)
                          % (info.total_size - args.request_size);
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long size_read = read(fd, data.data(),
                                  args.request_size);
            REQUIRE(size_read == args.request_size);
        }
        int status = close(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedUpdateStride2D",
          "[process="+std::to_string(info.comm_size)+"]"
          "[operation=batched_write]"
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=stride_2d][file=1]") {
    pretest();

    long rows = sqrt(info.total_size);
    long cols = rows;
    REQUIRE(rows * cols == info.total_size);
    long cell_size = 128;
    long cell_stride = rows * cols/ cell_size / info.num_iterations;
    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        std::string data(args.request_size, '1');
        long prev_cell_col = 0, prev_cell_row = 0;
        for (int i = 0; i < info.num_iterations; ++i) {
            long current_cell_col = (prev_cell_col +  cell_stride) % cols;
            long current_cell_row = prev_cell_col + cell_stride > cols ?
                                    prev_cell_row + 1: prev_cell_row;
            prev_cell_row = current_cell_row;
            auto offset = (current_cell_col*cell_stride + prev_cell_row*cols)
                          % (info.total_size - args.request_size);
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long size_read = write(fd, data.data(),
                                   args.request_size);
            REQUIRE(size_read == args.request_size);
        }
        int status = close(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedReadStride2DRSVariable",
          "[process="+std::to_string(info.comm_size)+"]"
          "[operation=batched_read]"
          "[request_size=type-variable][repetition=1024]"
          "[pattern=stride_2d][file=1]") {
    pretest();
    long rows = sqrt(info.total_size);
    long cols = rows;
    REQUIRE(rows * cols == info.total_size);
    long cell_size = 128;
    long cell_stride = rows * cols/ cell_size / info.num_iterations;
    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        long prev_cell_col = 0, prev_cell_row = 0;
        for (int i = 0; i < info.num_iterations; ++i) {
            long current_cell_col = (prev_cell_col +  cell_stride) % cols;
            long current_cell_row = prev_cell_col + cell_stride > cols ?
                                    prev_cell_row + 1: prev_cell_row;
            prev_cell_row = current_cell_row;
            auto offset = (current_cell_col*cell_stride + prev_cell_row*cols)
                          % (info.total_size - 2*args.request_size);
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long request_size = (args.request_size +
                                 (rand_r(&info.rs_seed)
                                  % args.request_size))
                                % (info.total_size - offset);
            std::string data(request_size, '1');
            long size_read = read(fd, data.data(),
                                  request_size);
            REQUIRE(size_read == request_size);
        }
        int status = close(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedUpdateStride2DRSVariable",
          "[process="+std::to_string(info.comm_size)+"]"
          "[operation=batched_write]"
          "[request_size=type-variable][repetition=1024]"
          "[pattern=stride_2d][file=1]") {
    pretest();
    long rows = sqrt(info.total_size);
    long cols = rows;
    REQUIRE(rows * cols == info.total_size);
    long cell_size = 128;
    long cell_stride = rows * cols/ cell_size / info.num_iterations;
    SECTION("write to existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        std::string data(args.request_size, '1');
        long prev_cell_col = 0, prev_cell_row = 0;
        for (int i = 0; i < info.num_iterations; ++i) {
            long current_cell_col = (prev_cell_col +  cell_stride) % cols;
            long current_cell_row = prev_cell_col + cell_stride > cols ?
                                    prev_cell_row + 1: prev_cell_row;
            prev_cell_row = current_cell_row;
            auto offset = (current_cell_col*cell_stride + prev_cell_row*cols)
                          % (info.total_size - 2*args.request_size);
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long request_size = args.request_size +
                                (rand_r(&info.rs_seed)
                                 % args.request_size);
            std::string data(request_size, '1');
            long size_written = write(fd, data.c_str(),
                                      request_size);
            REQUIRE(size_written == request_size);
        }
        int status = close(fd);
        REQUIRE(status == 0);
    }
    posttest();
}


/**
 * Temporal Fixed
 */


TEST_CASE("BatchedWriteTemporalFixed",
          "[process="+std::to_string(info.comm_size)+"]"
          "[operation=batched_write]"
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=sequential][file=1][temporal=fixed]") {
    pretest();

    SECTION("write to existing file") {
        int fd = open(info.new_file.c_str(), O_WRONLY | O_CREAT | O_EXCL);
        REQUIRE(fd != -1);

        for (int i = 0; i < info.num_iterations; ++i) {
            usleep(info.temporal_interval_ms * 1000);
            long offset = lseek(fd, 0, SEEK_SET);
            REQUIRE(offset == 0);
            long size_written = write(fd, info.write_data.c_str(),
                                      args.request_size);
            REQUIRE(size_written == args.request_size);
        }
        int status = close(fd);
        REQUIRE(status == 0);
        REQUIRE(fs::file_size(info.new_file) == args.request_size);
    }

    SECTION("write to new file always at start") {
        int fd = open(info.new_file.c_str(), O_WRONLY | O_CREAT | O_EXCL);
        REQUIRE(fd != -1);

        for (int i = 0; i < info.num_iterations; ++i) {
            usleep(info.temporal_interval_ms * 1000);
            long size_written = write(fd, info.write_data.c_str(),
                                      args.request_size);
            REQUIRE(size_written == args.request_size);
        }
        int status = close(fd);
        REQUIRE(status == 0);
        REQUIRE(fs::file_size(info.new_file)
        == info.num_iterations * args.request_size);
    }
    posttest();
}

TEST_CASE("BatchedReadSequentialTemporalFixed",
          "[process="+std::to_string(info.comm_size)+"]"
          "[operation=batched_read]"
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=sequential][file=1][temporal=fixed]") {
    pretest();

    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        std::string data(args.request_size, '1');
        for (int i = 0; i < info.num_iterations; ++i) {
            usleep(info.temporal_interval_ms * 1000);
            long size_read = read(fd, data.data(),
                                  args.request_size);
            REQUIRE(size_read == args.request_size);
        }
        int status = close(fd);
        REQUIRE(status == 0);
    }

    SECTION("read from existing file always at start") {
        int fd = open(info.existing_file.c_str(), O_WRONLY);
        REQUIRE(fd != -1);

        for (int i = 0; i < info.num_iterations; ++i) {
            usleep(info.temporal_interval_ms * 1000);
            int offset = lseek(fd, 0, SEEK_SET);
            REQUIRE(offset == 0);
            long size_written = write(fd, info.write_data.c_str(),
                                      args.request_size);
            REQUIRE(size_written == args.request_size);
        }
        int status = close(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedWriteTemporalVariable",
          "[process="+std::to_string(info.comm_size)+"]"
          "[operation=batched_write]"
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=sequential][file=1][temporal=variable]") {
    pretest();

    SECTION("write to existing file") {
        int fd = open(info.new_file.c_str(), O_WRONLY | O_CREAT | O_EXCL);
        REQUIRE(fd != -1);

        for (int i = 0; i < info.num_iterations; ++i) {
            info.temporal_interval_ms = rand_r(&info.temporal_interval_seed)
                    % info.temporal_interval_ms + 1;
            usleep(info.temporal_interval_ms * 1000);
            int offset = lseek(fd, 0, SEEK_SET);
            REQUIRE(offset == 0);
            long size_written = write(fd, info.write_data.c_str(),
                                      args.request_size);
            REQUIRE(size_written == args.request_size);
        }
        int status = close(fd);
        REQUIRE(status == 0);
        REQUIRE(fs::file_size(info.new_file) == args.request_size);
    }

    SECTION("write to new file always at start") {
        int fd = open(info.new_file.c_str(), O_WRONLY | O_CREAT | O_EXCL);
        REQUIRE(fd != -1);

        for (int i = 0; i < info.num_iterations; ++i) {
            info.temporal_interval_ms = rand_r(&info.temporal_interval_seed)
                                        % info.temporal_interval_ms + 1;
            usleep(info.temporal_interval_ms * 1000);
            long size_written = write(fd, info.write_data.c_str(),
                                      args.request_size);
            REQUIRE(size_written == args.request_size);
        }
        int status = close(fd);
        REQUIRE(status == 0);
        REQUIRE(fs::file_size(info.new_file)
        == info.num_iterations * args.request_size);
    }
    posttest();
}

TEST_CASE("BatchedReadSequentialTemporalVariable",
          "[process="+std::to_string(info.comm_size)+"]"
          "[operation=batched_read]"
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=sequential][file=1][temporal=variable]") {
    pretest();

    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        std::string data(args.request_size, '1');
        for (int i = 0; i < info.num_iterations; ++i) {
            info.temporal_interval_ms = rand_r(&info.temporal_interval_seed)
                                        % info.temporal_interval_ms + 1;
            usleep(info.temporal_interval_ms * 1000);
            long size_read = read(fd, data.data(),
                                  args.request_size);
            REQUIRE(size_read == args.request_size);
        }
        int status = close(fd);
        REQUIRE(status == 0);
    }

    SECTION("read from existing file always at start") {
        int fd = open(info.existing_file.c_str(), O_WRONLY);
        REQUIRE(fd != -1);

        for (int i = 0; i < info.num_iterations; ++i) {
            info.temporal_interval_ms = rand_r(&info.temporal_interval_seed)
                                        % info.temporal_interval_ms + 1;
            usleep(info.temporal_interval_ms * 1000);
            int offset = lseek(fd, 0, SEEK_SET);
            REQUIRE(offset == 0);
            long size_written = write(fd, info.write_data.c_str(),
                                      args.request_size);
            REQUIRE(size_written == args.request_size);
        }
        int status = close(fd);
        REQUIRE(status == 0);
    }
    posttest();
}


TEST_CASE("BatchedMixedSequential",
          "[process="+std::to_string(info.comm_size)+"]"
          "[operation=batched_mixed]"
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=sequential][file=1]") {
    pretest();
    SECTION("read after write on new file") {
        int fd = open(info.new_file.c_str(), O_RDWR | O_CREAT | O_EXCL);
        REQUIRE(fd != -1);
        long last_offset = 0;
        for (int i = 0; i < info.num_iterations; ++i) {
            long size_written = write(fd, info.write_data.data(),
                                      args.request_size);
            REQUIRE(size_written == args.request_size);
            auto status = lseek(fd, last_offset, SEEK_SET);
            REQUIRE(status == last_offset);
            long size_read = read(fd, info.read_data.data(),
                                  args.request_size);
            REQUIRE(size_read == args.request_size);
            last_offset += args.request_size;
        }
        int status = close(fd);
        REQUIRE(status == 0);
    }

    SECTION("write and read alternative existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        for (int i = 0; i < info.num_iterations; ++i) {
            if (i % 2 == 0) {
                long size_written = write(fd, info.write_data.data(),
                                          args.request_size);
                REQUIRE(size_written == args.request_size);
            } else {
                long size_read = read(fd, info.read_data.data(),
                                      args.request_size);
                REQUIRE(size_read == args.request_size);
            }
        }
        int status = close(fd);
        REQUIRE(status == 0);
    }
    SECTION("update after read existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        long last_offset = 0;
        for (int i = 0; i < info.num_iterations; ++i) {
            long size_read = read(fd, info.read_data.data(),
                                  args.request_size);
            REQUIRE(size_read == args.request_size);
            auto status = lseek(fd, last_offset, SEEK_SET);
            REQUIRE(status == last_offset);
            long size_written = write(fd, info.write_data.data(),
                                      args.request_size);
            REQUIRE(size_written == args.request_size);
            last_offset += args.request_size;
        }

        int status = close(fd);
        REQUIRE(status == 0);
    }
    SECTION("read all after write all on new file in single open") {
        int fd = open(info.new_file.c_str(), O_RDWR | O_CREAT | O_EXCL);
        REQUIRE(fd != -1);
        for (int i = 0; i < info.num_iterations; ++i) {
            long size_written = write(fd, info.write_data.data(),
                                      args.request_size);
            REQUIRE(size_written == args.request_size);
        }
        int status = lseek(fd, 0, SEEK_SET);
        REQUIRE(status == 0);
        for (int i = 0; i < info.num_iterations; ++i) {
            long size_read = read(fd, info.read_data.data(),
                                  args.request_size);
            REQUIRE(size_read == args.request_size);
        }
        status = close(fd);
        REQUIRE(status == 0);
    }
    SECTION("read all after write all on new file in different open") {
        int fd = open(info.new_file.c_str(), O_RDWR | O_CREAT,
                      S_IRWXU | S_IRWXG);
        REQUIRE(fd != -1);
        for (int i = 0; i < info.num_iterations; ++i) {
            long size_written = write(fd, info.write_data.data(),
                                      args.request_size);
            REQUIRE(size_written == args.request_size);
        }
        auto status = close(fd);
        REQUIRE(status == 0);
        int fd2 = open(info.new_file.c_str(), O_RDWR);
        REQUIRE(fd2 != -1);
        for (int i = 0; i < info.num_iterations; ++i) {
            long size_read = read(fd2, info.read_data.data(),
                                  args.request_size);
            REQUIRE(size_read == args.request_size);
        }
        status = close(fd2);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("SingleMixed",
          "[process="+std::to_string(info.comm_size)+"][operation=single_mixed]"
          "[request_size=type-fixed][repetition=1]"
          "[file=1]") {
    pretest();
    SECTION("read after write from new file") {
        int fd = open(info.new_file.c_str(), O_RDWR | O_CREAT | O_EXCL);
        REQUIRE(fd != -1);
        long size_write = write(fd, info.write_data.data(),
                                args.request_size);
        REQUIRE(size_write == args.request_size);
        int status = lseek(fd, 0, SEEK_SET);
        REQUIRE(status == 0);
        long size_read = read(fd, info.read_data.data(),
                              args.request_size);
        REQUIRE(size_read == args.request_size);
        status = close(fd);
        REQUIRE(status == 0);
    }
    SECTION("update after read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        long size_read = read(fd, info.read_data.data(),
                              args.request_size);
        REQUIRE(size_read == args.request_size);
        int status = lseek(fd, 0, SEEK_SET);
        REQUIRE(status == 0);
        long size_write = write(fd, info.write_data.data(),
                                args.request_size);
        REQUIRE(size_write == args.request_size);

        status = close(fd);
        REQUIRE(status == 0);
    }
    SECTION("read after write from new file different opens") {
        int fd = open(info.new_file.c_str(), O_RDWR | O_CREAT | O_EXCL);
        REQUIRE(fd != -1);
        long size_write = write(fd, info.write_data.data(),
                                args.request_size);
        REQUIRE(size_write == args.request_size);
        int status = close(fd);
        REQUIRE(status == 0);
        int fd2 = open(info.existing_file.c_str(), O_RDWR);
        long size_read = read(fd2, info.read_data.data(),
                              args.request_size);
        REQUIRE(size_read == args.request_size);
        status = close(fd2);
        REQUIRE(status == 0);
    }
    posttest();
}
