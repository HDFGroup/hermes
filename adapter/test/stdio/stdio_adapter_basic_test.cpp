TEST_CASE("Open", "[process=1][operation=single_open]"
                  "[repetition=1][file=1]") {
    pretest();
    SECTION("open non-existant file") {
        FILE* fd = fopen(info.new_file.c_str(), "r");
        REQUIRE(fd == nullptr);
        fd = fopen(info.new_file.c_str(), "r+");
        REQUIRE(fd == nullptr);
    }

    SECTION("truncate existing file and write-only") {
        FILE* fd = fopen(info.existing_file.c_str(), "w");
        REQUIRE(fd != nullptr);
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    SECTION("truncate existing file and read/write") {
        FILE* fd = fopen(info.existing_file.c_str(), "w+");
        REQUIRE(fd != nullptr);
        int status = fclose(fd);
        REQUIRE(status == 0);
    }

    SECTION("open existing file") {
        FILE* fd = fopen(info.existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        int status = fclose(fd);
        REQUIRE(status == 0);
        fd = fopen(info.existing_file.c_str(), "r");
        REQUIRE(fd != nullptr);
        status = fclose(fd);
        REQUIRE(status == 0);
    }

    SECTION("append write new file") {
        FILE* fd = fopen(info.existing_file.c_str(), "a");
        REQUIRE(fd != nullptr);
        int status = fclose(fd);
        REQUIRE(status == 0);
    }

    SECTION("append write and read new file") {
        FILE* fd = fopen(info.existing_file.c_str(), "a+");
        REQUIRE(fd != nullptr);
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("SingleWrite",
          "[process=1][operation=single_write]"
          "[request_size=type-fixed][repetition=1]"
          "[file=1]") {
    pretest();
    SECTION("write to existing file") {
        FILE* fd = fopen(info.existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        long offset = fseek(fd, 0, SEEK_SET);
        long size_written = fwrite(info.write_data.c_str(),
                                   sizeof(char), args.request_size, fd);
        REQUIRE(size_written == args.request_size);
        int status = fclose(fd);
        REQUIRE(status == 0);
    }

    SECTION("write to new  file") {
        FILE* fd = fopen(info.new_file.c_str(), "w+");
        REQUIRE(fd != nullptr);
        long size_written = fwrite(info.write_data.c_str(), sizeof(char),
                                   args.request_size, fd);
        REQUIRE(size_written == args.request_size);
        int status = fclose(fd);
        REQUIRE(status == 0);
        REQUIRE(fs::file_size(info.new_file) == size_written);
    }

    SECTION("write to existing file with truncate") {
        FILE* fd = fopen(info.existing_file.c_str(), "w");
        REQUIRE(fd != nullptr);
        long size_written = fwrite(info.write_data.c_str(), sizeof(char),
                                   args.request_size, fd);
        REQUIRE(size_written == args.request_size);
        int status = fclose(fd);
        REQUIRE(status == 0);
        REQUIRE(fs::file_size(info.existing_file) == size_written);
    }

    SECTION("write to existing file at the end") {
        FILE* fd = fopen(info.existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        int status = fseek(fd, 0, SEEK_END);
        REQUIRE(status == 0);
        long offset = ftell(fd);
        REQUIRE(offset == args.request_size * info.num_iterations);
        long size_written = fwrite(info.write_data.c_str(), sizeof(char),
                                   args.request_size, fd);
        REQUIRE(size_written == args.request_size);
        status = fclose(fd);
        REQUIRE(status == 0);
        REQUIRE(fs::file_size(info.existing_file) == size_written + offset);
    }

    SECTION("append to existing file") {
        auto existing_size = fs::file_size(info.existing_file);
        FILE* fd = fopen(info.existing_file.c_str(), "a+");
        REQUIRE(fd != nullptr);
        long size_written = fwrite(info.write_data.c_str(),
                                   sizeof(char), args.request_size, fd);
        REQUIRE(size_written == args.request_size);
        int status = fclose(fd);
        REQUIRE(status == 0);
        REQUIRE(fs::file_size(info.existing_file)
                == existing_size + size_written);
    }

    SECTION("append to new file") {
        FILE* fd = fopen(info.new_file.c_str(), "w+");
        REQUIRE(fd != nullptr);
        long size_written = fwrite(info.write_data.c_str(), sizeof(char),
                                   args.request_size, fd);
        REQUIRE(size_written == args.request_size);
        int status = fclose(fd);
        REQUIRE(status == 0);
        REQUIRE(fs::file_size(info.new_file) == size_written);
    }
    posttest();
}

TEST_CASE("SingleRead",
          "[process=1][operation=single_read]"
          "[request_size=type-fixed][repetition=1]"
          "[file=1]") {
    pretest();
    SECTION("read from non-existing file") {
        FILE* fd = fopen(info.new_file.c_str(), "r");
        REQUIRE(fd == nullptr);
    }

    SECTION("read from existing file") {
        FILE* fd = fopen(info.existing_file.c_str(), "r");
        REQUIRE(fd != nullptr);
        long offset = ftell(fd);
        REQUIRE(offset == 0);
        long size_read = fread(info.read_data.data(), sizeof(char),
                                   args.request_size, fd);
        REQUIRE(size_read == args.request_size);
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    SECTION("read at the end of existing file") {
        FILE* fd = fopen(info.existing_file.c_str(), "r");
        REQUIRE(fd != nullptr);
        int status = fseek(fd, 0, SEEK_END);
        REQUIRE(status == 0);
        long offset = ftell(fd);
        REQUIRE(offset == args.request_size * info.num_iterations);
        long size_read = fread(info.read_data.data(), sizeof(char),
                               args.request_size, fd);
        REQUIRE(size_read == 0);
        status = fclose(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedWriteSequential",
          "[process=1][operation=batched_write]"
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=sequential][file=1]") {
    pretest();
    SECTION("write to existing file") {
        FILE* fd = fopen(info.new_file.c_str(), "w+");
        REQUIRE(fd != nullptr);

        for (int i = 0; i < info.num_iterations; ++i) {
            int status = fseek(fd, 0, SEEK_SET);
            REQUIRE(status == 0);
            long offset = ftell(fd);
            REQUIRE(offset == 0);
            long size_written = fwrite(info.write_data.c_str(),
                                       sizeof(char), args.request_size, fd);
            REQUIRE(size_written == args.request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
        REQUIRE(fs::file_size(info.new_file) == args.request_size);
    }

    SECTION("write to new file always at start") {
        FILE* fd = fopen(info.new_file.c_str(), "w+");
        REQUIRE(fd != nullptr);

        for (int i = 0; i < info.num_iterations; ++i) {
            long size_written = fwrite(info.write_data.c_str(),
                                       sizeof(char), args.request_size, fd);
            REQUIRE(size_written == args.request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
        REQUIRE(fs::file_size(info.new_file) ==
        info.num_iterations * args.request_size);
    }
    posttest();
}


TEST_CASE("BatchedReadSequential",
          "[process=1][operation=batched_read]"
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=sequential][file=1]") {
    pretest();

    SECTION("read from existing file") {
        FILE* fd = fopen(info.existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        std::string data(args.request_size, '1');
        for (int i = 0; i < info.num_iterations; ++i) {
            long size_read = fread(data.data(),
                                       sizeof(char), args.request_size, fd);
            REQUIRE(size_read == args.request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }

    SECTION("read from existing file always at start") {
        FILE* fd = fopen(info.existing_file.c_str(), "w+");
        REQUIRE(fd != nullptr);

        for (int i = 0; i < info.num_iterations; ++i) {
            int status = fseek(fd, 0, SEEK_SET);
            REQUIRE(status == 0);
            long offset = ftell(fd);
            REQUIRE(offset == 0);
            long size_written = fwrite(info.write_data.c_str(),
                                       sizeof(char), args.request_size, fd);
            REQUIRE(size_written == args.request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedReadRandom",
          "[process=1][operation=batched_read]"
          "[request_size=type-fixed]"
          "[repetition=1024][pattern=random][file=1]") {
    pretest();
    SECTION("read from existing file") {
        FILE* fd = fopen(info.existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        std::string data(args.request_size, '1');
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = rand_r(&info.offset_seed)
                    % (info.total_size - args.request_size);
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long size_read = fread(data.data(),
                                   sizeof(char), args.request_size, fd);
            REQUIRE(size_read == args.request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedUpdateRandom",
          "[process=1][operation=batched_write]"
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=random][file=1]") {
    pretest();
    SECTION("read from existing file") {
        FILE* fd = fopen(info.existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        std::string data(args.request_size, '1');
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = rand_r(&info.offset_seed)
                    % (info.total_size - args.request_size);
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long size_read = fwrite(data.data(),
                                   sizeof(char), args.request_size, fd);
            REQUIRE(size_read == args.request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedReadStrideFixed",
          "[process=1][operation=batched_read]"
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=stride_fixed][file=1]") {
    pretest();

    SECTION("read from existing file") {
        FILE* fd = fopen(info.existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        std::string data(args.request_size, '1');
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = (i * info.stride_size) % info.total_size;
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long size_read = fread(data.data(),
                                   sizeof(char), args.request_size, fd);
            REQUIRE(size_read == args.request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedUpdateStrideFixed",
          "[process=1][operation=batched_write]"
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=stride_fixed][file=1]") {
    pretest();

    SECTION("read from existing file") {
        FILE* fd = fopen(info.existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        std::string data(args.request_size, '1');
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = (i * info.stride_size) % info.total_size;
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long size_read = fwrite(data.data(),
                                    sizeof(char), args.request_size, fd);
            REQUIRE(size_read == args.request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedReadStrideDynamic",
          "[process=1][operation=batched_read]"
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=stride_dynamic][file=1]") {
    pretest();

    SECTION("read from existing file") {
        FILE* fd = fopen(info.existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        std::string data(args.request_size, '1');
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = abs(((i * rand_r(&info.offset_seed))
                    % info.stride_size) % info.total_size);
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long size_read = fread(data.data(),
                                   sizeof(char), args.request_size, fd);
            REQUIRE(size_read == args.request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedUpdateStrideDynamic",
          "[process=1][operation=batched_write]"
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=stride_dynamic][file=1]") {
    pretest();
    SECTION("read from existing file") {
        FILE* fd = fopen(info.existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        std::string data(args.request_size, '1');
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = abs(((i * rand_r(&info.offset_seed))
                    % info.stride_size) % info.total_size);
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long size_read = fwrite(data.data(),
                                    sizeof(char), args.request_size, fd);
            REQUIRE(size_read == args.request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedWriteRSVariable",
          "[process=1][operation=batched_write]"
          "[request_size=type-variable][repetition=1024]"
          "[pattern=sequential][file=1]") {
    pretest();

    SECTION("write to new file always at the start") {
        FILE* fd = fopen(info.new_file.c_str(), "w+");
        REQUIRE(fd != nullptr);
        long biggest_size_written = 0;
        for (int i = 0; i < info.num_iterations; ++i) {
            int status = fseek(fd, 0, SEEK_SET);
            REQUIRE(status == 0);
            long offset = ftell(fd);
            REQUIRE(offset == 0);
            long request_size = args.request_size +
                                (rand_r(&info.offset_seed) % args.request_size);
            std::string data(request_size, '1');
            long size_written = fwrite(data.c_str(),
                                       sizeof(char), request_size, fd);
            REQUIRE(size_written == request_size);
            if (biggest_size_written < request_size)
                biggest_size_written = request_size;
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
        REQUIRE(fs::file_size(info.new_file) == biggest_size_written);
    }

    SECTION("write to new file") {
        FILE* fd = fopen(info.new_file.c_str(), "w+");
        REQUIRE(fd != nullptr);
        long total_size_written = 0;
        for (int i = 0; i < info.num_iterations; ++i) {
            long request_size = args.request_size +
                                (rand_r(&info.offset_seed) % args.request_size);
            std::string data(request_size, '1');
            long size_written = fwrite(data.c_str(),
                                       sizeof(char), request_size, fd);
            REQUIRE(size_written == request_size);
            total_size_written += size_written;
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
        REQUIRE(fs::file_size(info.new_file) == total_size_written);
    }
    posttest();
}


TEST_CASE("BatchedReadSequentialRSVariable",
          "[process=1][operation=batched_read]"
          "[request_size=type-variable][repetition=1024]"
          "[pattern=sequential][file=1]") {
    pretest();
    SECTION("read from existing file") {
        FILE* fd = fopen(info.existing_file.c_str(), "r");
        REQUIRE(fd != nullptr);
        std::string data(args.request_size, '1');
        long current_offset = 0;
        for (int i = 0; i < info.num_iterations; ++i) {
            long request_size = (args.request_size +
                                (rand_r(&info.offset_seed) % args.request_size))
                                        % (info.total_size - current_offset);
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
        FILE* fd = fopen(info.existing_file.c_str(), "r");
        REQUIRE(fd != nullptr);

        for (int i = 0; i < info.num_iterations; ++i) {
            int status = fseek(fd, 0, SEEK_SET);
            REQUIRE(status == 0);
            long offset = ftell(fd);
            REQUIRE(offset == 0);
            long request_size = args.request_size +
                                (rand_r(&info.offset_seed) % args.request_size);
            std::string data(request_size, '1');
            long size_read = fread(data.data(),
                                   sizeof(char), request_size, fd);
            REQUIRE(size_read == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedReadRandomRSVariable",
          "[process=1][operation=batched_read]"
          "[request_size=type-variable]"
          "[repetition=1024][pattern=random][file=1]") {
    pretest();

    SECTION("read from existing file") {
        FILE* fd = fopen(info.existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        std::string data(args.request_size, '1');
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = rand_r(&info.offset_seed)
                    % (info.total_size - args.request_size);
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = (args.request_size +
                                (rand_r(&info.rs_seed)
                                % args.request_size))
                                % (info.total_size - offset);
            std::string data(request_size, '1');
            long size_read = fread(data.data(),
                                   sizeof(char), request_size, fd);
            REQUIRE(size_read == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedUpdateRandomRSVariable",
          "[process=1][operation=batched_write]"
          "[request_size=type-variable][repetition=1024]"
          "[pattern=random][file=1]") {
    pretest();

    SECTION("read from existing file") {
        FILE* fd = fopen(info.existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        std::string data(args.request_size, '1');
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = rand_r(&info.offset_seed)
                    % (info.total_size - args.request_size);
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = args.request_size +
                                (rand_r(&info.rs_seed)
                                % args.request_size);
            std::string data(request_size, '1');
            long size_written = fwrite(data.c_str(),
                                       sizeof(char), request_size, fd);
            REQUIRE(size_written == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedReadStrideFixedRSVariable",
          "[process=1][operation=batched_read]"
          "[request_size=type-variable][repetition=1024]"
          "[pattern=stride_fixed][file=1]") {
    pretest();

    SECTION("read from existing file") {
        FILE* fd = fopen(info.existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = (i * info.stride_size) % info.total_size;
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = (args.request_size +
                                (rand_r(&info.rs_seed)
                                % args.request_size))
                                        % (info.total_size - offset);
            std::string data(request_size, '1');
            long size_read = fread(data.data(),
                                   sizeof(char), request_size, fd);
            REQUIRE(size_read == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedUpdateStrideFixedRSVariable",
          "[process=1][operation=batched_write]"
          "[request_size=type-variable][repetition=1024]"
          "[pattern=stride_fixed][file=1]") {
    pretest();

    SECTION("write to existing file") {
        FILE* fd = fopen(info.existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        std::string data(args.request_size, '1');
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = (i * info.stride_size) % info.total_size;
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = args.request_size +
                                (rand_r(&info.rs_seed)
                                % args.request_size);
            std::string data(request_size, '1');
            long size_written = fwrite(data.c_str(),
                                       sizeof(char), request_size, fd);
            REQUIRE(size_written == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedReadStrideDynamicRSVariable",
          "[process=1][operation=batched_read]"
          "[request_size=type-variable][repetition=1024]"
          "[pattern=stride_dynamic][file=1]") {
    pretest();

    SECTION("read from existing file") {
        FILE* fd = fopen(info.existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = abs(((i * rand_r(&info.offset_seed))
                    % info.stride_size) % info.total_size);
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = args.request_size +
                                (rand_r(&info.rs_seed)
                                % args.request_size);
            std::string data(request_size, '1');
            long size_read = fread(data.data(),
                                   sizeof(char), request_size, fd);
            REQUIRE(size_read == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedUpdateStrideDynamicRSVariable",
          "[process=1][operation=batched_write]"
          "[request_size=type-variable][repetition=1024]"
          "[pattern=stride_dynamic][file=1]") {
    pretest();
    SECTION("read from existing file") {
        FILE* fd = fopen(info.existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = abs(((i * rand_r(&info.offset_seed))
                    % info.stride_size) % info.total_size);
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = args.request_size +
                                (rand_r(&info.rs_seed)
                                 % args.request_size);
            std::string data(request_size, '1');
            long size_written = fwrite(data.c_str(),
                                       sizeof(char), request_size, fd);
            REQUIRE(size_written == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedReadStrideNegative",
          "[process=1][operation=batched_read]"
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=stride_negative][file=1]") {
    pretest();
    SECTION("read from existing file") {
        FILE* fd = fopen(info.existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        std::string data(args.request_size, '1');
        long prev_offset = info.total_size + 1;
        for (int i = 0; i < info.num_iterations; ++i) {
            auto stride_offset = info.total_size - i * info.stride_size;
            REQUIRE(prev_offset > stride_offset);
            prev_offset = stride_offset;
            auto offset = (stride_offset)
                        % (info.total_size - args.request_size);
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long size_read = fread(data.data(),
                                   sizeof(char), args.request_size, fd);
            REQUIRE(size_read == args.request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedUpdateStrideNegative",
          "[process=1][operation=batched_write]"
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=stride_negative][file=1]") {
    pretest();
    SECTION("read from existing file") {
        FILE* fd = fopen(info.existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        std::string data(args.request_size, '1');
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = info.total_size - ((i * info.stride_size)
                    % info.total_size);
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long size_read = fwrite(data.data(),
                                    sizeof(char), args.request_size, fd);
            REQUIRE(size_read == args.request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedReadStrideNegativeRSVariable",
          "[process=1][operation=batched_read]"
          "[request_size=type-variable][repetition=1024]"
          "[pattern=stride_negative][file=1]") {
    pretest();

    SECTION("read from existing file") {
        FILE* fd = fopen(info.existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = (info.total_size - i * info.stride_size)
                        % (info.total_size - 2*args.request_size);
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = (args.request_size +
                                 (rand_r(&info.rs_seed)
                                  % args.request_size))
                                % (info.total_size - offset);
            std::string data(request_size, '1');
            long size_read = fread(data.data(),
                                   sizeof(char), request_size, fd);
            REQUIRE(size_read == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedUpdateStrideNegativeRSVariable",
          "[process=1][operation=batched_write]"
          "[request_size=type-variable][repetition=1024]"
          "[pattern=stride_negative][file=1]") {
    pretest();

    SECTION("write to existing file") {
        FILE* fd = fopen(info.existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        std::string data(args.request_size, '1');
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = info.total_size - ((i * info.stride_size)
                    % info.total_size);
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = args.request_size +
                                (rand_r(&info.rs_seed)
                                 % args.request_size);
            std::string data(request_size, '1');
            long size_written = fwrite(data.c_str(),
                                       sizeof(char), request_size, fd);
            REQUIRE(size_written == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedReadStride2D",
          "[process=1][operation=batched_read]"
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=stride_2d][file=1]") {
    pretest();
    long rows = sqrt(info.total_size);
    long cols = rows;
    REQUIRE(rows * cols == info.total_size);
    long cell_size = 128;
    long cell_stride = rows * cols/ cell_size / info.num_iterations;
    SECTION("read from existing file") {
        FILE* fd = fopen(info.existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        std::string data(args.request_size, '1');
        long prev_cell_col = 0, prev_cell_row = 0;
        for (int i = 0; i < info.num_iterations; ++i) {
            long current_cell_col = (prev_cell_col +  cell_stride) % cols;
            long current_cell_row = prev_cell_col + cell_stride > cols ?
                    prev_cell_row + 1: prev_cell_row;
            prev_cell_row = current_cell_row;
            auto offset = (current_cell_col*cell_stride + prev_cell_row*cols)
                          % (info.total_size - args.request_size);
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long size_read = fread(data.data(),
                                   sizeof(char), args.request_size, fd);
            REQUIRE(size_read == args.request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedUpdateStride2D",
          "[process=1][operation=batched_write]"
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=stride_2d][file=1]") {
    pretest();

    long rows = sqrt(info.total_size);
    long cols = rows;
    REQUIRE(rows * cols == info.total_size);
    long cell_size = 128;
    long cell_stride = rows * cols/ cell_size / info.num_iterations;
    SECTION("read from existing file") {
        FILE* fd = fopen(info.existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        std::string data(args.request_size, '1');
        long prev_cell_col = 0, prev_cell_row = 0;
        for (int i = 0; i < info.num_iterations; ++i) {
            long current_cell_col = (prev_cell_col +  cell_stride) % cols;
            long current_cell_row = prev_cell_col + cell_stride > cols ?
                                    prev_cell_row + 1: prev_cell_row;
            prev_cell_row = current_cell_row;
            auto offset = (current_cell_col*cell_stride + prev_cell_row*cols)
                          % (info.total_size - args.request_size);
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long size_read = fwrite(data.data(),
                                    sizeof(char), args.request_size, fd);
            REQUIRE(size_read == args.request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedReadStride2DRSVariable",
          "[process=1][operation=batched_read]"
          "[request_size=type-variable][repetition=1024]"
          "[pattern=stride_2d][file=1]") {
    pretest();
    long rows = sqrt(info.total_size);
    long cols = rows;
    REQUIRE(rows * cols == info.total_size);
    long cell_size = 128;
    long cell_stride = rows * cols/ cell_size / info.num_iterations;
    SECTION("read from existing file") {
        FILE* fd = fopen(info.existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        long prev_cell_col = 0, prev_cell_row = 0;
        for (int i = 0; i < info.num_iterations; ++i) {
            long current_cell_col = (prev_cell_col +  cell_stride) % cols;
            long current_cell_row = prev_cell_col + cell_stride > cols ?
                                    prev_cell_row + 1: prev_cell_row;
            prev_cell_row = current_cell_row;
            auto offset = (current_cell_col*cell_stride + prev_cell_row*cols)
                          % (info.total_size - 2*args.request_size);
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = (args.request_size +
                                 (rand_r(&info.rs_seed)
                                  % args.request_size))
                                % (info.total_size - offset);
            std::string data(request_size, '1');
            long size_read = fread(data.data(),
                                   sizeof(char), request_size, fd);
            REQUIRE(size_read == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedUpdateStride2DRSVariable",
          "[process=1][operation=batched_write]"
          "[request_size=type-variable][repetition=1024]"
          "[pattern=stride_2d][file=1]") {
    pretest();
    long rows = sqrt(info.total_size);
    long cols = rows;
    REQUIRE(rows * cols == info.total_size);
    long cell_size = 128;
    long cell_stride = rows * cols/ cell_size / info.num_iterations;
    SECTION("write to existing file") {
        FILE* fd = fopen(info.existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        std::string data(args.request_size, '1');
        long prev_cell_col = 0, prev_cell_row = 0;
        for (int i = 0; i < info.num_iterations; ++i) {
            long current_cell_col = (prev_cell_col +  cell_stride) % cols;
            long current_cell_row = prev_cell_col + cell_stride > cols ?
                                    prev_cell_row + 1: prev_cell_row;
            prev_cell_row = current_cell_row;
            auto offset = (current_cell_col*cell_stride + prev_cell_row*cols)
                          % (info.total_size - 2*args.request_size);
            auto status = fseek(fd, offset, SEEK_SET);
            REQUIRE(status == 0);
            long request_size = args.request_size +
                                (rand_r(&info.rs_seed)
                                 % args.request_size);
            std::string data(request_size, '1');
            long size_written = fwrite(data.c_str(),
                                       sizeof(char), request_size, fd);
            REQUIRE(size_written == request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    posttest();
}


/**
 * Temporal Fixed
 */


TEST_CASE("BatchedWriteTemporalFixed",
          "[process=1][operation=batched_write]"
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=sequential][file=1][temporal=fixed]") {
    pretest();

    SECTION("write to existing file") {
        FILE* fd = fopen(info.new_file.c_str(), "w+");
        REQUIRE(fd != nullptr);

        for (int i = 0; i < info.num_iterations; ++i) {
            usleep(info.temporal_interval_ms * 1000);
            int status = fseek(fd, 0, SEEK_SET);
            REQUIRE(status == 0);
            long offset = ftell(fd);
            REQUIRE(offset == 0);
            long size_written = fwrite(info.write_data.c_str(),
                                       sizeof(char), args.request_size, fd);
            REQUIRE(size_written == args.request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
        REQUIRE(fs::file_size(info.new_file) == args.request_size);
    }

    SECTION("write to new file always at start") {
        FILE* fd = fopen(info.new_file.c_str(), "w+");
        REQUIRE(fd != nullptr);

        for (int i = 0; i < info.num_iterations; ++i) {
            usleep(info.temporal_interval_ms * 1000);
            long size_written = fwrite(info.write_data.c_str(),
                                       sizeof(char), args.request_size, fd);
            REQUIRE(size_written == args.request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
        REQUIRE(fs::file_size(info.new_file)
        == info.num_iterations * args.request_size);
    }
    posttest();
}

TEST_CASE("BatchedReadSequentialTemporalFixed",
          "[process=1][operation=batched_read]"
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=sequential][file=1][temporal=fixed]") {
    pretest();

    SECTION("read from existing file") {
        FILE* fd = fopen(info.existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        std::string data(args.request_size, '1');
        for (int i = 0; i < info.num_iterations; ++i) {
            usleep(info.temporal_interval_ms * 1000);
            long size_read = fread(data.data(),
                                   sizeof(char), args.request_size, fd);
            REQUIRE(size_read == args.request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }

    SECTION("read from existing file always at start") {
        FILE* fd = fopen(info.existing_file.c_str(), "w+");
        REQUIRE(fd != nullptr);

        for (int i = 0; i < info.num_iterations; ++i) {
            usleep(info.temporal_interval_ms * 1000);
            int status = fseek(fd, 0, SEEK_SET);
            REQUIRE(status == 0);
            long offset = ftell(fd);
            REQUIRE(offset == 0);
            long size_written = fwrite(info.write_data.c_str(),
                                       sizeof(char), args.request_size, fd);
            REQUIRE(size_written == args.request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("BatchedWriteTemporalVariable",
          "[process=1][operation=batched_write]"
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=sequential][file=1][temporal=variable]") {
    pretest();

    SECTION("write to existing file") {
        FILE* fd = fopen(info.new_file.c_str(), "w+");
        REQUIRE(fd != nullptr);

        for (int i = 0; i < info.num_iterations; ++i) {
            info.temporal_interval_ms = rand_r(&info.temporal_interval_seed)
                    % info.temporal_interval_ms + 1;
            usleep(info.temporal_interval_ms * 1000);
            int status = fseek(fd, 0, SEEK_SET);
            REQUIRE(status == 0);
            long offset = ftell(fd);
            REQUIRE(offset == 0);
            long size_written = fwrite(info.write_data.c_str(),
                                       sizeof(char), args.request_size, fd);
            REQUIRE(size_written == args.request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
        REQUIRE(fs::file_size(info.new_file) == args.request_size);
    }

    SECTION("write to new file always at start") {
        FILE* fd = fopen(info.new_file.c_str(), "w+");
        REQUIRE(fd != nullptr);

        for (int i = 0; i < info.num_iterations; ++i) {
            info.temporal_interval_ms = rand_r(&info.temporal_interval_seed)
                                        % info.temporal_interval_ms + 1;
            usleep(info.temporal_interval_ms * 1000);
            long size_written = fwrite(info.write_data.c_str(),
                                       sizeof(char), args.request_size, fd);
            REQUIRE(size_written == args.request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
        REQUIRE(fs::file_size(info.new_file)
        == info.num_iterations * args.request_size);
    }
    posttest();
}

TEST_CASE("BatchedReadSequentialTemporalVariable",
          "[process=1][operation=batched_read]"
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=sequential][file=1][temporal=variable]") {
    pretest();

    SECTION("read from existing file") {
        FILE* fd = fopen(info.existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        std::string data(args.request_size, '1');
        for (int i = 0; i < info.num_iterations; ++i) {
            info.temporal_interval_ms = rand_r(&info.temporal_interval_seed)
                                        % info.temporal_interval_ms + 1;
            usleep(info.temporal_interval_ms * 1000);
            long size_read = fread(data.data(),
                                   sizeof(char), args.request_size, fd);
            REQUIRE(size_read == args.request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }

    SECTION("read from existing file always at start") {
        FILE* fd = fopen(info.existing_file.c_str(), "w+");
        REQUIRE(fd != nullptr);

        for (int i = 0; i < info.num_iterations; ++i) {
            info.temporal_interval_ms = rand_r(&info.temporal_interval_seed)
                                        % info.temporal_interval_ms + 1;
            usleep(info.temporal_interval_ms * 1000);
            int status = fseek(fd, 0, SEEK_SET);
            REQUIRE(status == 0);
            long offset = ftell(fd);
            REQUIRE(offset == 0);
            long size_written = fwrite(info.write_data.c_str(),
                                       sizeof(char), args.request_size, fd);
            REQUIRE(size_written == args.request_size);
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    posttest();
}


TEST_CASE("BatchedMixedSequential",
          "[process=1][operation=batched_mixed]"
          "[request_size=type-fixed][repetition=1024]"
          "[pattern=sequential][file=1]") {
    pretest();
    SECTION("read after write on new file") {
        FILE* fd = fopen(info.new_file.c_str(), "w+");
        REQUIRE(fd != nullptr);
        long last_offset = 0;
        for (int i = 0; i < info.num_iterations; ++i) {
            long size_written = fwrite(info.write_data.data(),
                                       sizeof(char), args.request_size, fd);
            REQUIRE(size_written == args.request_size);
            auto status = fseek(fd, last_offset, SEEK_SET);
            REQUIRE(status == 0);
            long size_read = fread(info.read_data.data(),
                                   sizeof(char), args.request_size, fd);
            REQUIRE(size_read == args.request_size);
            last_offset += args.request_size;
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }

    SECTION("write and read alternative existing file") {
        FILE* fd = fopen(info.existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        for (int i = 0; i < info.num_iterations; ++i) {
            if (i % 2 == 0) {
                long size_written = fwrite(info.write_data.data(),
                                           sizeof(char), args.request_size, fd);
                REQUIRE(size_written == args.request_size);
            } else {
                long size_read = fread(info.read_data.data(),
                                       sizeof(char), args.request_size, fd);
                REQUIRE(size_read == args.request_size);
            }
        }
        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    SECTION("update after read existing file") {
        FILE* fd = fopen(info.existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        long last_offset = 0;
        for (int i = 0; i < info.num_iterations; ++i) {
            long size_read = fread(info.read_data.data(),
                                   sizeof(char), args.request_size, fd);
            REQUIRE(size_read == args.request_size);
            auto status = fseek(fd, last_offset, SEEK_SET);
            REQUIRE(status == 0);
            long size_written = fwrite(info.write_data.data(),
                                       sizeof(char), args.request_size, fd);
            REQUIRE(size_written == args.request_size);
            last_offset += args.request_size;
        }

        int status = fclose(fd);
        REQUIRE(status == 0);
    }
    SECTION("read all after write all on new file in single open") {
        FILE* fd = fopen(info.new_file.c_str(), "w+");
        REQUIRE(fd != nullptr);
        for (int i = 0; i < info.num_iterations; ++i) {
            long size_written = fwrite(info.write_data.data(),
                                       sizeof(char), args.request_size, fd);
            REQUIRE(size_written == args.request_size);
        }
        int status = fseek(fd, 0, SEEK_SET);
        REQUIRE(status == 0);
        for (int i = 0; i < info.num_iterations; ++i) {
            long size_read = fread(info.read_data.data(),
                                   sizeof(char), args.request_size, fd);
            REQUIRE(size_read == args.request_size);
        }
        status = fclose(fd);
        REQUIRE(status == 0);
    }
    SECTION("read all after write all on new file in different open") {
        FILE* fd = fopen(info.new_file.c_str(), "w+");
        REQUIRE(fd != nullptr);
        for (int i = 0; i < info.num_iterations; ++i) {
            long size_written = fwrite(info.write_data.data(),
                                       sizeof(char), args.request_size, fd);
            REQUIRE(size_written == args.request_size);
        }
        auto status = fclose(fd);
        REQUIRE(status == 0);
        FILE* fd2 = fopen(info.new_file.c_str(), "r");
        for (int i = 0; i < info.num_iterations; ++i) {
            long size_read = fread(info.read_data.data(),
                                   sizeof(char), args.request_size, fd2);
            REQUIRE(size_read == args.request_size);
        }
        status = fclose(fd2);
        REQUIRE(status == 0);
    }
    posttest();
}

TEST_CASE("SingleMixed",
          "[process=1][operation=single_mixed]"
          "[request_size=type-fixed][repetition=1]"
          "[file=1]") {
    pretest();
    SECTION("read after write from new file") {
        FILE* fd = fopen(info.new_file.c_str(), "w+");
        REQUIRE(fd != nullptr);
        long offset = ftell(fd);
        REQUIRE(offset == 0);
        long size_write = fwrite(info.write_data.data(), sizeof(char),
                               args.request_size, fd);
        REQUIRE(size_write == args.request_size);
        int status = fseek(fd, 0, SEEK_SET);
        REQUIRE(status == 0);
        long size_read = fread(info.read_data.data(), sizeof(char),
                               args.request_size, fd);
        REQUIRE(size_read == args.request_size);
        status = fclose(fd);
        REQUIRE(status == 0);
    }
    SECTION("update after read from existing file") {
        FILE* fd = fopen(info.existing_file.c_str(), "r+");
        REQUIRE(fd != nullptr);
        long offset = ftell(fd);
        REQUIRE(offset == 0);
        long size_read = fread(info.read_data.data(), sizeof(char),
                               args.request_size, fd);
        REQUIRE(size_read == args.request_size);
        int status = fseek(fd, 0, SEEK_SET);
        REQUIRE(status == 0);
        long size_write = fwrite(info.write_data.data(), sizeof(char),
                                 args.request_size, fd);
        REQUIRE(size_write == args.request_size);

        status = fclose(fd);
        REQUIRE(status == 0);
    }
    SECTION("read after write from new file different opens") {
        FILE* fd = fopen(info.new_file.c_str(), "w+");
        REQUIRE(fd != nullptr);
        long offset = ftell(fd);
        REQUIRE(offset == 0);
        long size_write = fwrite(info.write_data.data(), sizeof(char),
                                 args.request_size, fd);
        REQUIRE(size_write == args.request_size);
        int status = fclose(fd);
        REQUIRE(status == 0);
        FILE* fd2 = fopen(info.existing_file.c_str(), "r+");
        long size_read = fread(info.read_data.data(), sizeof(char),
                               args.request_size, fd2);
        REQUIRE(size_read == args.request_size);
        status = fclose(fd2);
        REQUIRE(status == 0);
    }
    posttest();
}
