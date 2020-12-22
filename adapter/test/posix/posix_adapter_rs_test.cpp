TEST_CASE("BatchedWriteRSRangeSmall",
          "[process=1][operation=batched_write]"
          "[request_size=range-small][repetition=1024]"
          "[pattern=sequential][file=1]") {
    pretest();
    SECTION("write to new file always at the start") {
        int fd = open(info.new_file.c_str(), O_WRONLY | O_CREAT);
        REQUIRE(fd != -1);
        long biggest_size_written = 0;
        for (int i = 0; i < info.num_iterations; ++i) {
            long offset = lseek(fd, 0, SEEK_SET);
            REQUIRE(offset == 0);
            long request_size = info.small_min +
                                (rand_r(&info.rs_seed) % info.small_max);
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
        int fd = open(info.new_file.c_str(), O_WRONLY | O_CREAT);
        REQUIRE(fd != -1);
        long total_size_written = 0;
        for (int i = 0; i < info.num_iterations; ++i) {
            long request_size = info.small_min +
                                (rand_r(&info.rs_seed) % info.small_max);
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


TEST_CASE("BatchedReadSequentialRSRangeSmall",
          "[process=1][operation=batched_read]"
          "[request_size=range-small][repetition=1024]"
          "[pattern=sequential][file=1]") {
    pretest();
    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDONLY);
        REQUIRE(fd != -1);
        long current_offset = 0;
        for (int i = 0; i < info.num_iterations; ++i) {
            long request_size = info.small_min +
                                (rand_r(&info.rs_seed) % info.small_max);
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
            long request_size = info.small_min +
                                (rand_r(&info.rs_seed) % info.small_max);
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

TEST_CASE("BatchedReadRandomRSRangeSmall",
          "[process=1][operation=batched_read]"
          "[request_size=range-small]"
          "[repetition=1024][pattern=random][file=1]") {
    pretest();

    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = rand_r(&info.offset_seed) %
                    (info.total_size - info.small_max);
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long request_size = info.small_min +
                                (rand_r(&info.rs_seed)
                                 % info.small_max);
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

TEST_CASE("BatchedUpdateRandomRSRangeSmall",
          "[process=1][operation=batched_write]"
          "[request_size=range-small][repetition=1024]"
          "[pattern=random][file=1]") {
    pretest();

    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = rand_r(&info.offset_seed)
                    % (info.total_size - info.small_max);
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long request_size = info.small_min +
                                (rand_r(&info.rs_seed) % info.small_max);
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

TEST_CASE("BatchedReadStrideFixedRSRangeSmall",
          "[process=1][operation=batched_read]"
          "[request_size=range-small][repetition=1024]"
          "[pattern=stride_fixed][file=1]") {
    pretest();
    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = (i * info.stride_size)
                    % (info.total_size - info.small_max);
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long request_size = info.small_min +
                                (rand_r(&info.rs_seed)
                                 % info.small_max);
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

TEST_CASE("BatchedUpdateStrideFixedRSRangeSmall",
          "[process=1][operation=batched_write]"
          "[request_size=range-small][repetition=1024]"
          "[pattern=stride_fixed][file=1]") {
    pretest();
    SECTION("write to existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = (i * info.stride_size)
                    % (info.total_size - info.small_max);
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long request_size = info.small_min +
                                (rand_r(&info.rs_seed) % info.small_max);
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

TEST_CASE("BatchedReadStrideDynamicRSRangeSmall",
          "[process=1][operation=batched_read]"
          "[request_size=range-small][repetition=1024]"
          "[pattern=stride_dynamic][file=1]") {
    pretest();
    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = abs(((i * rand_r(&info.offset_seed))
                    % info.stride_size)
                              % (info.total_size - info.small_max));
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long request_size = info.small_min +
                                (rand_r(&info.rs_seed) % info.small_max);
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

TEST_CASE("BatchedUpdateStrideDynamicRSRangeSmall",
          "[process=1][operation=batched_write]"
          "[request_size=range-small][repetition=1024]"
          "[pattern=stride_dynamic][file=1]") {
    pretest();
    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = abs(((i * rand_r(&info.offset_seed))
                            % info.stride_size)
                              % (info.total_size - info.small_max));
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long request_size = info.small_min +
                                (rand_r(&info.rs_seed) % info.small_max);
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

TEST_CASE("BatchedReadStrideNegativeRSRangeSmall",
          "[process=1][operation=batched_read]"
          "[request_size=range-small][repetition=1024]"
          "[pattern=stride_negative][file=1]") {
    pretest();
    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = (info.total_size - i * info.stride_size)
                          % (info.total_size - info.small_max);
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long request_size = info.small_min +
                                (rand_r(&info.rs_seed) % info.small_max);
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

TEST_CASE("BatchedUpdateStrideNegativeRSRangeSmall",
          "[process=1][operation=batched_write]"
          "[request_size=range-small][repetition=1024]"
          "[pattern=stride_negative][file=1]") {
    pretest();
    SECTION("write to existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = info.total_size - ((i * info.stride_size)
                                        % (info.total_size - info.small_max));
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long request_size = info.small_min +
                                (rand_r(&info.rs_seed) % info.small_max);
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

TEST_CASE("BatchedReadStride2DRSRangeSmall",
          "[process=1][operation=batched_read]"
          "[request_size=range-small][repetition=1024]"
          "[pattern=stride_2d][file=1]") {
    pretest();
    long rows = sqrt(info.total_size);
    long cols = rows;
    REQUIRE(rows * cols == info.total_size);
    long cell_size = 128;
    long cell_stride = rows * cols / cell_size / info.num_iterations;
    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        long prev_cell_col = 0, prev_cell_row = 0;
        for (int i = 0; i < info.num_iterations; ++i) {
            long current_cell_col = (prev_cell_col + cell_stride) % cols;
            long current_cell_row = prev_cell_col + cell_stride > cols ?
                                    prev_cell_row + 1 : prev_cell_row;
            prev_cell_row = current_cell_row;
            auto offset = (current_cell_col * cell_stride
                            + prev_cell_row * cols)
                          % (info.total_size - info.small_max);
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long request_size = info.small_min +
                                (rand_r(&info.rs_seed)
                                 % info.small_max);
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

TEST_CASE("BatchedUpdateStride2DRSRangeSmall",
          "[process=1][operation=batched_write]"
          "[request_size=range-small][repetition=1024]"
          "[pattern=stride_2d][file=1]") {
    pretest();
    long rows = sqrt(info.total_size);
    long cols = rows;
    REQUIRE(rows * cols == info.total_size);
    long cell_size = 128;
    long cell_stride = rows * cols / cell_size / info.num_iterations;
    SECTION("write to existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        long prev_cell_col = 0, prev_cell_row = 0;
        for (int i = 0; i < info.num_iterations; ++i) {
            long current_cell_col = (prev_cell_col + cell_stride) % cols;
            long current_cell_row = prev_cell_col + cell_stride > cols ?
                                    prev_cell_row + 1 : prev_cell_row;
            prev_cell_row = current_cell_row;
            auto offset = (current_cell_col * cell_stride
                    + prev_cell_row * cols)
                          % (info.total_size - info.small_max);
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long request_size = info.small_min +
                                (rand_r(&info.rs_seed)
                                 % info.small_max);
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
 * Medium RS
 **/

TEST_CASE("BatchedWriteRSRangeMedium",
          "[process=1][operation=batched_write]"
          "[request_size=range-medium][repetition=1024]"
          "[pattern=sequential][file=1]") {
    pretest();
    SECTION("write to new file always at the start") {
        int fd = open(info.new_file.c_str(), O_WRONLY | O_CREAT);
        REQUIRE(fd != -1);
        long biggest_size_written = 0;
        for (int i = 0; i < info.num_iterations; ++i) {
            int offset = lseek(fd, 0, SEEK_SET);
            REQUIRE(offset == 0);
            long request_size = info.medium_min +
                                (rand_r(&info.rs_seed) % info.medium_max);
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
        int fd = open(info.new_file.c_str(), O_WRONLY | O_CREAT);
        REQUIRE(fd != -1);
        long total_size_written = 0;
        for (int i = 0; i < info.num_iterations; ++i) {
            long request_size = info.medium_min +
                                (rand_r(&info.rs_seed) % info.medium_max);
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


TEST_CASE("BatchedReadSequentialRSRangeMedium",
          "[process=1][operation=batched_read]"
          "[request_size=range-medium][repetition=1024]"
          "[pattern=sequential][file=1]") {
    pretest();
    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDONLY);
        REQUIRE(fd != -1);
        std::string data(args.request_size, '1');
        long current_offset = 0;
        for (int i = 0; i < info.num_iterations; ++i) {
            long request_size = (info.medium_min +
                                (rand_r(&info.rs_seed) % info.medium_max))
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
            long offset = lseek(fd, 0, SEEK_SET);
            REQUIRE(offset == 0);
            long request_size = info.medium_min +
                                (rand_r(&info.rs_seed) % info.medium_max);
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

TEST_CASE("BatchedReadRandomRSRangeMedium",
          "[process=1][operation=batched_read]"
          "[request_size=range-medium]"
          "[repetition=1024][pattern=random][file=1]") {
    pretest();

    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = rand_r(&info.offset_seed) %
                          (info.total_size - info.medium_max);
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long request_size = info.medium_min +
                                (rand_r(&info.rs_seed)
                                 % info.medium_max);
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

TEST_CASE("BatchedUpdateRandomRSRangeMedium",
          "[process=1][operation=batched_write]"
          "[request_size=range-medium][repetition=1024]"
          "[pattern=random][file=1]") {
    pretest();

    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = rand_r(&info.offset_seed)
                    % (info.total_size - info.medium_max);
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long request_size = info.medium_min +
                                (rand_r(&info.rs_seed) % info.medium_max);
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

TEST_CASE("BatchedReadStrideFixedRSRangeMedium",
          "[process=1][operation=batched_read]"
          "[request_size=range-medium][repetition=1024]"
          "[pattern=stride_fixed][file=1]") {
    pretest();
    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = (i * info.stride_size)
                    % (info.total_size - info.medium_max);
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long request_size = info.medium_min +
                                (rand_r(&info.rs_seed)
                                 % info.medium_max);
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

TEST_CASE("BatchedUpdateStrideFixedRSRangeMedium",
          "[process=1][operation=batched_write]"
          "[request_size=range-medium][repetition=1024]"
          "[pattern=stride_fixed][file=1]") {
    pretest();
    SECTION("write to existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = (i * info.stride_size)
                    % (info.total_size - info.medium_max);
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long request_size = info.medium_min +
                                (rand_r(&info.rs_seed) % info.medium_max);
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

TEST_CASE("BatchedReadStrideDynamicRSRangeMedium",
          "[process=1][operation=batched_read]"
          "[request_size=range-medium][repetition=1024]"
          "[pattern=stride_dynamic][file=1]") {
    pretest();
    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = abs(((i * rand_r(&info.offset_seed))
                               % info.stride_size)
                              % (info.total_size - info.medium_max));
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long request_size = info.medium_min +
                                (rand_r(&info.rs_seed) % info.medium_max);
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

TEST_CASE("BatchedUpdateStrideDynamicRSRangeMedium",
          "[process=1][operation=batched_write]"
          "[request_size=range-medium][repetition=1024]"
          "[pattern=stride_dynamic][file=1]") {
    pretest();
    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = abs(((i * rand_r(&info.offset_seed))
                               % info.stride_size)
                              % (info.total_size - info.medium_max));
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long request_size = info.medium_min +
                                (rand_r(&info.rs_seed) % info.medium_max);
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

TEST_CASE("BatchedReadStrideNegativeRSRangeMedium",
          "[process=1][operation=batched_read]"
          "[request_size=range-medium][repetition=1024]"
          "[pattern=stride_negative][file=1]") {
    pretest();
    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = (info.total_size - i * info.stride_size)
                          % (info.total_size - info.medium_max);
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long request_size = info.medium_min +
                                (rand_r(&info.rs_seed) % info.medium_max);
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

TEST_CASE("BatchedUpdateStrideNegativeRSRangeMedium",
          "[process=1][operation=batched_write]"
          "[request_size=range-medium][repetition=1024]"
          "[pattern=stride_negative][file=1]") {
    pretest();
    SECTION("write to existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = info.total_size - ((i * info.stride_size)
                        % (info.total_size - info.medium_max));
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long request_size = info.medium_min +
                                (rand_r(&info.rs_seed) % info.medium_max);
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

TEST_CASE("BatchedReadStride2DRSRangeMedium",
          "[process=1][operation=batched_read]"
          "[request_size=range-medium][repetition=1024]"
          "[pattern=stride_2d][file=1]") {
    pretest();
    long rows = sqrt(info.total_size);
    long cols = rows;
    REQUIRE(rows * cols == info.total_size);
    long cell_size = 128;
    long cell_stride = rows * cols / cell_size / info.num_iterations;
    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        long prev_cell_col = 0, prev_cell_row = 0;
        for (int i = 0; i < info.num_iterations; ++i) {
            long current_cell_col = (prev_cell_col + cell_stride) % cols;
            long current_cell_row = prev_cell_col + cell_stride > cols ?
                                    prev_cell_row + 1 : prev_cell_row;
            prev_cell_row = current_cell_row;
            auto offset = (current_cell_col * cell_stride
                           + prev_cell_row * cols)
                          % (info.total_size - info.medium_max);
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long request_size = info.medium_min +
                                (rand_r(&info.rs_seed)
                                 % info.medium_max);
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

TEST_CASE("BatchedUpdateStride2DRSRangeMedium",
          "[process=1][operation=batched_write]"
          "[request_size=range-medium][repetition=1024]"
          "[pattern=stride_2d][file=1]") {
    pretest();
    long rows = sqrt(info.total_size);
    long cols = rows;
    REQUIRE(rows * cols == info.total_size);
    long cell_size = 128;
    long cell_stride = rows * cols / cell_size / info.num_iterations;
    SECTION("write to existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        long prev_cell_col = 0, prev_cell_row = 0;
        for (int i = 0; i < info.num_iterations; ++i) {
            long current_cell_col = (prev_cell_col + cell_stride) % cols;
            long current_cell_row = prev_cell_col + cell_stride > cols ?
                                    prev_cell_row + 1 : prev_cell_row;
            prev_cell_row = current_cell_row;
            auto offset = (current_cell_col * cell_stride
                           + prev_cell_row * cols)
                          % (info.total_size - info.medium_max);
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long request_size = info.medium_min +
                                (rand_r(&info.rs_seed)
                                 % info.medium_max);
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
 * Large RS
 **/

TEST_CASE("BatchedWriteRSRangeLarge",
          "[process=1][operation=batched_write]"
          "[request_size=range-large][repetition=1024]"
          "[pattern=sequential][file=1]") {
    pretest();
    SECTION("write to new file always at the start") {
        int fd = open(info.new_file.c_str(), O_WRONLY | O_CREAT);
        REQUIRE(fd != -1);
        long biggest_size_written = 0;
        for (int i = 0; i < info.num_iterations; ++i) {
            long offset = lseek(fd, 0, SEEK_SET);
            REQUIRE(offset == 0);
            long request_size = info.large_min +
                                (rand_r(&info.rs_seed) % info.large_max);
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
        int fd = open(info.new_file.c_str(), O_WRONLY | O_CREAT);
        REQUIRE(fd != -1);
        long total_size_written = 0;
        for (int i = 0; i < info.num_iterations; ++i) {
            long request_size = info.large_min +
                                (rand_r(&info.rs_seed) % info.large_max);
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


TEST_CASE("BatchedReadSequentialRSRangeLarge",
          "[process=1][operation=batched_read]"
          "[request_size=range-large][repetition=1024]"
          "[pattern=sequential][file=1]") {
    pretest();
    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDONLY);
        REQUIRE(fd != -1);
        std::string data(args.request_size, '1');
        long current_offset = 0;
        for (int i = 0; i < info.num_iterations; ++i) {
            long request_size = (info.large_min +
                                (rand_r(&info.rs_seed) % info.large_max))
                                % (info.total_size - current_offset);;
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
            long offset = lseek(fd, 0, SEEK_SET);
            REQUIRE(offset == 0);
            long request_size = info.large_min +
                                (rand_r(&info.rs_seed) % info.large_max);
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

TEST_CASE("BatchedReadRandomRSRangeLarge",
          "[process=1][operation=batched_read]"
          "[request_size=range-large]"
          "[repetition=1024][pattern=random][file=1]") {
    pretest();

    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = rand_r(&info.offset_seed) %
                          (info.total_size - info.large_max);
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long request_size = (info.large_min +
                                (rand_r(&info.rs_seed)
                                 % info.large_max))
                                % (info.total_size - offset);;
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

TEST_CASE("BatchedUpdateRandomRSRangeLarge",
          "[process=1][operation=batched_write]"
          "[request_size=range-large][repetition=1024]"
          "[pattern=random][file=1]") {
    pretest();

    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = rand_r(&info.offset_seed)
                    % (info.total_size - info.large_max);
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long request_size = info.large_min +
                                (rand_r(&info.rs_seed) % info.large_max);
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

TEST_CASE("BatchedReadStrideFixedRSRangeLarge",
          "[process=1][operation=batched_read]"
          "[request_size=range-large][repetition=1024]"
          "[pattern=stride_fixed][file=1]") {
    pretest();
    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = (i * info.stride_size)
                    % (info.total_size - info.large_max);
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long request_size = info.large_min +
                                (rand_r(&info.rs_seed)
                                 % info.large_max);
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

TEST_CASE("BatchedUpdateStrideFixedRSRangeLarge",
          "[process=1][operation=batched_write]"
          "[request_size=range-large][repetition=1024]"
          "[pattern=stride_fixed][file=1]") {
    pretest();
    SECTION("write to existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = (i * info.stride_size)
                    % (info.total_size - info.large_max);
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long request_size = info.large_min +
                                (rand_r(&info.rs_seed) % info.large_max);
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

TEST_CASE("BatchedReadStrideDynamicRSRangeLarge",
          "[process=1][operation=batched_read]"
          "[request_size=range-large][repetition=1024]"
          "[pattern=stride_dynamic][file=1]") {
    pretest();
    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = abs(((i * rand_r(&info.offset_seed))
                               % info.stride_size)
                              % (info.total_size - info.large_max));
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long request_size = info.large_min +
                                (rand_r(&info.rs_seed) % info.large_max);
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

TEST_CASE("BatchedUpdateStrideDynamicRSRangeLarge",
          "[process=1][operation=batched_write]"
          "[request_size=range-large][repetition=1024]"
          "[pattern=stride_dynamic][file=1]") {
    pretest();
    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = abs(((i * rand_r(&info.offset_seed))
                               % info.stride_size)
                              % (info.total_size - info.large_max));
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long request_size = info.large_min +
                                (rand_r(&info.rs_seed) % info.large_max);
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

TEST_CASE("BatchedReadStrideNegativeRSRangeLarge",
          "[process=1][operation=batched_read]"
          "[request_size=range-large][repetition=1024]"
          "[pattern=stride_negative][file=1]") {
    pretest();
    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = (info.total_size - i * info.stride_size)
                          % (info.total_size - info.large_max);
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long request_size = (info.large_min +
                                (rand_r(&info.rs_seed) % info.large_max))
                                % (info.total_size - info.large_max);
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

TEST_CASE("BatchedUpdateStrideNegativeRSRangeLarge",
          "[process=1][operation=batched_write]"
          "[request_size=range-large][repetition=1024]"
          "[pattern=stride_negative][file=1]") {
    pretest();
    SECTION("write to existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        for (int i = 0; i < info.num_iterations; ++i) {
            auto offset = info.total_size - ((i * info.stride_size)
                        % (info.total_size - info.large_max));
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long request_size = info.large_min +
                                (rand_r(&info.rs_seed) % info.large_max);
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

TEST_CASE("BatchedReadStride2DRSRangeLarge",
          "[process=1][operation=batched_read]"
          "[request_size=range-large][repetition=1024]"
          "[pattern=stride_2d][file=1]") {
    pretest();
    long rows = sqrt(info.total_size);
    long cols = rows;
    REQUIRE(rows * cols == info.total_size);
    long cell_size = 128;
    long cell_stride = rows * cols / cell_size / info.num_iterations;
    SECTION("read from existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        long prev_cell_col = 0, prev_cell_row = 0;
        for (int i = 0; i < info.num_iterations; ++i) {
            long current_cell_col = (prev_cell_col + cell_stride) % cols;
            long current_cell_row = prev_cell_col + cell_stride > cols ?
                                    prev_cell_row + 1 : prev_cell_row;
            prev_cell_row = current_cell_row;
            auto offset = (current_cell_col * cell_stride
                           + prev_cell_row * cols)
                          % (info.total_size - info.large_max);
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long request_size = info.large_min +
                                (rand_r(&info.rs_seed)
                                 % info.large_max);
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

TEST_CASE("BatchedUpdateStride2DRSRangeLarge",
          "[process=1][operation=batched_write]"
          "[request_size=range-large][repetition=1024]"
          "[pattern=stride_2d][file=1]") {
    pretest();
    long rows = sqrt(info.total_size);
    long cols = rows;
    REQUIRE(rows * cols == info.total_size);
    long cell_size = 128;
    long cell_stride = rows * cols / cell_size / info.num_iterations;
    SECTION("write to existing file") {
        int fd = open(info.existing_file.c_str(), O_RDWR);
        REQUIRE(fd != -1);
        long prev_cell_col = 0, prev_cell_row = 0;
        for (int i = 0; i < info.num_iterations; ++i) {
            long current_cell_col = (prev_cell_col + cell_stride) % cols;
            long current_cell_row = prev_cell_col + cell_stride > cols ?
                                    prev_cell_row + 1 : prev_cell_row;
            prev_cell_row = current_cell_row;
            auto offset = (current_cell_col * cell_stride
                           + prev_cell_row * cols)
                          % (info.total_size - info.large_max);
            auto status = lseek(fd, offset, SEEK_SET);
            REQUIRE(status == offset);
            long request_size = info.large_min +
                                (rand_r(&info.rs_seed)
                                 % info.large_max);
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
