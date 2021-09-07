/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Distributed under BSD 3-Clause license.                                   *
 * Copyright by The HDF Group.                                               *
 * Copyright by the Illinois Institute of Technology.                        *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of Hermes. The full Hermes copyright notice, including  *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the top directory. If you do not  *
 * have access to the file, you may request a copy from help@hdfgroup.org.   *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

TEST_CASE("BatchedWriteRSRangeSmall",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              ""
              "[operation=batched_write]"
              "[request_size=range-small][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=sequential][file=1]") {
  pretest();
  SECTION("write to new file always at the start") {
    test::test_fopen(info.new_file.c_str(), "w+");
    REQUIRE(test::fh_orig != nullptr);
    size_t biggest_written = 0;
    for (size_t i = 0; i < info.num_iterations; ++i) {
      test::test_fseek(0, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t offset = ftell(test::fh_orig);
      REQUIRE(offset == 0);
      size_t request_size =
          info.small_min + (rand_r(&info.rs_seed) % info.small_max);
      std::string data(request_size, '1');
      test::test_fwrite(data.data(), request_size);
      REQUIRE(test::size_written_orig == request_size);
      if (biggest_written < request_size) biggest_written = request_size;
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
    REQUIRE(fs::file_size(info.new_file) == biggest_written);
  }

  SECTION("write to new file") {
    test::test_fopen(info.new_file.c_str(), "w+");
    REQUIRE(test::fh_orig != nullptr);
    size_t total_written = 0;
    for (size_t i = 0; i < info.num_iterations; ++i) {
      size_t request_size =
          info.small_min + (rand_r(&info.rs_seed) % info.small_max);
      std::string data(request_size, '1');
      test::test_fwrite(data.data(), request_size);
      REQUIRE(test::size_written_orig == request_size);
      total_written += test::size_written_orig;
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
    REQUIRE(fs::file_size(info.new_file) == total_written);
  }
  posttest();
}

TEST_CASE("BatchedReadSequentialRSRangeSmall",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-small][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=sequential][file=1]") {
  pretest();
  SECTION("read from existing file") {
    test::test_fopen(info.existing_file.c_str(), "r");
    REQUIRE(test::fh_orig != nullptr);
    std::string data(args.request_size, '1');
    size_t current_offset = 0;
    for (size_t i = 0; i < info.num_iterations; ++i) {
      size_t request_size =
          info.small_min + (rand_r(&info.rs_seed) % info.small_max);
      std::string data(request_size, '1');
      test::test_fread(data.data(), request_size);
      REQUIRE(test::size_read_orig == request_size);
      current_offset += test::size_read_orig;
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }

  SECTION("read from existing file always at start") {
    test::test_fopen(info.existing_file.c_str(), "r");
    REQUIRE(test::fh_orig != nullptr);

    for (size_t i = 0; i < info.num_iterations; ++i) {
      test::test_fseek(0, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t offset = ftell(test::fh_orig);
      REQUIRE(offset == 0);
      size_t request_size =
          info.small_min + (rand_r(&info.rs_seed) % info.small_max);
      std::string data(request_size, '1');
      test::test_fread(data.data(), request_size);
      REQUIRE(test::size_read_orig == request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedReadRandomRSRangeSmall",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-small]"
              "[repetition=" +
              std::to_string(info.num_iterations) +
              "][pattern=random][file=1]") {
  pretest();

  SECTION("read from existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      auto offset =
          rand_r(&info.offset_seed) % (info.total_size - info.small_max);
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t request_size =
          info.small_min + (rand_r(&info.rs_seed) % info.small_max);
      std::string data(request_size, '1');
      test::test_fread(data.data(), request_size);
      REQUIRE(test::size_read_orig == request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedUpdateRandomRSRangeSmall",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-small][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=random][file=1]") {
  pretest();

  SECTION("read from existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      auto offset =
          rand_r(&info.offset_seed) % (info.total_size - info.small_max);
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t request_size =
          info.small_min + (rand_r(&info.rs_seed) % info.small_max);
      std::string data(request_size, '1');
      test::test_fwrite(data.data(), request_size);
      REQUIRE(test::size_written_orig == request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedReadStrideFixedRSRangeSmall",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-small][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=stride_fixed][file=1]") {
  pretest();
  SECTION("read from existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      auto offset = (i * info.stride_size) % (info.total_size - info.small_max);
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t request_size =
          info.small_min + (rand_r(&info.rs_seed) % info.small_max);
      std::string data(request_size, '1');
      test::test_fread(data.data(), request_size);
      REQUIRE(test::size_read_orig == request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedUpdateStrideFixedRSRangeSmall",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-small][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=stride_fixed][file=1]") {
  pretest();
  SECTION("write to existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      auto offset = (i * info.stride_size) % (info.total_size - info.small_max);
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t request_size =
          info.small_min + (rand_r(&info.rs_seed) % info.small_max);
      std::string data(request_size, '1');
      test::test_fwrite(data.data(), request_size);
      REQUIRE(test::size_written_orig == request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedReadStrideDynamicRSRangeSmall",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-small][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=stride_dynamic][file=1]") {
  pretest();
  SECTION("read from existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      auto offset = GetRandomOffset(i, info.offset_seed, info.stride_size,
                                    info.total_size - info.small_max);
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t request_size =
          info.small_min + (rand_r(&info.rs_seed) % info.small_max);
      std::string data(request_size, '1');
      test::test_fread(data.data(), request_size);
      REQUIRE(test::size_read_orig == request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedUpdateStrideDynamicRSRangeSmall",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-small][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=stride_dynamic][file=1]") {
  pretest();
  SECTION("read from existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      auto offset = GetRandomOffset(i, info.offset_seed, info.stride_size,
                                    info.total_size - info.small_max);
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t request_size =
          info.small_min + (rand_r(&info.rs_seed) % info.small_max);
      std::string data(request_size, '1');
      test::test_fwrite(data.data(), request_size);
      REQUIRE(test::size_written_orig == request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedReadStrideNegativeRSRangeSmall",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-small][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=stride_negative][file=1]") {
  pretest();
  SECTION("read from existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      auto offset = (info.total_size - i * info.stride_size) %
                    (info.total_size - info.small_max);
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t request_size =
          info.small_min + (rand_r(&info.rs_seed) % info.small_max);
      std::string data(request_size, '1');
      test::test_fread(data.data(), request_size);
      REQUIRE(test::size_read_orig == request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedUpdateStrideNegativeRSRangeSmall",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              ""
              "[operation=batched_write]"
              "[request_size=range-small][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=stride_negative][file=1]") {
  pretest();
  SECTION("write to existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      auto offset = info.total_size - ((i * info.stride_size) %
                                       (info.total_size - info.small_max));
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t request_size =
          info.small_min + (rand_r(&info.rs_seed) % info.small_max);
      std::string data(request_size, '1');
      test::test_fwrite(data.data(), request_size);
      REQUIRE(test::size_written_orig == request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedReadStride2DRSRangeSmall",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-small][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=stride_2d][file=1]") {
  pretest();
  size_t rows = sqrt(info.total_size);
  size_t cols = rows;
  REQUIRE(rows * cols == info.total_size);
  size_t cell_size = 128;
  size_t cell_stride = rows * cols / cell_size / info.num_iterations;
  SECTION("read from existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    size_t prev_cell_col = 0, prev_cell_row = 0;
    for (size_t i = 0; i < info.num_iterations; ++i) {
      size_t current_cell_col = (prev_cell_col + cell_stride) % cols;
      size_t current_cell_row = prev_cell_col + cell_stride > cols
                                    ? prev_cell_row + 1
                                    : prev_cell_row;
      prev_cell_row = current_cell_row;
      auto offset = (current_cell_col * cell_stride + prev_cell_row * cols) %
                    (info.total_size - info.small_max);
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t request_size =
          info.small_min + (rand_r(&info.rs_seed) % info.small_max);
      std::string data(request_size, '1');
      test::test_fread(data.data(), request_size);
      REQUIRE(test::size_read_orig == request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedUpdateStride2DRSRangeSmall",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-small][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=stride_2d][file=1]") {
  pretest();
  size_t rows = sqrt(info.total_size);
  size_t cols = rows;
  REQUIRE(rows * cols == info.total_size);
  size_t cell_size = 128;
  size_t cell_stride = rows * cols / cell_size / info.num_iterations;
  SECTION("write to existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    size_t prev_cell_col = 0, prev_cell_row = 0;
    for (size_t i = 0; i < info.num_iterations; ++i) {
      size_t current_cell_col = (prev_cell_col + cell_stride) % cols;
      size_t current_cell_row = prev_cell_col + cell_stride > cols
                                    ? prev_cell_row + 1
                                    : prev_cell_row;
      prev_cell_row = current_cell_row;
      auto offset = (current_cell_col * cell_stride + prev_cell_row * cols) %
                    (info.total_size - info.small_max);
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t request_size =
          info.small_min + (rand_r(&info.rs_seed) % info.small_max);
      std::string data(request_size, '1');
      test::test_fwrite(data.data(), request_size);
      REQUIRE(test::size_written_orig == request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}
/**
 * Medium RS
 **/

TEST_CASE("BatchedWriteRSRangeMedium",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-medium][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=sequential][file=1]") {
  pretest();
  SECTION("write to new file always at the start") {
    test::test_fopen(info.new_file.c_str(), "w+");
    REQUIRE(test::fh_orig != nullptr);
    size_t biggest_written = 0;
    for (size_t i = 0; i < info.num_iterations; ++i) {
      test::test_fseek(0, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t offset = ftell(test::fh_orig);
      REQUIRE(offset == 0);
      size_t request_size =
          info.medium_min + (rand_r(&info.rs_seed) % info.medium_max);
      std::string data(request_size, '1');
      test::test_fwrite(data.data(), request_size);
      REQUIRE(test::size_written_orig == request_size);
      if (biggest_written < request_size) biggest_written = request_size;
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
    REQUIRE(fs::file_size(info.new_file) == biggest_written);
  }

  SECTION("write to new file") {
    test::test_fopen(info.new_file.c_str(), "w+");
    REQUIRE(test::fh_orig != nullptr);
    size_t total_written = 0;
    for (size_t i = 0; i < info.num_iterations; ++i) {
      size_t request_size =
          info.medium_min + (rand_r(&info.rs_seed) % info.medium_max);
      std::string data(request_size, '1');
      test::test_fwrite(data.data(), request_size);
      REQUIRE(test::size_written_orig == request_size);
      total_written += test::size_written_orig;
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedReadSequentialRSRangeMedium",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-medium][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=sequential][file=1]") {
  pretest();
  SECTION("read from existing file") {
    test::test_fopen(info.existing_file.c_str(), "r");
    REQUIRE(test::fh_orig != nullptr);
    std::string data(args.request_size, '1');
    size_t current_offset = 0;
    for (size_t i = 0; i < info.num_iterations; ++i) {
      size_t request_size =
          (info.medium_min + (rand_r(&info.rs_seed) % info.medium_max)) %
          (info.total_size - current_offset);
      std::string data(request_size, '1');
      test::test_fread(data.data(), request_size);
      REQUIRE(test::size_read_orig == request_size);
      current_offset += test::size_read_orig;
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }

  SECTION("read from existing file always at start") {
    test::test_fopen(info.existing_file.c_str(), "r");
    REQUIRE(test::fh_orig != nullptr);

    for (size_t i = 0; i < info.num_iterations; ++i) {
      test::test_fseek(0, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t offset = ftell(test::fh_orig);
      REQUIRE(offset == 0);
      size_t request_size =
          info.medium_min + (rand_r(&info.rs_seed) % info.medium_max);
      std::string data(request_size, '1');
      test::test_fread(data.data(), request_size);
      REQUIRE(test::size_read_orig == request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedReadRandomRSRangeMedium",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-medium]"
              "[repetition=" +
              std::to_string(info.num_iterations) +
              "][pattern=random][file=1]") {
  pretest();

  SECTION("read from existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      auto offset =
          rand_r(&info.offset_seed) % (info.total_size - info.medium_max);
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t request_size =
          info.medium_min + (rand_r(&info.rs_seed) % info.medium_max);
      std::string data(request_size, '1');
      test::test_fread(data.data(), request_size);
      REQUIRE(test::size_read_orig == request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedUpdateRandomRSRangeMedium",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-medium][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=random][file=1]") {
  pretest();

  SECTION("read from existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      auto offset =
          rand_r(&info.offset_seed) % (info.total_size - info.medium_max);
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t request_size =
          info.medium_min + (rand_r(&info.rs_seed) % info.medium_max);
      std::string data(request_size, '1');
      test::test_fwrite(data.data(), request_size);
      REQUIRE(test::size_written_orig == request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedReadStrideFixedRSRangeMedium",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-medium][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=stride_fixed][file=1]") {
  pretest();
  SECTION("read from existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      auto offset =
          (i * info.stride_size) % (info.total_size - info.medium_max);
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t request_size =
          info.medium_min + (rand_r(&info.rs_seed) % info.medium_max);
      std::string data(request_size, '1');
      test::test_fread(data.data(), request_size);
      REQUIRE(test::size_read_orig == request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedUpdateStrideFixedRSRangeMedium",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-medium][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=stride_fixed][file=1]") {
  pretest();
  SECTION("write to existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      auto offset =
          (i * info.stride_size) % (info.total_size - info.medium_max);
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t request_size =
          info.medium_min + (rand_r(&info.rs_seed) % info.medium_max);
      std::string data(request_size, '1');
      test::test_fwrite(data.data(), request_size);
      REQUIRE(test::size_written_orig == request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedReadStrideDynamicRSRangeMedium",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-medium][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=stride_dynamic][file=1]") {
  pretest();
  SECTION("read from existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      auto offset = GetRandomOffset(i, info.offset_seed, info.stride_size,
                                    info.total_size - info.medium_max);
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t request_size =
          info.medium_min + (rand_r(&info.rs_seed) % info.medium_max);
      std::string data(request_size, '1');
      test::test_fread(data.data(), request_size);
      REQUIRE(test::size_read_orig == request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedUpdateStrideDynamicRSRangeMedium",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-medium][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=stride_dynamic][file=1]") {
  pretest();
  SECTION("read from existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      auto offset = GetRandomOffset(i, info.offset_seed, info.stride_size,
                                    info.total_size - info.medium_max);
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t request_size =
          info.medium_min + (rand_r(&info.rs_seed) % info.medium_max);
      std::string data(request_size, '1');
      test::test_fwrite(data.data(), request_size);
      REQUIRE(test::size_written_orig == request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedReadStrideNegativeRSRangeMedium",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-medium][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=stride_negative][file=1]") {
  pretest();
  SECTION("read from existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      auto offset = (info.total_size - i * info.stride_size) %
                    (info.total_size - info.medium_max);
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t request_size =
          info.medium_min + (rand_r(&info.rs_seed) % info.medium_max);
      std::string data(request_size, '1');
      test::test_fread(data.data(), request_size);
      REQUIRE(test::size_read_orig == request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedUpdateStrideNegativeRSRangeMedium",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-medium][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=stride_negative][file=1]") {
  pretest();
  SECTION("write to existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      auto offset = info.total_size - ((i * info.stride_size) %
                                       (info.total_size - info.medium_max));
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t request_size =
          info.medium_min + (rand_r(&info.rs_seed) % info.medium_max);
      std::string data(request_size, '1');
      test::test_fwrite(data.data(), request_size);
      REQUIRE(test::size_written_orig == request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedReadStride2DRSRangeMedium",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-medium][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=stride_2d][file=1]") {
  pretest();
  size_t rows = sqrt(info.total_size);
  size_t cols = rows;
  REQUIRE(rows * cols == info.total_size);
  size_t cell_size = 128;
  size_t cell_stride = rows * cols / cell_size / info.num_iterations;
  SECTION("read from existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    size_t prev_cell_col = 0, prev_cell_row = 0;
    for (size_t i = 0; i < info.num_iterations; ++i) {
      size_t current_cell_col = (prev_cell_col + cell_stride) % cols;
      size_t current_cell_row = prev_cell_col + cell_stride > cols
                                    ? prev_cell_row + 1
                                    : prev_cell_row;
      prev_cell_row = current_cell_row;
      auto offset = (current_cell_col * cell_stride + prev_cell_row * cols) %
                    (info.total_size - info.medium_max);
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t request_size =
          info.medium_min + (rand_r(&info.rs_seed) % info.medium_max);
      std::string data(request_size, '1');
      test::test_fread(data.data(), request_size);
      REQUIRE(test::size_read_orig == request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedUpdateStride2DRSRangeMedium",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-medium][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=stride_2d][file=1]") {
  pretest();
  size_t rows = sqrt(info.total_size);
  size_t cols = rows;
  REQUIRE(rows * cols == info.total_size);
  size_t cell_size = 128;
  size_t cell_stride = rows * cols / cell_size / info.num_iterations;
  SECTION("write to existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    size_t prev_cell_col = 0, prev_cell_row = 0;
    for (size_t i = 0; i < info.num_iterations; ++i) {
      size_t current_cell_col = (prev_cell_col + cell_stride) % cols;
      size_t current_cell_row = prev_cell_col + cell_stride > cols
                                    ? prev_cell_row + 1
                                    : prev_cell_row;
      prev_cell_row = current_cell_row;
      auto offset = (current_cell_col * cell_stride + prev_cell_row * cols) %
                    (info.total_size - info.medium_max);
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t request_size =
          info.medium_min + (rand_r(&info.rs_seed) % info.medium_max);
      std::string data(request_size, '1');
      test::test_fwrite(data.data(), request_size);
      REQUIRE(test::size_written_orig == request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}
/**
 * Large RS
 **/

TEST_CASE("BatchedWriteRSRangeLarge",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-large][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=sequential][file=1]") {
  pretest();
  SECTION("write to new file always at the start") {
    test::test_fopen(info.new_file.c_str(), "w+");
    REQUIRE(test::fh_orig != nullptr);
    size_t biggest_written = 0;
    for (size_t i = 0; i < info.num_iterations; ++i) {
      test::test_fseek(0, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t offset = ftell(test::fh_orig);
      REQUIRE(offset == 0);
      size_t request_size =
          info.large_min + (rand_r(&info.rs_seed) % info.large_max);
      std::string data(request_size, '1');
      test::test_fwrite(data.data(), request_size);
      REQUIRE(test::size_written_orig == request_size);
      if (biggest_written < request_size) biggest_written = request_size;
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }

  SECTION("write to new file") {
    test::test_fopen(info.new_file.c_str(), "w+");
    REQUIRE(test::fh_orig != nullptr);
    size_t total_written = 0;
    for (size_t i = 0; i < info.num_iterations; ++i) {
      size_t request_size =
          info.large_min + (rand_r(&info.rs_seed) % info.large_max);
      std::string data(request_size, '1');
      test::test_fwrite(data.data(), request_size);
      REQUIRE(test::size_written_orig == request_size);
      total_written += test::size_written_orig;
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
    REQUIRE(fs::file_size(info.new_file) == total_written);
  }
  posttest();
}

TEST_CASE("BatchedReadSequentialRSRangeLarge",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-large][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=sequential][file=1]") {
  pretest();
  SECTION("read from existing file") {
    test::test_fopen(info.existing_file.c_str(), "r");
    REQUIRE(test::fh_orig != nullptr);
    std::string data(args.request_size, '1');
    size_t current_offset = 0;
    for (size_t i = 0; i < info.num_iterations; ++i) {
      size_t request_size =
          (info.large_min + (rand_r(&info.rs_seed) % info.large_max)) %
          (info.total_size - current_offset);
      std::string data(request_size, '1');
      test::test_fread(data.data(), request_size);
      REQUIRE(test::size_read_orig == request_size);
      current_offset += test::size_read_orig;
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }

  SECTION("read from existing file always at start") {
    test::test_fopen(info.existing_file.c_str(), "r");
    REQUIRE(test::fh_orig != nullptr);

    for (size_t i = 0; i < info.num_iterations; ++i) {
      test::test_fseek(0, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t offset = ftell(test::fh_orig);
      REQUIRE(offset == 0);
      size_t request_size =
          info.large_min + (rand_r(&info.rs_seed) % info.large_max);
      std::string data(request_size, '1');
      test::test_fread(data.data(), request_size);
      REQUIRE(test::size_read_orig == request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedReadRandomRSRangeLarge",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-large]"
              "[repetition=" +
              std::to_string(info.num_iterations) +
              "][pattern=random][file=1]") {
  pretest();

  SECTION("read from existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      auto offset =
          rand_r(&info.offset_seed) % (info.total_size - info.large_max);
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t request_size =
          (info.large_min + (rand_r(&info.rs_seed) % info.large_max)) %
          (info.total_size - offset);
      std::string data(request_size, '1');
      test::test_fread(data.data(), request_size);
      REQUIRE(test::size_read_orig == request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedUpdateRandomRSRangeLarge",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-large][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=random][file=1]") {
  pretest();

  SECTION("write into existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      auto offset =
          rand_r(&info.offset_seed) % (info.total_size - info.large_max);
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t request_size =
          info.large_min + (rand_r(&info.rs_seed) % info.large_max);
      std::string data = GenRandom(request_size);
      test::test_fwrite(data.data(), request_size);
      REQUIRE(test::size_written_orig == request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedReadStrideFixedRSRangeLarge",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-large][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=stride_fixed][file=1]") {
  pretest();
  SECTION("read from existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      auto offset = (i * info.stride_size) % (info.total_size - info.large_max);
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t request_size =
          info.large_min + (rand_r(&info.rs_seed) % info.large_max);
      std::string data(request_size, '1');
      test::test_fread(data.data(), request_size);
      REQUIRE(test::size_read_orig == request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedUpdateStrideFixedRSRangeLarge",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-large][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=stride_fixed][file=1]") {
  pretest();
  SECTION("write to existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      auto offset = (i * info.stride_size) % (info.total_size - info.large_max);
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t request_size =
          info.large_min + (rand_r(&info.rs_seed) % info.large_max);
      std::string data(request_size, '1');
      test::test_fwrite(data.data(), request_size);
      REQUIRE(test::size_written_orig == request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedReadStrideDynamicRSRangeLarge",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-large][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=stride_dynamic][file=1]") {
  pretest();
  SECTION("read from existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      auto offset = GetRandomOffset(i, info.offset_seed, info.stride_size,
                                    info.total_size - info.large_max);
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t request_size =
          info.large_min + (rand_r(&info.rs_seed) % info.large_max);
      std::string data(request_size, '1');
      test::test_fread(data.data(), request_size);
      REQUIRE(test::size_read_orig == request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedUpdateStrideDynamicRSRangeLarge",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-large][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=stride_dynamic][file=1]") {
  pretest();
  SECTION("read from existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      auto offset = GetRandomOffset(i, info.offset_seed, info.stride_size,
                                    info.total_size - info.large_max);
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t request_size =
          info.large_min + (rand_r(&info.rs_seed) % info.large_max);
      std::string data(request_size, '1');
      test::test_fwrite(data.data(), request_size);
      REQUIRE(test::size_written_orig == request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedReadStrideNegativeRSRangeLarge",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-large][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=stride_negative][file=1]") {
  pretest();
  SECTION("read from existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      auto offset = (info.total_size - i * info.stride_size) %
                    (info.total_size - info.large_max);
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t request_size =
          (info.large_min + (rand_r(&info.rs_seed) % info.large_max)) %
          (info.total_size - info.large_max);
      std::string data(request_size, '1');
      test::test_fread(data.data(), request_size);
      REQUIRE(test::size_read_orig == request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedUpdateStrideNegativeRSRangeLarge",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-large][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=stride_negative][file=1]") {
  pretest();
  SECTION("write to existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      auto offset = info.total_size - ((i * info.stride_size) %
                                       (info.total_size - info.large_max));
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t request_size =
          info.large_min + (rand_r(&info.rs_seed) % info.large_max);
      std::string data(request_size, '1');
      test::test_fwrite(data.data(), request_size);
      REQUIRE(test::size_written_orig == request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedReadStride2DRSRangeLarge",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-large][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=stride_2d][file=1]") {
  pretest();
  size_t rows = sqrt(info.total_size);
  size_t cols = rows;
  REQUIRE(rows * cols == info.total_size);
  size_t cell_size = 128;
  size_t cell_stride = rows * cols / cell_size / info.num_iterations;
  SECTION("read from existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    size_t prev_cell_col = 0, prev_cell_row = 0;
    for (size_t i = 0; i < info.num_iterations; ++i) {
      size_t current_cell_col = (prev_cell_col + cell_stride) % cols;
      size_t current_cell_row = prev_cell_col + cell_stride > cols
                                    ? prev_cell_row + 1
                                    : prev_cell_row;
      prev_cell_row = current_cell_row;
      auto offset = (current_cell_col * cell_stride + prev_cell_row * cols) %
                    (info.total_size - info.large_max);
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t request_size =
          info.large_min + (rand_r(&info.rs_seed) % info.large_max);
      std::string data(request_size, '1');
      test::test_fread(data.data(), request_size);
      REQUIRE(test::size_read_orig == request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedUpdateStride2DRSRangeLarge",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              ""
              "[operation=batched_write]"
              "[request_size=range-large][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=stride_2d][file=1]") {
  pretest();
  size_t rows = sqrt(info.total_size);
  size_t cols = rows;
  REQUIRE(rows * cols == info.total_size);
  size_t cell_size = 128;
  size_t cell_stride = rows * cols / cell_size / info.num_iterations;
  SECTION("write to existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    size_t prev_cell_col = 0, prev_cell_row = 0;
    for (size_t i = 0; i < info.num_iterations; ++i) {
      size_t current_cell_col = (prev_cell_col + cell_stride) % cols;
      size_t current_cell_row = prev_cell_col + cell_stride > cols
                                    ? prev_cell_row + 1
                                    : prev_cell_row;
      prev_cell_row = current_cell_row;
      auto offset = (current_cell_col * cell_stride + prev_cell_row * cols) %
                    (info.total_size - info.large_max);
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t request_size =
          info.large_min + (rand_r(&info.rs_seed) % info.large_max);
      std::string data(request_size, '1');
      test::test_fwrite(data.data(), request_size);
      REQUIRE(test::size_written_orig == request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}
