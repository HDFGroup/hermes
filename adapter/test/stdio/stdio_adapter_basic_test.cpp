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

TEST_CASE("Open", "[process=" + std::to_string(info.comm_size) +
                      "]"
                      "[operation=single_open]"
                      "[repetition=1][file=1]") {
  pretest();
  SECTION("open non-existant file") {
    test::test_fopen(info.new_file.c_str(), "r");
    REQUIRE(test::fh_orig == nullptr);
    test::test_fopen(info.new_file.c_str(), "r+");
    REQUIRE(test::fh_orig == nullptr);
  }

  SECTION("truncate existing file and write-only") {
    test::test_fopen(info.existing_file.c_str(), "w");
    REQUIRE(test::fh_orig != nullptr);
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  SECTION("truncate existing file and read/write") {
    test::test_fopen(info.existing_file.c_str(), "w+");
    REQUIRE(test::fh_orig != nullptr);
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }

  SECTION("open existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
    test::test_fopen(info.existing_file.c_str(), "r");
    REQUIRE(test::fh_orig != nullptr);
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }

  SECTION("append write existing file") {
    test::test_fopen(info.existing_file.c_str(), "a");
    REQUIRE(test::fh_orig != nullptr);
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }

  SECTION("append write and read existing file") {
    test::test_fopen(info.existing_file.c_str(), "a+");
    REQUIRE(test::fh_orig != nullptr);
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("SingleWrite", "[process=" + std::to_string(info.comm_size) +
                             "]"
                             "[operation=single_write]"
                             "[request_size=type-fixed][repetition=1]"
                             "[file=1]") {
  pretest();
  SECTION("write to existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    test::test_fseek(0, SEEK_SET);
    REQUIRE(test::status_orig == 0);
    test::test_fwrite(info.write_data.c_str(), args.request_size);
    REQUIRE(test::size_written_orig == args.request_size);
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }

  SECTION("write to new  file") {
    test::test_fopen(info.new_file.c_str(), "w+");
    REQUIRE(test::fh_orig != nullptr);
    test::test_fwrite(info.write_data.c_str(), args.request_size);
    REQUIRE(test::size_written_orig == args.request_size);
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
    REQUIRE(fs::file_size(info.new_file) == test::size_written_orig);
  }

  SECTION("write to existing file with truncate") {
    test::test_fopen(info.existing_file.c_str(), "w");
    REQUIRE(test::fh_orig != nullptr);
    test::test_fwrite(info.write_data.c_str(), args.request_size);
    REQUIRE(test::size_written_orig == args.request_size);
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
    REQUIRE(fs::file_size(info.existing_file) == test::size_written_orig);
  }

  SECTION("write to existing file at the end") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    test::test_fseek(0, SEEK_END);
    REQUIRE(test::status_orig == 0);
    size_t offset = ftell(test::fh_orig);
    REQUIRE(offset == args.request_size * info.num_iterations);
    test::test_fwrite(info.write_data.c_str(), args.request_size);
    REQUIRE(test::size_written_orig == args.request_size);
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
    REQUIRE(fs::file_size(info.existing_file) ==
            test::size_written_orig + offset);
  }

  SECTION("append to existing file") {
    auto existing_size = fs::file_size(info.existing_file);
    test::test_fopen(info.existing_file.c_str(), "a+");
    REQUIRE(test::fh_orig != nullptr);
    test::test_fwrite(info.write_data.c_str(), args.request_size);
    REQUIRE(test::size_written_orig == args.request_size);
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
    REQUIRE(fs::file_size(info.existing_file) ==
            existing_size + test::size_written_orig);
  }

  SECTION("append to new file") {
    test::test_fopen(info.new_file.c_str(), "w+");
    REQUIRE(test::fh_orig != nullptr);
    test::test_fwrite(info.write_data.c_str(), args.request_size);
    REQUIRE(test::size_written_orig == args.request_size);
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
    REQUIRE(fs::file_size(info.new_file) == test::size_written_orig);
  }
  posttest();
}

TEST_CASE("SingleRead", "[process=" + std::to_string(info.comm_size) +
                            "]"
                            "[operation=single_read]"
                            "[request_size=type-fixed][repetition=1]"
                            "[file=1]") {
  pretest();
  SECTION("read from non-existing file") {
    test::test_fopen(info.new_file.c_str(), "r");
    REQUIRE(test::fh_orig == nullptr);
  }

  SECTION("read from existing file") {
    test::test_fopen(info.existing_file.c_str(), "r");
    REQUIRE(test::fh_orig != nullptr);
    size_t offset = ftell(test::fh_orig);
    REQUIRE(offset == 0);
    test::test_fread(info.read_data.data(), args.request_size);
    REQUIRE(test::size_read_orig == args.request_size);
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  SECTION("read at the end of existing file") {
    test::test_fopen(info.existing_file.c_str(), "r");
    REQUIRE(test::fh_orig != nullptr);
    test::test_fseek(0, SEEK_END);
    REQUIRE(test::status_orig == 0);
    size_t offset = ftell(test::fh_orig);
    REQUIRE(offset == args.request_size * info.num_iterations);
    test::test_fread(info.read_data.data(), args.request_size);
    REQUIRE(test::size_read_orig == 0);
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedWriteSequential",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_write]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=sequential][file=1]") {
  pretest();
  SECTION("write to existing file") {
    test::test_fopen(info.new_file.c_str(), "w+");
    REQUIRE(test::fh_orig != nullptr);

    for (size_t i = 0; i < info.num_iterations; ++i) {
      test::test_fseek(0, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t offset = ftell(test::fh_orig);
      REQUIRE(offset == 0);
      test::test_fwrite(info.write_data.c_str(), args.request_size);
      REQUIRE(test::size_written_orig == args.request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
    REQUIRE(fs::file_size(info.new_file) == args.request_size);
  }

  SECTION("write to new file always at end") {
    test::test_fopen(info.new_file.c_str(), "w+");
    REQUIRE(test::fh_orig != nullptr);

    for (size_t i = 0; i < info.num_iterations; ++i) {
      test::test_fwrite(info.write_data.c_str(), args.request_size);
      REQUIRE(test::size_written_orig == args.request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
    REQUIRE(fs::file_size(info.new_file) ==
            info.num_iterations * args.request_size);
  }
  posttest();
}

TEST_CASE("BatchedReadSequential",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_read]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=sequential][file=1]") {
  pretest();

  SECTION("read from existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    std::string data(args.request_size, '1');
    for (size_t i = 0; i < info.num_iterations; ++i) {
      test::test_fread(data.data(), args.request_size);
      REQUIRE(test::size_read_orig == args.request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }

  SECTION("read from existing file always at start") {
    test::test_fopen(info.existing_file.c_str(), "w+");
    REQUIRE(test::fh_orig != nullptr);

    for (size_t i = 0; i < info.num_iterations; ++i) {
      test::test_fseek(0, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t offset = ftell(test::fh_orig);
      REQUIRE(offset == 0);
      test::test_fwrite(info.write_data.c_str(), args.request_size);
      REQUIRE(test::size_written_orig == args.request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedReadRandom", "[process=" + std::to_string(info.comm_size) +
                                   "]"
                                   "[operation=batched_read]"
                                   "[request_size=type-fixed]"
                                   "[repetition=" +
                                   std::to_string(info.num_iterations) +
                                   "][pattern=random][file=1]") {
  pretest();
  SECTION("read from existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    std::string data(args.request_size, '1');
    for (size_t i = 0; i < info.num_iterations; ++i) {
      auto offset =
          rand_r(&info.offset_seed) % (info.total_size - args.request_size);
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      test::test_fread(data.data(), args.request_size);
      REQUIRE(test::size_read_orig == args.request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedUpdateRandom", "[process=" + std::to_string(info.comm_size) +
                                     "]"
                                     "[operation=batched_update]"
                                     "[request_size=type-fixed][repetition=" +
                                     std::to_string(info.num_iterations) +
                                     "]"
                                     "[pattern=random][file=1]") {
  pretest();
  SECTION("update into existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      auto offset =
          rand_r(&info.offset_seed) % (info.total_size - args.request_size);
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      test::test_fwrite(info.write_data.data(), args.request_size);
      REQUIRE(test::size_written_orig == args.request_size);
      fflush(test::fh_orig);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedReadStrideFixed",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_read]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=stride_fixed][file=1]") {
  pretest();

  SECTION("read from existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    std::string data(args.request_size, '1');
    for (size_t i = 0; i < info.num_iterations; ++i) {
      auto offset = (i * info.stride_size) % info.total_size;
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      test::test_fread(data.data(), args.request_size);
      REQUIRE(test::size_read_orig == args.request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedUpdateStrideFixed",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_update]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=stride_fixed][file=1]") {
  pretest();

  SECTION("update from existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    std::string data(args.request_size, '1');
    for (size_t i = 0; i < info.num_iterations; ++i) {
      auto offset = (i * info.stride_size) % info.total_size;
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      test::test_fwrite(data.data(), args.request_size);
      REQUIRE(test::size_read_orig == args.request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedReadStrideDynamic",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_read]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=stride_dynamic][file=1]") {
  pretest();

  SECTION("read from existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    std::string data(args.request_size, '1');
    for (size_t i = 0; i < info.num_iterations; ++i) {
      auto offset = GetRandomOffset(i, info.total_size);
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      test::test_fread(data.data(), args.request_size);
      REQUIRE(test::size_read_orig == args.request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedUpdateStrideDynamic",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_update]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=stride_dynamic][file=1]") {
  pretest();
  SECTION("update from existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    std::string data(args.request_size, '1');
    for (size_t i = 0; i < info.num_iterations; ++i) {
      auto offset = GetRandomOffset(i, info.total_size);
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      test::test_fwrite(data.data(), args.request_size);
      REQUIRE(test::size_read_orig == args.request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedWriteRSVariable",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_write]"
              "[request_size=type-variable][repetition=" +
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
          args.request_size + (rand_r(&info.offset_seed) % args.request_size);
      std::string data(request_size, '1');
      test::test_fwrite(data.c_str(), request_size);
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
    size_t total_test_written = 0;
    for (size_t i = 0; i < info.num_iterations; ++i) {
      size_t request_size =
          args.request_size + (rand_r(&info.offset_seed) % args.request_size);
      std::string data(request_size, '1');
      test::test_fwrite(data.c_str(), request_size);
      REQUIRE(test::size_written_orig == request_size);
      total_test_written += test::size_written_orig;
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
    REQUIRE(fs::file_size(info.new_file) == total_test_written);
  }
  posttest();
}

TEST_CASE("BatchedReadSequentialRSVariable",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_read]"
              "[request_size=type-variable][repetition=" +
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
      size_t request_size = (args.request_size +
                             (rand_r(&info.offset_seed) % args.request_size)) %
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
          args.request_size + (rand_r(&info.offset_seed) % args.request_size);
      std::string data(request_size, '1');
      test::test_fread(data.data(), request_size);
      REQUIRE(test::size_read_orig == request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedReadRandomRSVariable",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_read]"
              "[request_size=type-variable]"
              "[repetition=" +
              std::to_string(info.num_iterations) +
              "][pattern=random][file=1]") {
  pretest();

  SECTION("read from existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    std::string data(args.request_size, '1');
    for (size_t i = 0; i < info.num_iterations; ++i) {
      auto offset =
          rand_r(&info.offset_seed) % (info.total_size - args.request_size);
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t request_size =
          (args.request_size + (rand_r(&info.rs_seed) % args.request_size)) %
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

TEST_CASE("BatchedUpdateRandomRSVariable",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_update]"
              "[request_size=type-variable][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=random][file=1]") {
  pretest();

  SECTION("read from existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    std::string data(args.request_size, '1');
    for (size_t i = 0; i < info.num_iterations; ++i) {
      auto offset =
          rand_r(&info.offset_seed) % (info.total_size - args.request_size);
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t request_size =
          args.request_size + (rand_r(&info.rs_seed) % args.request_size);
      std::string data(request_size, '1');
      test::test_fwrite(data.c_str(), request_size);
      REQUIRE(test::size_written_orig == request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedReadStrideFixedRSVariable",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_read]"
              "[request_size=type-variable][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=stride_fixed][file=1]") {
  pretest();

  SECTION("read from existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      auto offset = (i * info.stride_size) % info.total_size;
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t request_size =
          (args.request_size + (rand_r(&info.rs_seed) % args.request_size)) %
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

TEST_CASE("BatchedUpdateStrideFixedRSVariable",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_update]"
              "[request_size=type-variable][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=stride_fixed][file=1]") {
  pretest();

  SECTION("write to existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    std::string data(args.request_size, '1');
    for (size_t i = 0; i < info.num_iterations; ++i) {
      auto offset = (i * info.stride_size) % info.total_size;
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t request_size =
          args.request_size + (rand_r(&info.rs_seed) % args.request_size);
      std::string data(request_size, '1');
      test::test_fwrite(data.c_str(), request_size);
      REQUIRE(test::size_written_orig == request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedReadStrideDynamicRSVariable",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_read]"
              "[request_size=type-variable][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=stride_dynamic][file=1]") {
  pretest();

  SECTION("read from existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      auto offset = GetRandomOffset(i, info.total_size);
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t request_size =
          args.request_size + (rand_r(&info.rs_seed) % args.request_size);
      std::string data(request_size, '1');
      test::test_fread(data.data(), request_size);
      REQUIRE(test::size_read_orig == request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedUpdateStrideDynamicRSVariable",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_update]"
              "[request_size=type-variable][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=stride_dynamic][file=1]") {
  pretest();
  SECTION("read from existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      auto offset = GetRandomOffset(i, info.total_size);
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t request_size =
          args.request_size + (rand_r(&info.rs_seed) % args.request_size);
      std::string data(request_size, '1');
      test::test_fwrite(data.c_str(), request_size);
      REQUIRE(test::size_written_orig == request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedReadStrideNegative",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_read]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=stride_negative][file=1]") {
  pretest();
  SECTION("read from existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    std::string data(args.request_size, '1');
    size_t prev_offset = info.total_size + 1;
    for (size_t i = 0; i < info.num_iterations; ++i) {
      auto stride_offset = info.total_size - i * info.stride_size;
      REQUIRE(prev_offset > stride_offset);
      prev_offset = stride_offset;
      auto offset = (stride_offset) % (info.total_size - args.request_size);
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      test::test_fread(data.data(), args.request_size);
      REQUIRE(test::size_read_orig == args.request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedUpdateStrideNegative",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_update]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=stride_negative][file=1]") {
  pretest();
  SECTION("read from existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    std::string data(args.request_size, '1');
    for (size_t i = 0; i < info.num_iterations; ++i) {
      auto offset =
          info.total_size - ((i * info.stride_size) % info.total_size);
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      test::test_fwrite(data.data(), args.request_size);
      REQUIRE(test::size_read_orig == args.request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedReadStrideNegativeRSVariable",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_read]"
              "[request_size=type-variable][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=stride_negative][file=1]") {
  pretest();

  SECTION("read from existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      auto offset = (info.total_size - i * info.stride_size) %
                    (info.total_size - 2 * args.request_size);
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t request_size =
          (args.request_size + (rand_r(&info.rs_seed) % args.request_size)) %
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

TEST_CASE("BatchedUpdateStrideNegativeRSVariable",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_update]"
              "[request_size=type-variable][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=stride_negative][file=1]") {
  pretest();

  SECTION("write to existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    std::string data(args.request_size, '1');
    for (size_t i = 0; i < info.num_iterations; ++i) {
      auto offset =
          info.total_size - ((i * info.stride_size) % info.total_size);
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t request_size =
          args.request_size + (rand_r(&info.rs_seed) % args.request_size);
      std::string data(request_size, '1');
      test::test_fwrite(data.c_str(), request_size);
      REQUIRE(test::size_written_orig == request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedReadStride2D", "[process=" + std::to_string(info.comm_size) +
                                     "]"
                                     "[operation=batched_read]"
                                     "[request_size=type-fixed][repetition=" +
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
    std::string data(args.request_size, '1');
    size_t prev_cell_col = 0, prev_cell_row = 0;
    for (size_t i = 0; i < info.num_iterations; ++i) {
      size_t current_cell_col = (prev_cell_col + cell_stride) % cols;
      size_t current_cell_row = prev_cell_col + cell_stride > cols
                                    ? prev_cell_row + 1
                                    : prev_cell_row;
      prev_cell_row = current_cell_row;
      auto offset = (current_cell_col * cell_stride + prev_cell_row * cols) %
                    (info.total_size - args.request_size);
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      test::test_fread(data.data(), args.request_size);
      REQUIRE(test::size_read_orig == args.request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedUpdateStride2D",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_update]"
              "[request_size=type-fixed][repetition=" +
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
    std::string data(args.request_size, '1');
    size_t prev_cell_col = 0, prev_cell_row = 0;
    for (size_t i = 0; i < info.num_iterations; ++i) {
      size_t current_cell_col = (prev_cell_col + cell_stride) % cols;
      size_t current_cell_row = prev_cell_col + cell_stride > cols
                                    ? prev_cell_row + 1
                                    : prev_cell_row;
      prev_cell_row = current_cell_row;
      auto offset = (current_cell_col * cell_stride + prev_cell_row * cols) %
                    (info.total_size - args.request_size);
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      test::test_fwrite(data.data(), args.request_size);
      REQUIRE(test::size_read_orig == args.request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedReadStride2DRSVariable",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              ""
              "[operation=batched_read]"
              "[request_size=type-variable][repetition=" +
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
                    (info.total_size - 2 * args.request_size);
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t request_size =
          (args.request_size + (rand_r(&info.rs_seed) % args.request_size)) %
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

TEST_CASE("BatchedUpdateStride2DRSVariable",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_update]"
              "[request_size=type-variable][repetition=" +
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
    std::string data(args.request_size, '1');
    size_t prev_cell_col = 0, prev_cell_row = 0;
    for (size_t i = 0; i < info.num_iterations; ++i) {
      size_t current_cell_col = (prev_cell_col + cell_stride) % cols;
      size_t current_cell_row = prev_cell_col + cell_stride > cols
                                    ? prev_cell_row + 1
                                    : prev_cell_row;
      prev_cell_row = current_cell_row;
      auto offset = (current_cell_col * cell_stride + prev_cell_row * cols) %
                    (info.total_size - 2 * args.request_size);
      test::test_fseek(offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t request_size =
          args.request_size + (rand_r(&info.rs_seed) % args.request_size);
      std::string data(request_size, '1');
      test::test_fwrite(data.c_str(), request_size);
      REQUIRE(test::size_written_orig == request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

/**
 * Temporal Fixed
 */

TEST_CASE("BatchedWriteTemporalFixed",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_write]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=sequential][file=1][temporal=fixed]") {
  pretest();

  SECTION("write to existing file") {
    test::test_fopen(info.new_file.c_str(), "w+");
    REQUIRE(test::fh_orig != nullptr);

    for (size_t i = 0; i < info.num_iterations; ++i) {
      usleep(info.temporal_interval_ms * 1000);
      test::test_fseek(0, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t offset = ftell(test::fh_orig);
      REQUIRE(offset == 0);
      test::test_fwrite(info.write_data.c_str(), args.request_size);
      REQUIRE(test::size_written_orig == args.request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
    REQUIRE(fs::file_size(info.new_file) == args.request_size);
  }

  SECTION("write to new file always at start") {
    test::test_fopen(info.new_file.c_str(), "w+");
    REQUIRE(test::fh_orig != nullptr);

    for (size_t i = 0; i < info.num_iterations; ++i) {
      usleep(info.temporal_interval_ms * 1000);
      test::test_fwrite(info.write_data.c_str(), args.request_size);
      REQUIRE(test::size_written_orig == args.request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
    REQUIRE(fs::file_size(info.new_file) ==
            info.num_iterations * args.request_size);
  }
  posttest();
}

TEST_CASE("BatchedReadSequentialTemporalFixed",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_read]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=sequential][file=1][temporal=fixed]") {
  pretest();

  SECTION("read from existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    std::string data(args.request_size, '1');
    for (size_t i = 0; i < info.num_iterations; ++i) {
      usleep(info.temporal_interval_ms * 1000);
      test::test_fread(data.data(), args.request_size);
      REQUIRE(test::size_read_orig == args.request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }

  SECTION("read from existing file always at start") {
    test::test_fopen(info.existing_file.c_str(), "w+");
    REQUIRE(test::fh_orig != nullptr);

    for (size_t i = 0; i < info.num_iterations; ++i) {
      usleep(info.temporal_interval_ms * 1000);
      test::test_fseek(0, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t offset = ftell(test::fh_orig);
      REQUIRE(offset == 0);
      test::test_fwrite(info.write_data.c_str(), args.request_size);
      REQUIRE(test::size_written_orig == args.request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedWriteTemporalVariable",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_write]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=sequential][file=1][temporal=variable]") {
  pretest();

  SECTION("write to existing file") {
    test::test_fopen(info.new_file.c_str(), "w+");
    REQUIRE(test::fh_orig != nullptr);

    for (size_t i = 0; i < info.num_iterations; ++i) {
      info.temporal_interval_ms =
          rand_r(&info.temporal_interval_seed) % info.temporal_interval_ms + 1;
      usleep(info.temporal_interval_ms * 1000);
      test::test_fseek(0, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t offset = ftell(test::fh_orig);
      REQUIRE(offset == 0);
      test::test_fwrite(info.write_data.c_str(), args.request_size);
      REQUIRE(test::size_written_orig == args.request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
    REQUIRE(fs::file_size(info.new_file) == args.request_size);
  }

  SECTION("write to new file always at start") {
    test::test_fopen(info.new_file.c_str(), "w+");
    REQUIRE(test::fh_orig != nullptr);

    for (size_t i = 0; i < info.num_iterations; ++i) {
      info.temporal_interval_ms =
          rand_r(&info.temporal_interval_seed) % info.temporal_interval_ms + 1;
      usleep(info.temporal_interval_ms * 1000);
      test::test_fwrite(info.write_data.c_str(), args.request_size);
      REQUIRE(test::size_written_orig == args.request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
    REQUIRE(fs::file_size(info.new_file) ==
            info.num_iterations * args.request_size);
  }
  posttest();
}

TEST_CASE("BatchedReadSequentialTemporalVariable",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_read]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=sequential][file=1][temporal=variable]") {
  pretest();

  SECTION("read from existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    std::string data(args.request_size, '1');
    for (size_t i = 0; i < info.num_iterations; ++i) {
      info.temporal_interval_ms =
          rand_r(&info.temporal_interval_seed) % info.temporal_interval_ms + 1;
      usleep(info.temporal_interval_ms * 1000);
      test::test_fread(data.data(), args.request_size);
      REQUIRE(test::size_read_orig == args.request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }

  SECTION("read from existing file always at start") {
    test::test_fopen(info.existing_file.c_str(), "w+");
    REQUIRE(test::fh_orig != nullptr);

    for (size_t i = 0; i < info.num_iterations; ++i) {
      info.temporal_interval_ms =
          rand_r(&info.temporal_interval_seed) % info.temporal_interval_ms + 1;
      usleep(info.temporal_interval_ms * 1000);
      test::test_fseek(0, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t offset = ftell(test::fh_orig);
      REQUIRE(offset == 0);
      test::test_fwrite(info.write_data.c_str(), args.request_size);
      REQUIRE(test::size_written_orig == args.request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedMixedSequential",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_mixed]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=sequential][file=1]") {
  pretest();
  SECTION("read after write on new file") {
    test::test_fopen(info.new_file.c_str(), "w+");
    REQUIRE(test::fh_orig != nullptr);
    size_t last_offset = 0;
    for (size_t i = 0; i < info.num_iterations; ++i) {
      test::test_fwrite(info.write_data.data(), args.request_size);
      REQUIRE(test::size_written_orig == args.request_size);
      test::test_fseek(last_offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      test::test_fread(info.read_data.data(), args.request_size);
      REQUIRE(test::size_read_orig == args.request_size);
      last_offset += args.request_size;
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }

  SECTION("write and read alternative existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      if (i % 2 == 0) {
        test::test_fwrite(info.write_data.data(), args.request_size);
        REQUIRE(test::size_written_orig == args.request_size);
      } else {
        test::test_fread(info.read_data.data(), args.request_size);
        REQUIRE(test::size_read_orig == args.request_size);
      }
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  SECTION("update after read existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    size_t last_offset = 0;
    for (size_t i = 0; i < info.num_iterations; ++i) {
      test::test_fread(info.read_data.data(), args.request_size);
      REQUIRE(test::size_read_orig == args.request_size);
      test::test_fseek(last_offset, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      test::test_fwrite(info.write_data.data(), args.request_size);
      REQUIRE(test::size_written_orig == args.request_size);
      last_offset += args.request_size;
    }

    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  SECTION("read all after write all on new file in single open") {
    test::test_fopen(info.new_file.c_str(), "w+");
    REQUIRE(test::fh_orig != nullptr);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      test::test_fwrite(info.write_data.data(), args.request_size);
      REQUIRE(test::size_written_orig == args.request_size);
    }
    test::test_fseek(0, SEEK_SET);
    REQUIRE(test::status_orig == 0);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      test::test_fread(info.read_data.data(), args.request_size);
      REQUIRE(test::size_read_orig == args.request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  SECTION("read all after write all on new file in different open") {
    test::test_fopen(info.new_file.c_str(), "w+");
    REQUIRE(test::fh_orig != nullptr);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      test::test_fwrite(info.write_data.data(), args.request_size);
      REQUIRE(test::size_written_orig == args.request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
    test::test_fopen(info.new_file.c_str(), "r");
    for (size_t i = 0; i < info.num_iterations; ++i) {
      test::test_fread(info.read_data.data(), args.request_size);
      REQUIRE(test::size_read_orig == args.request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("SingleMixed", "[process=" + std::to_string(info.comm_size) +
                             "][operation=single_mixed]"
                             "[request_size=type-fixed][repetition=1]"
                             "[file=1]") {
  pretest();
  SECTION("read after write from new file") {
    test::test_fopen(info.new_file.c_str(), "w+");
    REQUIRE(test::fh_orig != nullptr);
    size_t offset = ftell(test::fh_orig);
    REQUIRE(offset == 0);
    test::test_fwrite(info.write_data.data(), args.request_size);
    REQUIRE(test::size_written_orig == args.request_size);
    test::test_fseek(0, SEEK_SET);
    REQUIRE(test::status_orig == 0);
    test::test_fread(info.read_data.data(), args.request_size);
    REQUIRE(test::size_read_orig == args.request_size);
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  SECTION("update after read from existing file") {
    test::test_fopen(info.existing_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    size_t offset = ftell(test::fh_orig);
    REQUIRE(offset == 0);
    test::test_fread(info.read_data.data(), args.request_size);
    REQUIRE(test::size_read_orig == args.request_size);
    test::test_fseek(0, SEEK_SET);
    REQUIRE(test::status_orig == 0);
    test::test_fwrite(info.write_data.data(), args.request_size);
    REQUIRE(test::size_written_orig == args.request_size);

    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  SECTION("read after write from new file different opens") {
    test::test_fopen(info.new_file.c_str(), "w+");
    REQUIRE(test::fh_orig != nullptr);
    size_t offset = ftell(test::fh_orig);
    REQUIRE(offset == 0);
    test::test_fwrite(info.write_data.data(), args.request_size);
    REQUIRE(test::size_written_orig == args.request_size);
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
    test::test_fopen(info.new_file.c_str(), "r+");
    test::test_fread(info.read_data.data(), args.request_size);
    REQUIRE(test::size_read_orig == args.request_size);
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}
