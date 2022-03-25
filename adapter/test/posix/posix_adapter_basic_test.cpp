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

#include <sys/stat.h>

TEST_CASE("Open", "[process=" + std::to_string(info.comm_size) +
                      "]"
                      "[operation=single_open]"
                      "[repetition=1][file=1]") {
  pretest();
  SECTION("open non-existant file") {
    test::test_open(info.new_file.c_str(), O_WRONLY);
    REQUIRE(test::fh_orig == -1);
    test::test_open(info.new_file.c_str(), O_RDONLY);
    REQUIRE(test::fh_orig == -1);
    test::test_open(info.new_file.c_str(), O_RDWR);
    REQUIRE(test::fh_orig == -1);
  }

  SECTION("truncate existing file and write-only") {
    test::test_open(info.existing_file.c_str(), O_WRONLY | O_TRUNC);
    REQUIRE(test::fh_orig != -1);
    test::test_close();
    REQUIRE(test::status_orig == 0);
  }
  SECTION("truncate existing file and read/write") {
    test::test_open(info.existing_file.c_str(), O_RDWR | O_TRUNC);
    REQUIRE(test::fh_orig != -1);
    test::test_close();
    REQUIRE(test::status_orig == 0);
  }

  SECTION("open existing file") {
    test::test_open(info.existing_file.c_str(), O_WRONLY);
    REQUIRE(test::fh_orig != -1);
    test::test_close();
    REQUIRE(test::status_orig == 0);
    test::test_open(info.existing_file.c_str(), O_RDONLY);
    REQUIRE(test::fh_orig != -1);
    test::test_close();
    REQUIRE(test::status_orig == 0);
    test::test_open(info.existing_file.c_str(), O_RDWR);
    REQUIRE(test::fh_orig != -1);
    test::test_close();
    REQUIRE(test::status_orig == 0);
  }

  SECTION("append write existing file") {
    test::test_open(info.existing_file.c_str(), O_APPEND);
    REQUIRE(test::fh_orig != -1);
    test::test_close();
    REQUIRE(test::status_orig == 0);
  }

  SECTION("create a new file") {
    test::test_open(info.new_file.c_str(), O_WRONLY | O_CREAT, 0600);
    REQUIRE(test::fh_orig != -1);
    test::test_close();
    REQUIRE(test::status_orig == 0);
    fs::remove(info.new_file);

    test::test_open(info.new_file.c_str(), O_RDONLY | O_CREAT, 0600);
    REQUIRE(test::fh_orig != -1);
    test::test_close();
    REQUIRE(test::status_orig == 0);
    fs::remove(info.new_file);

    test::test_open(info.new_file.c_str(), O_RDWR | O_CREAT, 0600);
    REQUIRE(test::fh_orig != -1);
    test::test_close();
    REQUIRE(test::status_orig == 0);
  }
  SECTION("create a existing file") {
    test::test_open(info.existing_file.c_str(), O_WRONLY | O_CREAT, 0600);
    REQUIRE(test::fh_orig != -1);
    test::test_close();
    REQUIRE(test::status_orig == 0);
    test::test_open(info.existing_file.c_str(), O_RDONLY | O_CREAT, 0600);
    REQUIRE(test::fh_orig != -1);
    test::test_close();
    REQUIRE(test::status_orig == 0);
    test::test_open(info.existing_file.c_str(), O_RDWR | O_CREAT, 0600);
    REQUIRE(test::fh_orig != -1);
    test::test_close();
    REQUIRE(test::status_orig == 0);

    test::test_open(info.existing_file.c_str(), O_WRONLY | O_CREAT | O_EXCL,
                    0600);
    REQUIRE(test::fh_orig == -1);
    test::test_open(info.existing_file.c_str(), O_RDONLY | O_CREAT | O_EXCL,
                    0600);
    REQUIRE(test::fh_orig == -1);
    test::test_open(info.existing_file.c_str(), O_RDWR | O_CREAT | O_EXCL,
                    0600);
    REQUIRE(test::fh_orig == -1);
  }
  SECTION("Async I/O") {
    test::test_open(info.existing_file.c_str(), O_WRONLY | O_ASYNC);
    REQUIRE(test::fh_orig != -1);
    test::test_close();
    REQUIRE(test::status_orig == 0);
    test::test_open(info.existing_file.c_str(), O_RDONLY | O_ASYNC);
    REQUIRE(test::fh_orig != -1);
    test::test_close();
    REQUIRE(test::status_orig == 0);
    test::test_open(info.existing_file.c_str(), O_RDWR | O_ASYNC);
    REQUIRE(test::fh_orig != -1);
    test::test_close();
    REQUIRE(test::status_orig == 0);
    test::test_open(info.existing_file.c_str(), O_APPEND | O_ASYNC);
    REQUIRE(test::fh_orig != -1);
    test::test_close();
    REQUIRE(test::status_orig == 0);

    test::test_open(info.existing_file.c_str(), O_WRONLY | O_NONBLOCK);
    REQUIRE(test::fh_orig != -1);
    test::test_close();
    REQUIRE(test::status_orig == 0);
    test::test_open(info.existing_file.c_str(), O_RDONLY | O_NONBLOCK);
    REQUIRE(test::fh_orig != -1);
    test::test_close();
    REQUIRE(test::status_orig == 0);
    test::test_open(info.existing_file.c_str(), O_RDWR | O_NONBLOCK);
    REQUIRE(test::fh_orig != -1);
    test::test_close();
    REQUIRE(test::status_orig == 0);
    test::test_open(info.existing_file.c_str(), O_APPEND | O_NONBLOCK);
    REQUIRE(test::fh_orig != -1);
    test::test_close();
    REQUIRE(test::status_orig == 0);

    test::test_open(info.existing_file.c_str(), O_WRONLY | O_NDELAY);
    REQUIRE(test::fh_orig != -1);
    test::test_close();
    REQUIRE(test::status_orig == 0);
    test::test_open(info.existing_file.c_str(), O_RDONLY | O_NDELAY);
    REQUIRE(test::fh_orig != -1);
    test::test_close();
    REQUIRE(test::status_orig == 0);
    test::test_open(info.existing_file.c_str(), O_RDWR | O_NDELAY);
    REQUIRE(test::fh_orig != -1);
    test::test_close();
    REQUIRE(test::status_orig == 0);
    test::test_open(info.existing_file.c_str(), O_APPEND | O_NDELAY);
    REQUIRE(test::fh_orig != -1);
    test::test_close();
    REQUIRE(test::status_orig == 0);
  }
  SECTION("Async I/O") {
    test::test_open(info.existing_file.c_str(), O_WRONLY | O_DIRECT);
    REQUIRE(test::fh_orig != -1);
    test::test_close();
    REQUIRE(test::status_orig == 0);
    test::test_open(info.existing_file.c_str(), O_RDONLY | O_DIRECT);
    REQUIRE(test::fh_orig != -1);
    test::test_close();
    REQUIRE(test::status_orig == 0);
    test::test_open(info.existing_file.c_str(), O_RDWR | O_DIRECT);
    REQUIRE(test::fh_orig != -1);
    test::test_close();
    REQUIRE(test::status_orig == 0);
    test::test_open(info.existing_file.c_str(), O_APPEND | O_DIRECT);
    REQUIRE(test::fh_orig != -1);
    test::test_close();
    REQUIRE(test::status_orig == 0);
  }
  SECTION("Write Synchronize") {
    /* File synchronicity */
    test::test_open(info.existing_file.c_str(), O_WRONLY | O_DSYNC);
    REQUIRE(test::fh_orig != -1);
    test::test_close();
    REQUIRE(test::status_orig == 0);
    test::test_open(info.existing_file.c_str(), O_RDONLY | O_DSYNC);
    REQUIRE(test::fh_orig != -1);
    test::test_close();
    REQUIRE(test::status_orig == 0);
    test::test_open(info.existing_file.c_str(), O_RDWR | O_DSYNC);
    REQUIRE(test::fh_orig != -1);
    test::test_close();
    REQUIRE(test::status_orig == 0);
    test::test_open(info.existing_file.c_str(), O_APPEND | O_DSYNC);
    REQUIRE(test::fh_orig != -1);
    test::test_close();
    REQUIRE(test::status_orig == 0);

    /* Write synchronicity */
    test::test_open(info.existing_file.c_str(), O_WRONLY | O_SYNC);
    REQUIRE(test::fh_orig != -1);
    test::test_close();
    REQUIRE(test::status_orig == 0);
    test::test_open(info.existing_file.c_str(), O_RDONLY | O_SYNC);
    REQUIRE(test::fh_orig != -1);
    test::test_close();
    REQUIRE(test::status_orig == 0);
    test::test_open(info.existing_file.c_str(), O_RDWR | O_SYNC);
    REQUIRE(test::fh_orig != -1);
    test::test_close();
    REQUIRE(test::status_orig == 0);
    test::test_open(info.existing_file.c_str(), O_APPEND | O_SYNC);
    REQUIRE(test::fh_orig != -1);
    test::test_close();
    REQUIRE(test::status_orig == 0);
  }
  SECTION("Temporary file") {
    if (info.supports_tmpfile) {
      test::test_open("/tmp", O_WRONLY | O_TMPFILE, 0600);
      REQUIRE(test::fh_orig != -1);
      test::test_close();
      REQUIRE(test::status_orig == 0);
      test::test_open(info.new_file.c_str(), O_RDONLY | O_TMPFILE, 0600);
      REQUIRE(test::fh_orig == -1);
      test::test_open(info.new_file.c_str(), O_RDWR | O_TMPFILE, 0600);
      REQUIRE(test::fh_orig == -1);
      test::test_open(info.new_file.c_str(), O_APPEND | O_TMPFILE, 0600);
      REQUIRE(test::fh_orig == -1);

      test::test_open(info.existing_file.c_str(), O_WRONLY | O_TMPFILE, 0600);
      REQUIRE(test::fh_orig == -1);
      test::test_open(info.existing_file.c_str(), O_RDONLY | O_TMPFILE, 0600);
      REQUIRE(test::fh_orig == -1);
      test::test_open(info.existing_file.c_str(), O_RDWR | O_TMPFILE, 0600);
      REQUIRE(test::fh_orig == -1);
    }
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
    test::test_open(info.existing_file.c_str(), O_RDWR);
    REQUIRE(test::fh_orig != -1);
    test::test_seek(0, SEEK_SET);
    REQUIRE(test::status_orig == 0);
    test::test_write(info.write_data.data(), args.request_size);
    REQUIRE(test::size_written_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == 0);
  }

  SECTION("write to new  file") {
    test::test_open(info.new_file.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600);
    REQUIRE(test::fh_orig != -1);
    test::test_write(info.write_data.data(), args.request_size);
    REQUIRE(test::size_written_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == 0);
    REQUIRE(fs::file_size(info.new_file) == test::size_written_orig);
  }

  SECTION("write to existing file with truncate") {
    test::test_open(info.existing_file.c_str(), O_WRONLY | O_TRUNC);
    REQUIRE(test::fh_orig != -1);
    test::test_write(info.write_data.data(), args.request_size);
    REQUIRE(test::size_written_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == 0);
    REQUIRE(fs::file_size(info.existing_file) == test::size_written_orig);
  }

  SECTION("write to existing file at the end") {
    test::test_open(info.existing_file.c_str(), O_RDWR);
    REQUIRE(test::fh_orig != -1);
    test::test_seek(0, SEEK_END);
    REQUIRE(((size_t)test::status_orig) ==
            args.request_size * info.num_iterations);
    test::test_write(info.write_data.data(), args.request_size);
    REQUIRE(test::size_written_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == 0);
    REQUIRE(fs::file_size(info.existing_file) ==
            test::size_written_orig + args.request_size * info.num_iterations);
  }

  SECTION("append to existing file") {
    auto existing_size = fs::file_size(info.existing_file);
    test::test_open(info.existing_file.c_str(), O_RDWR | O_APPEND);
    REQUIRE(test::fh_orig != -1);
    test::test_write(info.write_data.data(), args.request_size);
    REQUIRE(test::size_written_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == 0);
    REQUIRE(fs::file_size(info.existing_file) ==
            existing_size + test::size_written_orig);
  }

  SECTION("append to new file") {
    test::test_open(info.new_file.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600);
    REQUIRE(test::fh_orig != -1);
    test::test_write(info.write_data.data(), args.request_size);
    REQUIRE(test::size_written_orig == args.request_size);
    test::test_close();
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
    test::test_open(info.new_file.c_str(), O_RDONLY);
    REQUIRE(test::fh_orig == -1);
  }

  SECTION("read from existing file") {
    test::test_open(info.existing_file.c_str(), O_RDONLY);
    REQUIRE(test::fh_orig != -1);
    test::test_seek(0, SEEK_CUR);
    REQUIRE(test::status_orig == 0);
    test::test_read(info.read_data.data(), args.request_size);
    REQUIRE(test::size_read_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == 0);
  }
  SECTION("read at the end of existing file") {
    test::test_open(info.existing_file.c_str(), O_RDONLY);
    REQUIRE(test::fh_orig != -1);
    test::test_seek(0, SEEK_END);
    REQUIRE(((size_t)test::status_orig) ==
            args.request_size * info.num_iterations);
    test::test_read(info.read_data.data(), args.request_size);
    REQUIRE(test::size_read_orig == 0);
    test::test_close();
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
    test::test_open(info.new_file.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600);
    REQUIRE(test::fh_orig != -1);

    for (size_t i = 0; i < info.num_iterations; ++i) {
      test::test_seek(0, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      test::test_write(info.write_data.data(), args.request_size);
      REQUIRE(test::size_written_orig == args.request_size);
    }
    test::test_close();
    REQUIRE(test::status_orig == 0);
    REQUIRE(fs::file_size(info.new_file) == args.request_size);
  }

  SECTION("write to new file always at start") {
    test::test_open(info.new_file.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600);
    REQUIRE(test::fh_orig != -1);

    for (size_t i = 0; i < info.num_iterations; ++i) {
      test::test_write(info.write_data.data(), args.request_size);
      REQUIRE(test::size_written_orig == args.request_size);
    }
    test::test_close();
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
    test::test_open(info.existing_file.c_str(), O_RDWR);
    REQUIRE(test::fh_orig != -1);
    std::string data(args.request_size, '1');
    for (size_t i = 0; i < info.num_iterations; ++i) {
      test::test_read(data.data(), args.request_size);
      REQUIRE(test::size_read_orig == args.request_size);
    }
    test::test_close();
    REQUIRE(test::status_orig == 0);
  }

  SECTION("read from existing file always at start") {
    test::test_open(info.existing_file.c_str(), O_WRONLY);
    REQUIRE(test::fh_orig != -1);

    for (size_t i = 0; i < info.num_iterations; ++i) {
      test::test_seek(0, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      test::test_write(info.write_data.data(), args.request_size);
      REQUIRE(test::size_written_orig == args.request_size);
    }
    test::test_close();
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
    test::test_open(info.existing_file.c_str(), O_RDWR);
    REQUIRE(test::fh_orig != -1);
    std::string data(args.request_size, '1');
    for (size_t i = 0; i < info.num_iterations; ++i) {
      size_t offset =
          rand_r(&info.offset_seed) % (info.total_size - args.request_size);
      test::test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)test::status_orig) == offset);
      test::test_read(data.data(), args.request_size);
      REQUIRE(test::size_read_orig == args.request_size);
    }
    test::test_close();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedUpdateRandom", "[process=" + std::to_string(info.comm_size) +
                                     "]"
                                     "[operation=batched_write]"
                                     "[request_size=type-fixed][repetition=" +
                                     std::to_string(info.num_iterations) +
                                     "]"
                                     "[pattern=random][file=1]") {
  pretest();
  SECTION("update into existing file") {
    test::test_open(info.existing_file.c_str(), O_RDWR);
    REQUIRE(test::fh_orig != -1);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      size_t offset =
          rand_r(&info.offset_seed) % (info.total_size - args.request_size - 1);
      test::test_seek(offset, SEEK_SET);  // 630978
      REQUIRE(((size_t)test::status_orig) == offset);
      test::test_write(info.write_data.data(), args.request_size);
      REQUIRE(test::size_written_orig == args.request_size);
      fsync(test::fh_orig);
    }
    test::test_close();
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
    test::test_open(info.existing_file.c_str(), O_RDWR);
    REQUIRE(test::fh_orig != -1);
    std::string data(args.request_size, '1');
    for (size_t i = 0; i < info.num_iterations; ++i) {
      size_t offset = (i * info.stride_size) % info.total_size;
      test::test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)test::status_orig) == offset);
      test::test_read(data.data(), args.request_size);
      REQUIRE(test::size_read_orig == args.request_size);
    }
    test::test_close();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedUpdateStrideFixed",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_write]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=stride_fixed][file=1]") {
  pretest();

  SECTION("read from existing file") {
    test::test_open(info.existing_file.c_str(), O_RDWR);
    REQUIRE(test::fh_orig != -1);
    std::string data(args.request_size, '1');
    for (size_t i = 0; i < info.num_iterations; ++i) {
      size_t offset = (i * info.stride_size) % info.total_size;
      test::test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)test::status_orig) == offset);
      test::test_write(data.data(), args.request_size);
      REQUIRE(test::size_read_orig == args.request_size);
    }
    test::test_close();
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
    test::test_open(info.existing_file.c_str(), O_RDWR);
    REQUIRE(test::fh_orig != -1);
    std::string data(args.request_size, '1');
    for (size_t i = 0; i < info.num_iterations; ++i) {
      size_t offset = GetRandomOffset(i, info.offset_seed, info.stride_size,
                                      info.total_size);
      test::test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)test::status_orig) == offset);
      test::test_read(data.data(), args.request_size);
      REQUIRE(test::size_read_orig == args.request_size);
    }
    test::test_close();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedUpdateStrideDynamic",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_write]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=stride_dynamic][file=1]") {
  pretest();
  SECTION("read from existing file") {
    test::test_open(info.existing_file.c_str(), O_RDWR);
    REQUIRE(test::fh_orig != -1);
    std::string data(args.request_size, '1');
    for (size_t i = 0; i < info.num_iterations; ++i) {
      size_t offset = GetRandomOffset(i, info.offset_seed, info.stride_size,
                                      info.total_size);
      test::test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)test::status_orig) == offset);
      test::test_write(data.data(), args.request_size);
      REQUIRE(test::size_read_orig == args.request_size);
    }
    test::test_close();
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
    test::test_open(info.new_file.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600);
    REQUIRE(test::fh_orig != -1);
    size_t biggest_size_written = 0;
    for (size_t i = 0; i < info.num_iterations; ++i) {
      test::test_seek(0, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t request_size =
          args.request_size + (rand_r(&info.offset_seed) % args.request_size);
      std::string data(request_size, '1');
      test::test_write(data.c_str(), request_size);
      REQUIRE(test::size_written_orig == request_size);
      if (biggest_size_written < request_size)
        biggest_size_written = request_size;
    }
    test::test_close();
    REQUIRE(test::status_orig == 0);
    REQUIRE(fs::file_size(info.new_file) == biggest_size_written);
  }

  SECTION("write to new file") {
    test::test_open(info.new_file.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600);
    REQUIRE(test::fh_orig != -1);
    size_t total_size_written = 0;
    for (size_t i = 0; i < info.num_iterations; ++i) {
      size_t request_size =
          args.request_size + (rand_r(&info.offset_seed) % args.request_size);
      std::string data(request_size, '1');
      test::test_write(data.c_str(), request_size);
      REQUIRE(test::size_written_orig == request_size);
      total_size_written += test::size_written_orig;
    }
    test::test_close();
    REQUIRE(test::status_orig == 0);
    REQUIRE(fs::file_size(info.new_file) == total_size_written);
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
    test::test_open(info.existing_file.c_str(), O_RDONLY);
    REQUIRE(test::fh_orig != -1);
    std::string data(args.request_size, '1');
    size_t current_offset = 0;
    for (size_t i = 0; i < info.num_iterations; ++i) {
      size_t request_size = (args.request_size +
                             (rand_r(&info.offset_seed) % args.request_size)) %
                            (info.total_size - current_offset);
      std::string data(request_size, '1');
      test::test_read(data.data(), request_size);
      REQUIRE(test::size_read_orig == request_size);
      current_offset += test::size_read_orig;
    }
    test::test_close();
    REQUIRE(test::status_orig == 0);
  }

  SECTION("read from existing file always at start") {
    test::test_open(info.existing_file.c_str(), O_RDONLY);
    REQUIRE(test::fh_orig != -1);

    for (size_t i = 0; i < info.num_iterations; ++i) {
      test::test_seek(0, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      size_t request_size =
          args.request_size + (rand_r(&info.offset_seed) % args.request_size);
      std::string data(request_size, '1');
      test::test_read(data.data(), request_size);
      REQUIRE(test::size_read_orig == request_size);
    }
    test::test_close();
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
    test::test_open(info.existing_file.c_str(), O_RDWR);
    REQUIRE(test::fh_orig != -1);
    std::string data(args.request_size, '1');
    for (size_t i = 0; i < info.num_iterations; ++i) {
      size_t offset =
          rand_r(&info.offset_seed) % (info.total_size - args.request_size);
      test::test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)test::status_orig) == offset);
      size_t request_size =
          (args.request_size + (rand_r(&info.rs_seed) % args.request_size)) %
          (info.total_size - offset);
      std::string data(request_size, '1');
      test::test_read(data.data(), request_size);
      REQUIRE(test::size_read_orig == request_size);
    }
    test::test_close();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedUpdateRandomRSVariable",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_write]"
              "[request_size=type-variable][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=random][file=1]") {
  pretest();

  SECTION("read from existing file") {
    test::test_open(info.existing_file.c_str(), O_RDWR);
    REQUIRE(test::fh_orig != -1);
    std::string data(args.request_size, '1');
    for (size_t i = 0; i < info.num_iterations; ++i) {
      size_t offset =
          rand_r(&info.offset_seed) % (info.total_size - args.request_size);
      test::test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)test::status_orig) == offset);
      size_t request_size =
          args.request_size + (rand_r(&info.rs_seed) % args.request_size);
      std::string data(request_size, '1');
      test::test_write(data.c_str(), request_size);
      REQUIRE(test::size_written_orig == request_size);
    }
    test::test_close();
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
    test::test_open(info.existing_file.c_str(), O_RDWR);
    REQUIRE(test::fh_orig != -1);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      size_t offset = (i * info.stride_size) % info.total_size;
      test::test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)test::status_orig) == offset);
      size_t request_size =
          (args.request_size + (rand_r(&info.rs_seed) % args.request_size)) %
          (info.total_size - offset);
      std::string data(request_size, '1');
      test::test_read(data.data(), request_size);
      REQUIRE(test::size_read_orig == request_size);
    }
    test::test_close();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedUpdateStrideFixedRSVariable",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_write]"
              "[request_size=type-variable][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=stride_fixed][file=1]") {
  pretest();

  SECTION("write to existing file") {
    test::test_open(info.existing_file.c_str(), O_RDWR);
    REQUIRE(test::fh_orig != -1);
    std::string data(args.request_size, '1');
    for (size_t i = 0; i < info.num_iterations; ++i) {
      size_t offset = (i * info.stride_size) % info.total_size;
      test::test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)test::status_orig) == offset);
      size_t request_size =
          args.request_size + (rand_r(&info.rs_seed) % args.request_size);
      std::string data(request_size, '1');
      test::test_write(data.c_str(), request_size);
      REQUIRE(test::size_written_orig == request_size);
    }
    test::test_close();
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
    test::test_open(info.existing_file.c_str(), O_RDWR);
    REQUIRE(test::fh_orig != -1);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      size_t offset = GetRandomOffset(i, info.offset_seed, info.stride_size,
                                      info.total_size);
      test::test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)test::status_orig) == offset);
      size_t request_size =
          args.request_size + (rand_r(&info.rs_seed) % args.request_size);
      std::string data(request_size, '1');
      test::test_read(data.data(), request_size);
      REQUIRE(test::size_read_orig == request_size);
    }
    test::test_close();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedUpdateStrideDynamicRSVariable",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_write]"
              "[request_size=type-variable][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=stride_dynamic][file=1]") {
  pretest();
  SECTION("read from existing file") {
    test::test_open(info.existing_file.c_str(), O_RDWR);
    REQUIRE(test::fh_orig != -1);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      size_t offset = GetRandomOffset(i, info.offset_seed, info.stride_size,
                                      info.total_size);
      test::test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)test::status_orig) == offset);
      size_t request_size =
          args.request_size + (rand_r(&info.rs_seed) % args.request_size);
      std::string data(request_size, '1');
      test::test_write(data.c_str(), request_size);
      REQUIRE(test::size_written_orig == request_size);
    }
    test::test_close();
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
    test::test_open(info.existing_file.c_str(), O_RDWR);
    REQUIRE(test::fh_orig != -1);
    std::string data(args.request_size, '1');
    size_t prev_offset = info.total_size + 1;
    for (size_t i = 0; i < info.num_iterations; ++i) {
      auto stride_offset = info.total_size - i * info.stride_size;
      REQUIRE(prev_offset > stride_offset);
      prev_offset = stride_offset;
      size_t offset = (stride_offset) % (info.total_size - args.request_size);
      test::test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)test::status_orig) == offset);
      test::test_read(data.data(), args.request_size);
      REQUIRE(test::size_read_orig == args.request_size);
    }
    test::test_close();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedUpdateStrideNegative",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_write]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=stride_negative][file=1]") {
  pretest();
  SECTION("read from existing file") {
    test::test_open(info.existing_file.c_str(), O_RDWR);
    REQUIRE(test::fh_orig != -1);
    std::string data(args.request_size, '1');
    for (size_t i = 0; i < info.num_iterations; ++i) {
      size_t offset =
          info.total_size - ((i * info.stride_size) % info.total_size);
      test::test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)test::status_orig) == offset);
      test::test_write(data.data(), args.request_size);
      REQUIRE(test::size_read_orig == args.request_size);
    }
    test::test_close();
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
    test::test_open(info.existing_file.c_str(), O_RDWR);
    REQUIRE(test::fh_orig != -1);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      size_t offset = (info.total_size - i * info.stride_size) %
                      (info.total_size - 2 * args.request_size);
      test::test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)test::status_orig) == offset);
      size_t request_size =
          (args.request_size + (rand_r(&info.rs_seed) % args.request_size)) %
          (info.total_size - offset);
      std::string data(request_size, '1');
      test::test_read(data.data(), request_size);
      REQUIRE(test::size_read_orig == request_size);
    }
    test::test_close();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedUpdateStrideNegativeRSVariable",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_write]"
              "[request_size=type-variable][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=stride_negative][file=1]") {
  pretest();

  SECTION("write to existing file") {
    test::test_open(info.existing_file.c_str(), O_RDWR);
    REQUIRE(test::fh_orig != -1);
    std::string data(args.request_size, '1');
    for (size_t i = 0; i < info.num_iterations; ++i) {
      size_t offset =
          info.total_size - ((i * info.stride_size) % info.total_size);
      test::test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)test::status_orig) == offset);
      size_t request_size =
          args.request_size + (rand_r(&info.rs_seed) % args.request_size);
      std::string data(request_size, '1');
      test::test_write(data.c_str(), request_size);
      REQUIRE(test::size_written_orig == request_size);
    }
    test::test_close();
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
    test::test_open(info.existing_file.c_str(), O_RDWR);
    REQUIRE(test::fh_orig != -1);
    std::string data(args.request_size, '1');
    size_t prev_cell_col = 0, prev_cell_row = 0;
    for (size_t i = 0; i < info.num_iterations; ++i) {
      size_t current_cell_col = (prev_cell_col + cell_stride) % cols;
      size_t current_cell_row = prev_cell_col + cell_stride > cols
                                    ? prev_cell_row + 1
                                    : prev_cell_row;
      prev_cell_row = current_cell_row;
      size_t offset = (current_cell_col * cell_stride + prev_cell_row * cols) %
                      (info.total_size - args.request_size);
      test::test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)test::status_orig) == offset);
      test::test_read(data.data(), args.request_size);
      REQUIRE(test::size_read_orig == args.request_size);
    }
    test::test_close();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedUpdateStride2D",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_write]"
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
    test::test_open(info.existing_file.c_str(), O_RDWR);
    REQUIRE(test::fh_orig != -1);
    std::string data(args.request_size, '1');
    size_t prev_cell_col = 0, prev_cell_row = 0;
    for (size_t i = 0; i < info.num_iterations; ++i) {
      size_t current_cell_col = (prev_cell_col + cell_stride) % cols;
      size_t current_cell_row = prev_cell_col + cell_stride > cols
                                    ? prev_cell_row + 1
                                    : prev_cell_row;
      prev_cell_row = current_cell_row;
      size_t offset = (current_cell_col * cell_stride + prev_cell_row * cols) %
                      (info.total_size - args.request_size);
      test::test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)test::status_orig) == offset);
      test::test_write(data.data(), args.request_size);
      REQUIRE(test::size_read_orig == args.request_size);
    }
    test::test_close();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedReadStride2DRSVariable",
          "[process=" + std::to_string(info.comm_size) +
              "]"
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
    test::test_open(info.existing_file.c_str(), O_RDWR);
    REQUIRE(test::fh_orig != -1);
    size_t prev_cell_col = 0, prev_cell_row = 0;
    for (size_t i = 0; i < info.num_iterations; ++i) {
      size_t current_cell_col = (prev_cell_col + cell_stride) % cols;
      size_t current_cell_row = prev_cell_col + cell_stride > cols
                                    ? prev_cell_row + 1
                                    : prev_cell_row;
      prev_cell_row = current_cell_row;
      size_t offset = (current_cell_col * cell_stride + prev_cell_row * cols) %
                      (info.total_size - 2 * args.request_size);
      test::test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)test::status_orig) == offset);
      size_t request_size =
          (args.request_size + (rand_r(&info.rs_seed) % args.request_size)) %
          (info.total_size - offset);
      std::string data(request_size, '1');
      test::test_read(data.data(), request_size);
      REQUIRE(test::size_read_orig == request_size);
    }
    test::test_close();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("BatchedUpdateStride2DRSVariable",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_write]"
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
    test::test_open(info.existing_file.c_str(), O_RDWR);
    REQUIRE(test::fh_orig != -1);
    std::string data(args.request_size, '1');
    size_t prev_cell_col = 0, prev_cell_row = 0;
    for (size_t i = 0; i < info.num_iterations; ++i) {
      size_t current_cell_col = (prev_cell_col + cell_stride) % cols;
      size_t current_cell_row = prev_cell_col + cell_stride > cols
                                    ? prev_cell_row + 1
                                    : prev_cell_row;
      prev_cell_row = current_cell_row;
      size_t offset = (current_cell_col * cell_stride + prev_cell_row * cols) %
                      (info.total_size - 2 * args.request_size);
      test::test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)test::status_orig) == offset);
      size_t request_size =
          args.request_size + (rand_r(&info.rs_seed) % args.request_size);
      std::string data(request_size, '1');
      test::test_write(data.c_str(), request_size);
      REQUIRE(test::size_written_orig == request_size);
    }
    test::test_close();
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
    test::test_open(info.new_file.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600);
    REQUIRE(test::fh_orig != -1);

    for (size_t i = 0; i < info.num_iterations; ++i) {
      usleep(info.temporal_interval_ms * 1000);
      test::test_seek(0, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      test::test_write(info.write_data.data(), args.request_size);
      REQUIRE(test::size_written_orig == args.request_size);
    }
    test::test_close();
    REQUIRE(test::status_orig == 0);
    REQUIRE(fs::file_size(info.new_file) == args.request_size);
  }

  SECTION("write to new file always at start") {
    test::test_open(info.new_file.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600);
    REQUIRE(test::fh_orig != -1);

    for (size_t i = 0; i < info.num_iterations; ++i) {
      usleep(info.temporal_interval_ms * 1000);
      test::test_write(info.write_data.data(), args.request_size);
      REQUIRE(test::size_written_orig == args.request_size);
    }
    test::test_close();
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
    test::test_open(info.existing_file.c_str(), O_RDWR);
    REQUIRE(test::fh_orig != -1);
    std::string data(args.request_size, '1');
    for (size_t i = 0; i < info.num_iterations; ++i) {
      usleep(info.temporal_interval_ms * 1000);
      test::test_read(data.data(), args.request_size);
      REQUIRE(test::size_read_orig == args.request_size);
    }
    test::test_close();
    REQUIRE(test::status_orig == 0);
  }

  SECTION("read from existing file always at start") {
    test::test_open(info.existing_file.c_str(), O_WRONLY);
    REQUIRE(test::fh_orig != -1);

    for (size_t i = 0; i < info.num_iterations; ++i) {
      usleep(info.temporal_interval_ms * 1000);
      test::test_seek(0, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      test::test_write(info.write_data.data(), args.request_size);
      REQUIRE(test::size_written_orig == args.request_size);
    }
    test::test_close();
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
    test::test_open(info.new_file.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600);
    REQUIRE(test::fh_orig != -1);

    for (size_t i = 0; i < info.num_iterations; ++i) {
      info.temporal_interval_ms =
          rand_r(&info.temporal_interval_seed) % info.temporal_interval_ms + 1;
      usleep(info.temporal_interval_ms * 1000);
      test::test_seek(0, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      test::test_write(info.write_data.data(), args.request_size);
      REQUIRE(test::size_written_orig == args.request_size);
    }
    test::test_close();
    REQUIRE(test::status_orig == 0);
    REQUIRE(fs::file_size(info.new_file) == args.request_size);
  }

  SECTION("write to new file always at start") {
    test::test_open(info.new_file.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600);
    REQUIRE(test::fh_orig != -1);

    for (size_t i = 0; i < info.num_iterations; ++i) {
      info.temporal_interval_ms =
          rand_r(&info.temporal_interval_seed) % info.temporal_interval_ms + 1;
      usleep(info.temporal_interval_ms * 1000);
      test::test_write(info.write_data.data(), args.request_size);
      REQUIRE(test::size_written_orig == args.request_size);
    }
    test::test_close();
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
    test::test_open(info.existing_file.c_str(), O_RDWR);
    REQUIRE(test::fh_orig != -1);
    std::string data(args.request_size, '1');
    for (size_t i = 0; i < info.num_iterations; ++i) {
      info.temporal_interval_ms =
          rand_r(&info.temporal_interval_seed) % info.temporal_interval_ms + 1;
      usleep(info.temporal_interval_ms * 1000);
      test::test_read(data.data(), args.request_size);
      REQUIRE(test::size_read_orig == args.request_size);
    }
    test::test_close();
    REQUIRE(test::status_orig == 0);
  }

  SECTION("read from existing file always at start") {
    test::test_open(info.existing_file.c_str(), O_WRONLY);
    REQUIRE(test::fh_orig != -1);

    for (size_t i = 0; i < info.num_iterations; ++i) {
      info.temporal_interval_ms =
          rand_r(&info.temporal_interval_seed) % info.temporal_interval_ms + 1;
      usleep(info.temporal_interval_ms * 1000);
      test::test_seek(0, SEEK_SET);
      REQUIRE(test::status_orig == 0);
      test::test_write(info.write_data.data(), args.request_size);
      REQUIRE(test::size_written_orig == args.request_size);
    }
    test::test_close();
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
    test::test_open(info.new_file.c_str(), O_RDWR | O_CREAT | O_EXCL, 0600);
    REQUIRE(test::fh_orig != -1);
    size_t last_offset = 0;
    for (size_t i = 0; i < info.num_iterations; ++i) {
      test::test_write(info.write_data.data(), args.request_size);
      REQUIRE(test::size_written_orig == args.request_size);
      test::test_seek(last_offset, SEEK_SET);
      REQUIRE(((size_t)test::status_orig) == last_offset);
      test::test_read(info.read_data.data(), args.request_size);
      REQUIRE(test::size_read_orig == args.request_size);
      last_offset += args.request_size;
    }
    test::test_close();
    REQUIRE(test::status_orig == 0);
  }

  SECTION("write and read alternative existing file") {
    test::test_open(info.existing_file.c_str(), O_RDWR);
    REQUIRE(test::fh_orig != -1);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      if (i % 2 == 0) {
        test::test_write(info.write_data.data(), args.request_size);
        REQUIRE(test::size_written_orig == args.request_size);
      } else {
        test::test_read(info.read_data.data(), args.request_size);
        REQUIRE(test::size_read_orig == args.request_size);
      }
    }
    test::test_close();
    REQUIRE(test::status_orig == 0);
  }
  SECTION("update after read existing file") {
    test::test_open(info.existing_file.c_str(), O_RDWR);
    REQUIRE(test::fh_orig != -1);
    size_t last_offset = 0;
    for (size_t i = 0; i < info.num_iterations; ++i) {
      test::test_read(info.read_data.data(), args.request_size);
      REQUIRE(test::size_read_orig == args.request_size);
      test::test_seek(last_offset, SEEK_SET);
      REQUIRE(((size_t)test::status_orig) == last_offset);
      test::test_write(info.write_data.data(), args.request_size);
      REQUIRE(test::size_written_orig == args.request_size);
      last_offset += args.request_size;
    }

    test::test_close();
    REQUIRE(test::status_orig == 0);
  }
  SECTION("read all after write all on new file in single open") {
    test::test_open(info.new_file.c_str(), O_RDWR | O_CREAT | O_EXCL, 0600);
    REQUIRE(test::fh_orig != -1);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      test::test_write(info.write_data.data(), args.request_size);
      REQUIRE(test::size_written_orig == args.request_size);
    }
    test::test_seek(0, SEEK_SET);
    REQUIRE(test::status_orig == 0);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      test::test_read(info.read_data.data(), args.request_size);
      REQUIRE(test::size_read_orig == args.request_size);
    }
    test::test_close();
    REQUIRE(test::status_orig == 0);
  }
  SECTION("read all after write all on new file in different open") {
    test::test_open(info.new_file.c_str(), O_RDWR | O_CREAT, S_IRWXU | S_IRWXG);
    REQUIRE(test::fh_orig != -1);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      test::test_write(info.write_data.data(), args.request_size);
      REQUIRE(test::size_written_orig == args.request_size);
    }
    test::test_close();
    REQUIRE(test::status_orig == 0);
    test::test_open(info.new_file.c_str(), O_RDWR);
    REQUIRE(test::fh_orig != -1);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      test::test_read(info.read_data.data(), args.request_size);
      REQUIRE(test::size_read_orig == args.request_size);
    }
    test::test_close();
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
    test::test_open(info.new_file.c_str(), O_RDWR | O_CREAT | O_EXCL, 0600);
    REQUIRE(test::fh_orig != -1);
    test::test_write(info.write_data.data(), args.request_size);
    REQUIRE(test::size_written_orig == args.request_size);
    test::test_seek(0, SEEK_SET);
    REQUIRE(test::status_orig == 0);
    test::test_read(info.read_data.data(), args.request_size);
    REQUIRE(test::size_read_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == 0);
  }
  SECTION("update after read from existing file") {
    test::test_open(info.existing_file.c_str(), O_RDWR);
    REQUIRE(test::fh_orig != -1);
    test::test_read(info.read_data.data(), args.request_size);
    REQUIRE(test::size_read_orig == args.request_size);
    test::test_seek(0, SEEK_SET);
    REQUIRE(test::status_orig == 0);
    test::test_write(info.write_data.data(), args.request_size);
    REQUIRE(test::size_written_orig == args.request_size);

    test::test_close();
    REQUIRE(test::status_orig == 0);
  }
  SECTION("read after write from new file different opens") {
    test::test_open(info.new_file.c_str(), O_RDWR | O_CREAT | O_EXCL, 0600);
    REQUIRE(test::fh_orig != -1);
    test::test_write(info.write_data.data(), args.request_size);
    REQUIRE(test::size_written_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == 0);
    test::test_open(info.new_file.c_str(), O_RDWR);
    test::test_read(info.read_data.data(), args.request_size);
    REQUIRE(test::size_read_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("fstat") {
  pretest();

  SECTION("fstat on new file") {
    test::test_open(info.new_file.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600);
    REQUIRE(test::fh_orig != -1);
    test::test_write(info.write_data.data(), args.request_size);
    REQUIRE(test::size_written_orig == args.request_size);

    struct stat buf = {};
    int result = fstat(test::fh_orig, &buf);
    REQUIRE(result == 0);
    REQUIRE(buf.st_size == (off_t)test::size_written_orig);

    test::test_close();
    REQUIRE(test::status_orig == 0);
  }

  posttest();
}
