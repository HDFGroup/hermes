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
                      "[coordination=independent]"
                      "[synchronicity=sync]"
                      "[operation=single_open]"
                      "[repetition=1][file=1]") {
  pretest();
  SECTION("open non-existant file") {
    test::test_open(info.new_file.c_str(), MPI_MODE_RDONLY, MPI_COMM_SELF);
    REQUIRE(test::status_orig != MPI_SUCCESS);
    test::test_open(info.new_file.c_str(), MPI_MODE_RDWR | MPI_MODE_EXCL,
                    MPI_COMM_SELF);
    REQUIRE(test::status_orig != MPI_SUCCESS);
  }

  SECTION("opening existing file write-only") {
    test::test_open(info.existing_file.c_str(), MPI_MODE_WRONLY | MPI_MODE_EXCL,
                    MPI_COMM_SELF);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
  }
  SECTION("opening existing file and read/write") {
    test::test_open(info.existing_file.c_str(), MPI_MODE_RDWR | MPI_MODE_CREATE,
                    MPI_COMM_SELF);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
  }

  SECTION("open existing file") {
    test::test_open(info.existing_file.c_str(), MPI_MODE_RDONLY | MPI_MODE_EXCL,
                    MPI_COMM_SELF);
    REQUIRE(test::status_orig != MPI_SUCCESS);
    test::test_open(info.existing_file.c_str(), MPI_MODE_RDWR, MPI_COMM_SELF);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_open(info.existing_file.c_str(), MPI_MODE_RDONLY, MPI_COMM_SELF);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
  }

  SECTION("append write existing file") {
    test::test_open(info.existing_file.c_str(), MPI_MODE_APPEND | MPI_MODE_EXCL,
                    MPI_COMM_SELF);
    REQUIRE(test::status_orig != MPI_SUCCESS);
    test::test_open(info.existing_file.c_str(), MPI_MODE_APPEND, MPI_COMM_SELF);
    REQUIRE(test::status_orig != MPI_SUCCESS);
    test::test_open(info.existing_file.c_str(),
                    MPI_MODE_WRONLY | MPI_MODE_APPEND, MPI_COMM_SELF);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_open(info.existing_file.c_str(),
                    MPI_MODE_RDONLY | MPI_MODE_APPEND, MPI_COMM_SELF);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_open(info.existing_file.c_str(), MPI_MODE_RDWR | MPI_MODE_APPEND,
                    MPI_COMM_SELF);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
  }

  SECTION("append write and read new file") {
    test::test_open(info.existing_file.c_str(),
                    MPI_MODE_RDWR | MPI_MODE_APPEND | MPI_MODE_CREATE,
                    MPI_COMM_SELF);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
  }

  SECTION("delete on close mode") {
    test::test_open(
        info.new_file.c_str(),
        MPI_MODE_WRONLY | MPI_MODE_CREATE | MPI_MODE_DELETE_ON_CLOSE,
        MPI_COMM_SELF);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    REQUIRE(fs::exists(info.new_file.c_str()));
    test::test_close();
    REQUIRE(!fs::exists(info.new_file.c_str()));
    REQUIRE(test::status_orig == MPI_SUCCESS);
  }
  posttest();
}

TEST_CASE("OpenCollective", "[process=" + std::to_string(info.comm_size) +
                                "]"
                                "[coordination=collective]"
                                "[synchronicity=sync]"
                                "[operation=single_open]"
                                "[repetition=1][file=1]") {
  pretest();
  SECTION("open on non-existant shared file") {
    test::test_open(info.shared_new_file.c_str(),
                    MPI_MODE_RDONLY | MPI_MODE_EXCL, MPI_COMM_WORLD);
    REQUIRE(test::status_orig != MPI_SUCCESS);
    test::test_open(info.shared_new_file.c_str(),
                    MPI_MODE_RDONLY | MPI_MODE_CREATE, MPI_COMM_WORLD);
    REQUIRE(test::status_orig != MPI_SUCCESS);
    test::test_open(info.shared_new_file.c_str(), MPI_MODE_RDWR | MPI_MODE_EXCL,
                    MPI_COMM_WORLD);
    REQUIRE(test::status_orig != MPI_SUCCESS);
    test::test_open(info.shared_new_file.c_str(),
                    MPI_MODE_WRONLY | MPI_MODE_CREATE, MPI_COMM_WORLD);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_open(info.shared_new_file.c_str(),
                    MPI_MODE_RDWR | MPI_MODE_CREATE, MPI_COMM_WORLD);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
  }

  SECTION("open on shared existing file") {
    test::test_open(info.shared_existing_file.c_str(),
                    MPI_MODE_WRONLY | MPI_MODE_EXCL, MPI_COMM_WORLD);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_open(info.shared_existing_file.c_str(),
                    MPI_MODE_RDWR | MPI_MODE_EXCL, MPI_COMM_WORLD);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_open(info.shared_existing_file.c_str(),
                    MPI_MODE_RDONLY | MPI_MODE_EXCL, MPI_COMM_WORLD);
    REQUIRE(test::status_orig != MPI_SUCCESS);
    test::test_open(info.shared_existing_file.c_str(), MPI_MODE_RDONLY,
                    MPI_COMM_WORLD);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_open(info.shared_existing_file.c_str(), MPI_MODE_RDWR,
                    MPI_COMM_WORLD);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
  }

  SECTION("append write/read on shared existing file") {
    test::test_open(info.shared_existing_file.c_str(),
                    MPI_MODE_RDWR | MPI_MODE_APPEND, MPI_COMM_WORLD);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
  }

  SECTION("append write and read on shared new file") {
    test::test_open(info.shared_new_file.c_str(),
                    MPI_MODE_RDWR | MPI_MODE_APPEND | MPI_MODE_CREATE,
                    MPI_COMM_WORLD);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
  }

  SECTION("delete on close mode on new file") {
    test::test_open(
        info.shared_new_file.c_str(),
        MPI_MODE_WRONLY | MPI_MODE_CREATE | MPI_MODE_DELETE_ON_CLOSE,
        MPI_COMM_WORLD);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
    REQUIRE(!fs::exists(info.shared_new_file.c_str()));
  }
  posttest();
}

TEST_CASE("SingleWrite", "[process=" + std::to_string(info.comm_size) +
                             "]"
                             "[operation=single_write]"
                             "[synchronicity=sync]"
                             "[coordination=independent]"
                             "[request_size=type-fixed][repetition=1]"
                             "[file=1]") {
  pretest();
  bool check_bytes = true;
  SECTION("write to existing file") {
    test::test_open(info.existing_file.c_str(), MPI_MODE_RDWR, MPI_COMM_SELF);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_seek(info.rank * args.request_size, MPI_SEEK_SET);
    REQUIRE(test::status_orig == 0);
    test::test_write(info.write_data.c_str(), args.request_size, MPI_CHAR);
    REQUIRE((size_t)test::size_written_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
  }

  SECTION("write to new  file") {
    test::test_open(info.new_file.c_str(), MPI_MODE_WRONLY | MPI_MODE_CREATE,
                    MPI_COMM_SELF);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_write(info.write_data.c_str(), args.request_size, MPI_CHAR);
    REQUIRE((size_t)test::size_written_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
    REQUIRE(fs::file_size(info.new_file) == (size_t)test::size_written_orig);
  }

  SECTION("write to new  file using shared ptr") {
    test::test_open(info.shared_new_file.c_str(),
                    MPI_MODE_WRONLY | MPI_MODE_CREATE, MPI_COMM_SELF);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_seek_shared(0, MPI_SEEK_SET);
    REQUIRE(test::status_orig == 0);
    test::test_write_shared(info.write_data.c_str(), args.request_size,
                            MPI_CHAR);
    REQUIRE((size_t)test::size_written_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
    REQUIRE(fs::file_size(info.shared_new_file) ==
            (size_t)test::size_written_orig);
  }

  SECTION("write to new file with allocate") {
    test::test_open(info.shared_new_file.c_str(),
                    MPI_MODE_WRONLY | MPI_MODE_CREATE, MPI_COMM_SELF);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_preallocate(args.request_size * info.comm_size);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_seek(args.request_size * info.rank, MPI_SEEK_SET);
    REQUIRE(test::status_orig == 0);
    test::test_write(info.write_data.c_str(), args.request_size, MPI_CHAR);
    REQUIRE((size_t)test::size_written_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
    MPI_Barrier(MPI_COMM_WORLD);
    REQUIRE(fs::file_size(info.shared_new_file) ==
            (size_t)test::size_written_orig * info.comm_size);
  }

  SECTION("append to existing file") {
    auto existing_size = fs::file_size(info.existing_file);
    test::test_open(info.existing_file.c_str(),
                    MPI_MODE_WRONLY | MPI_MODE_APPEND | MPI_MODE_EXCL,
                    MPI_COMM_SELF);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_write(info.write_data.c_str(), args.request_size, MPI_CHAR);
    REQUIRE((size_t)test::size_written_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
    REQUIRE(fs::file_size(info.existing_file) ==
            existing_size + test::size_written_orig);
  }

  SECTION("append to new file") {
    test::test_open(info.new_file.c_str(),
                    MPI_MODE_WRONLY | MPI_MODE_APPEND | MPI_MODE_CREATE,
                    MPI_COMM_SELF);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_write(info.write_data.c_str(), args.request_size, MPI_CHAR);
    REQUIRE((size_t)test::size_written_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
    REQUIRE(fs::file_size(info.new_file) == (size_t)test::size_written_orig);
  }

  SECTION("write_at to existing file") {
    test::test_open(info.existing_file.c_str(), MPI_MODE_RDWR, MPI_COMM_SELF);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_write_at(info.write_data.c_str(), args.request_size, MPI_CHAR,
                        info.rank * args.request_size);
    REQUIRE((size_t)test::size_written_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
  }

  SECTION("write_at to new file") {
    test::test_open(info.new_file.c_str(), MPI_MODE_WRONLY | MPI_MODE_CREATE,
                    MPI_COMM_SELF);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_write_at(info.write_data.c_str(), args.request_size, MPI_CHAR,
                        0);
    REQUIRE((size_t)test::size_written_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
    REQUIRE(fs::file_size(info.new_file) == (size_t)test::size_written_orig);
  }

  SECTION("delete on close mode on new file") {
    test::test_open(
        info.new_file.c_str(),
        MPI_MODE_WRONLY | MPI_MODE_CREATE | MPI_MODE_DELETE_ON_CLOSE,
        MPI_COMM_SELF);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_seek(0, MPI_SEEK_SET);
    REQUIRE(test::status_orig == 0);
    test::test_write(info.write_data.c_str(), args.request_size, MPI_CHAR);
    REQUIRE((size_t)test::size_written_orig == args.request_size);
    REQUIRE(fs::exists(info.new_file.c_str()));
    REQUIRE(fs::file_size(info.new_file) == (size_t)test::size_written_orig);
    test::test_close();
    REQUIRE(!fs::exists(info.new_file.c_str()));
    REQUIRE(test::status_orig == MPI_SUCCESS);
  }

  SECTION("delete on close mode on existing file") {
    auto original_size = fs::file_size(info.existing_file);
    test::test_open(info.existing_file.c_str(),
                    MPI_MODE_WRONLY | MPI_MODE_EXCL | MPI_MODE_DELETE_ON_CLOSE,
                    MPI_COMM_SELF);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_seek(info.rank * args.request_size, MPI_SEEK_SET);
    REQUIRE(test::status_orig == 0);
    test::test_write(info.write_data.c_str(), args.request_size, MPI_CHAR);
    REQUIRE((size_t)test::size_written_orig == args.request_size);
    REQUIRE(fs::exists(info.existing_file.c_str()));
    auto new_size =
        original_size > (size_t)test::size_written_orig * info.comm_size
            ? original_size
            : test::size_written_orig * info.comm_size;
    REQUIRE(fs::file_size(info.existing_file) == (size_t)new_size);
    test::test_close();
    REQUIRE(!fs::exists(info.existing_file.c_str()));
    REQUIRE(test::status_orig == MPI_SUCCESS);
    check_bytes = false;
  }
  posttest(check_bytes);
}

TEST_CASE("SingleWriteCollective",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=single_write]"
              "[synchronicity=sync]"
              "[coordination=collective]"
              "[request_size=type-fixed][repetition=1]"
              "[file=1]") {
  pretest();
  bool check_bytes = true;
  SECTION("write to existing file") {
    test::test_open(info.shared_existing_file.c_str(), MPI_MODE_RDWR,
                    MPI_COMM_WORLD);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_seek(info.rank * args.request_size, MPI_SEEK_SET);
    REQUIRE(test::status_orig == 0);
    test::test_write(info.write_data.c_str(), args.request_size, MPI_CHAR);
    REQUIRE((size_t)test::size_written_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
  }

  SECTION("write to new  file") {
    test::test_open(info.shared_new_file.c_str(),
                    MPI_MODE_WRONLY | MPI_MODE_CREATE, MPI_COMM_WORLD);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_seek(0, MPI_SEEK_SET);
    REQUIRE(test::status_orig == 0);
    test::test_write(info.write_data.c_str(), args.request_size, MPI_CHAR);
    REQUIRE((size_t)test::size_written_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
    REQUIRE(fs::file_size(info.shared_new_file) ==
            (size_t)test::size_written_orig);
  }

  SECTION("write to new  file using shared ptr") {
    test::test_open(info.shared_new_file.c_str(),
                    MPI_MODE_WRONLY | MPI_MODE_CREATE, MPI_COMM_WORLD);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_seek_shared(0, MPI_SEEK_SET);
    REQUIRE(test::status_orig == 0);
    test::test_write_shared(info.write_data.c_str(), args.request_size,
                            MPI_CHAR);
    REQUIRE((size_t)test::size_written_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
    REQUIRE(fs::file_size(info.shared_new_file) ==
            (size_t)test::size_written_orig * info.comm_size);
  }

  SECTION("write to new file with allocate") {
    test::test_open(info.shared_new_file.c_str(),
                    MPI_MODE_WRONLY | MPI_MODE_CREATE, MPI_COMM_WORLD);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_preallocate(args.request_size * info.comm_size);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_seek(args.request_size * info.rank, MPI_SEEK_SET);
    REQUIRE(test::status_orig == 0);
    test::test_write(info.write_data.c_str(), args.request_size, MPI_CHAR);
    REQUIRE((size_t)test::size_written_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
    MPI_Barrier(MPI_COMM_WORLD);
    REQUIRE(fs::file_size(info.shared_new_file) ==
            (size_t)test::size_written_orig * info.comm_size);
  }

  SECTION("write_at_all to existing file") {
    test::test_open(info.existing_file.c_str(), MPI_MODE_RDWR, MPI_COMM_SELF);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_write_at_all(info.write_data.c_str(), args.request_size,
                            MPI_CHAR, info.rank * args.request_size);
    REQUIRE((size_t)test::size_written_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
  }

  SECTION("write_at_all to new file") {
    test::test_open(info.new_file.c_str(), MPI_MODE_WRONLY | MPI_MODE_CREATE,
                    MPI_COMM_SELF);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_write_at_all(info.write_data.c_str(), args.request_size,
                            MPI_CHAR, 0);
    REQUIRE((size_t)test::size_written_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
    REQUIRE(fs::file_size(info.new_file) == (size_t)test::size_written_orig);
  }

  SECTION("append to existing file") {
    auto existing_size = fs::file_size(info.existing_file);
    test::test_open(info.shared_existing_file.c_str(),
                    MPI_MODE_WRONLY | MPI_MODE_APPEND | MPI_MODE_EXCL,
                    MPI_COMM_WORLD);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_write_all(info.write_data.c_str(), args.request_size, MPI_CHAR);
    REQUIRE((size_t)test::size_written_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
    REQUIRE(fs::file_size(info.shared_existing_file) ==
            existing_size + test::size_written_orig);
  }

  SECTION("append to new file") {
    test::test_open(info.shared_new_file.c_str(),
                    MPI_MODE_WRONLY | MPI_MODE_CREATE, MPI_COMM_WORLD);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_write_all(info.write_data.c_str(), args.request_size, MPI_CHAR);
    REQUIRE((size_t)test::size_written_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
    REQUIRE(fs::file_size(info.shared_new_file) ==
            (size_t)test::size_written_orig);
  }

  SECTION("write_ordered to existing file") {
    test::test_open(info.existing_file.c_str(), MPI_MODE_RDWR, MPI_COMM_SELF);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_write_ordered(info.write_data.c_str(), args.request_size,
                             MPI_CHAR);
    REQUIRE((size_t)test::size_written_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
  }

  SECTION("write_ordered to new file") {
    test::test_open(info.new_file.c_str(), MPI_MODE_WRONLY | MPI_MODE_CREATE,
                    MPI_COMM_SELF);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_write_ordered(info.write_data.c_str(), args.request_size,
                             MPI_CHAR);
    REQUIRE((size_t)test::size_written_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
    REQUIRE(fs::file_size(info.new_file) == (size_t)test::size_written_orig);
  }
  SECTION("delete on close mode on new file") {
    test::test_open(
        info.shared_new_file.c_str(),
        MPI_MODE_WRONLY | MPI_MODE_CREATE | MPI_MODE_DELETE_ON_CLOSE,
        MPI_COMM_WORLD);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_seek(info.rank * args.request_size, MPI_SEEK_SET);
    REQUIRE(test::status_orig == 0);
    test::test_write(info.write_data.c_str(), args.request_size, MPI_CHAR);
    REQUIRE((size_t)test::size_written_orig == args.request_size);
    REQUIRE(fs::exists(info.shared_new_file.c_str()));
    MPI_Barrier(MPI_COMM_WORLD);
    REQUIRE(fs::file_size(info.shared_new_file) ==
            (size_t)test::size_written_orig * info.comm_size);
    test::test_close();
    REQUIRE(!fs::exists(info.shared_new_file.c_str()));
    REQUIRE(test::status_orig == MPI_SUCCESS);
  }

  SECTION("delete on close mode on existing file") {
    auto original_size = fs::file_size(info.shared_existing_file);
    test::test_open(info.shared_existing_file.c_str(),
                    MPI_MODE_WRONLY | MPI_MODE_EXCL | MPI_MODE_DELETE_ON_CLOSE,
                    MPI_COMM_WORLD);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_seek(info.rank * args.request_size, MPI_SEEK_SET);
    REQUIRE(test::status_orig == 0);
    test::test_write(info.write_data.c_str(), args.request_size, MPI_CHAR);
    REQUIRE((size_t)test::size_written_orig == args.request_size);
    REQUIRE(fs::exists(info.shared_existing_file.c_str()));
    auto new_size =
        original_size > (size_t)test::size_written_orig * info.comm_size
            ? original_size
            : test::size_written_orig * info.comm_size;
    REQUIRE(fs::file_size(info.shared_existing_file) == (size_t)new_size);
    test::test_close();
    REQUIRE(!fs::exists(info.shared_existing_file.c_str()));
    REQUIRE(test::status_orig == MPI_SUCCESS);
    check_bytes = false;
  }
  posttest(check_bytes);
}

TEST_CASE("SingleAsyncWrite", "[process=" + std::to_string(info.comm_size) +
                                  "]"
                                  "[operation=single_write]"
                                  "[coordination=independent]"
                                  "[synchronicity=async]"
                                  "[request_size=type-fixed][repetition=1]"
                                  "[file=1]") {
  pretest();
  bool check_bytes = true;
  SECTION("write to existing file") {
    test::test_open(info.existing_file.c_str(), MPI_MODE_RDWR, MPI_COMM_SELF);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_seek(info.rank * args.request_size, MPI_SEEK_SET);
    REQUIRE(test::status_orig == 0);
    test::test_iwrite(info.write_data.c_str(), args.request_size, MPI_CHAR);
    REQUIRE((size_t)test::size_written_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
  }

  SECTION("write to new  file") {
    test::test_open(info.new_file.c_str(), MPI_MODE_WRONLY | MPI_MODE_CREATE,
                    MPI_COMM_SELF);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_seek(0, MPI_SEEK_SET);
    REQUIRE(test::status_orig == 0);
    test::test_iwrite(info.write_data.c_str(), args.request_size, MPI_CHAR);
    REQUIRE((size_t)test::size_written_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
    REQUIRE(fs::file_size(info.new_file) == (size_t)test::size_written_orig);
  }

  SECTION("write to new  file using shared ptr") {
    test::test_open(info.shared_new_file.c_str(),
                    MPI_MODE_WRONLY | MPI_MODE_CREATE, MPI_COMM_SELF);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_seek_shared(0, MPI_SEEK_SET);
    REQUIRE(test::status_orig == 0);
    test::test_iwrite_shared(info.write_data.c_str(), args.request_size,
                             MPI_CHAR);
    REQUIRE((size_t)test::size_written_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
    REQUIRE(fs::file_size(info.shared_new_file) ==
            (size_t)test::size_written_orig);
  }

  SECTION("write to new file with allocate") {
    test::test_open(info.shared_new_file.c_str(),
                    MPI_MODE_WRONLY | MPI_MODE_CREATE, MPI_COMM_SELF);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_preallocate(args.request_size * info.comm_size);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_seek(args.request_size * info.rank, MPI_SEEK_SET);
    REQUIRE(test::status_orig == 0);
    test::test_write(info.write_data.c_str(), args.request_size, MPI_CHAR);
    REQUIRE((size_t)test::size_written_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
    MPI_Barrier(MPI_COMM_WORLD);
    REQUIRE(fs::file_size(info.shared_new_file) ==
            (size_t)test::size_written_orig * info.comm_size);
  }

  SECTION("append to existing file") {
    auto existing_size = fs::file_size(info.existing_file);
    test::test_open(info.existing_file.c_str(),
                    MPI_MODE_WRONLY | MPI_MODE_APPEND | MPI_MODE_EXCL,
                    MPI_COMM_SELF);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_iwrite(info.write_data.c_str(), args.request_size, MPI_CHAR);
    REQUIRE((size_t)test::size_written_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
    REQUIRE(fs::file_size(info.existing_file) ==
            existing_size + test::size_written_orig);
  }

  SECTION("append to new file") {
    test::test_open(info.new_file.c_str(),
                    MPI_MODE_WRONLY | MPI_MODE_APPEND | MPI_MODE_CREATE,
                    MPI_COMM_SELF);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_iwrite(info.write_data.c_str(), args.request_size, MPI_CHAR);
    REQUIRE((size_t)test::size_written_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
    REQUIRE(fs::file_size(info.new_file) == (size_t)test::size_written_orig);
  }

  SECTION("write_at to existing file") {
    test::test_open(info.existing_file.c_str(), MPI_MODE_RDWR, MPI_COMM_SELF);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_iwrite_at(info.write_data.c_str(), args.request_size, MPI_CHAR,
                         info.rank * args.request_size);
    REQUIRE((size_t)test::size_written_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
  }

  SECTION("write_at to new file") {
    test::test_open(info.new_file.c_str(), MPI_MODE_WRONLY | MPI_MODE_CREATE,
                    MPI_COMM_SELF);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_iwrite_at(info.write_data.c_str(), args.request_size, MPI_CHAR,
                         0);
    REQUIRE((size_t)test::size_written_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
    REQUIRE(fs::file_size(info.new_file) == (size_t)test::size_written_orig);
  }

  SECTION("delete on close mode on new file") {
    test::test_open(
        info.new_file.c_str(),
        MPI_MODE_WRONLY | MPI_MODE_CREATE | MPI_MODE_DELETE_ON_CLOSE,
        MPI_COMM_SELF);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_seek(0, MPI_SEEK_SET);
    REQUIRE(test::status_orig == 0);
    test::test_iwrite(info.write_data.c_str(), args.request_size, MPI_CHAR);
    REQUIRE((size_t)test::size_written_orig == args.request_size);
    REQUIRE(fs::exists(info.new_file.c_str()));
    REQUIRE(fs::file_size(info.new_file) == (size_t)test::size_written_orig);
    test::test_close();
    REQUIRE(!fs::exists(info.new_file.c_str()));
    REQUIRE(test::status_orig == MPI_SUCCESS);
  }

  SECTION("delete on close mode on existing file") {
    auto original_size = fs::file_size(info.existing_file);
    test::test_open(info.existing_file.c_str(),
                    MPI_MODE_WRONLY | MPI_MODE_EXCL | MPI_MODE_DELETE_ON_CLOSE,
                    MPI_COMM_SELF);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_seek(info.rank * args.request_size, MPI_SEEK_SET);
    REQUIRE(test::status_orig == 0);
    test::test_iwrite(info.write_data.c_str(), args.request_size, MPI_CHAR);
    REQUIRE((size_t)test::size_written_orig == args.request_size);
    REQUIRE(fs::exists(info.existing_file.c_str()));
    auto new_size =
        original_size > (size_t)test::size_written_orig * info.comm_size
            ? original_size
            : test::size_written_orig * info.comm_size;
    REQUIRE(fs::file_size(info.existing_file) == (size_t)new_size);
    test::test_close();
    REQUIRE(!fs::exists(info.existing_file.c_str()));
    REQUIRE(test::status_orig == MPI_SUCCESS);
    check_bytes = false;
  }
  posttest(check_bytes);
}

TEST_CASE("SingleAsyncWriteCollective",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=single_write]"
              "[synchronicity=async]"
              "[coordination=collective]"
              "[request_size=type-fixed][repetition=1]"
              "[file=1]") {
  pretest();
  bool check_bytes = true;
  SECTION("write to existing file") {
    test::test_open(info.shared_existing_file.c_str(), MPI_MODE_RDWR,
                    MPI_COMM_WORLD);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_seek(info.rank * args.request_size, MPI_SEEK_SET);
    REQUIRE(test::status_orig == 0);
    test::test_iwrite(info.write_data.c_str(), args.request_size, MPI_CHAR);
    REQUIRE((size_t)test::size_written_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
  }

  SECTION("write to new  file") {
    test::test_open(info.shared_new_file.c_str(),
                    MPI_MODE_WRONLY | MPI_MODE_CREATE, MPI_COMM_WORLD);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_seek(0, MPI_SEEK_SET);
    REQUIRE(test::status_orig == 0);
    test::test_iwrite(info.write_data.c_str(), args.request_size, MPI_CHAR);
    REQUIRE((size_t)test::size_written_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
    REQUIRE(fs::file_size(info.shared_new_file) ==
            (size_t)test::size_written_orig);
  }

  SECTION("write to new  file using shared ptr") {
    test::test_open(info.shared_new_file.c_str(),
                    MPI_MODE_WRONLY | MPI_MODE_CREATE, MPI_COMM_WORLD);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_seek_shared(0, MPI_SEEK_SET);
    REQUIRE(test::status_orig == 0);
    test::test_iwrite_shared(info.write_data.c_str(), args.request_size,
                             MPI_CHAR);
    REQUIRE((size_t)test::size_written_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
    REQUIRE(fs::file_size(info.shared_new_file) ==
            (size_t)test::size_written_orig * info.comm_size);
  }

  SECTION("write to new file with allocate") {
    test::test_open(info.shared_new_file.c_str(),
                    MPI_MODE_WRONLY | MPI_MODE_CREATE, MPI_COMM_WORLD);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_preallocate(args.request_size * info.comm_size);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_seek(args.request_size * info.rank, MPI_SEEK_SET);
    REQUIRE(test::status_orig == 0);
    test::test_iwrite(info.write_data.c_str(), args.request_size, MPI_CHAR);
    REQUIRE((size_t)test::size_written_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
    MPI_Barrier(MPI_COMM_WORLD);
    REQUIRE(fs::file_size(info.shared_new_file) ==
            (size_t)test::size_written_orig * info.comm_size);
  }

  SECTION("write_at_all to existing file") {
    test::test_open(info.existing_file.c_str(), MPI_MODE_RDWR, MPI_COMM_SELF);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_iwrite_at_all(info.write_data.c_str(), args.request_size,
                             MPI_CHAR, info.rank * args.request_size);
    REQUIRE((size_t)test::size_written_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
  }

  SECTION("write_at_all to new file") {
    test::test_open(info.new_file.c_str(), MPI_MODE_WRONLY | MPI_MODE_CREATE,
                    MPI_COMM_SELF);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_iwrite_at_all(info.write_data.c_str(), args.request_size,
                             MPI_CHAR, 0);
    REQUIRE((size_t)test::size_written_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
    REQUIRE(fs::file_size(info.new_file) == (size_t)test::size_written_orig);
  }
  SECTION("append to existing file") {
    auto existing_size = fs::file_size(info.existing_file);
    test::test_open(info.shared_existing_file.c_str(),
                    MPI_MODE_WRONLY | MPI_MODE_APPEND | MPI_MODE_EXCL,
                    MPI_COMM_WORLD);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_iwrite_all(info.write_data.c_str(), args.request_size, MPI_CHAR);
    REQUIRE((size_t)test::size_written_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
    REQUIRE(fs::file_size(info.shared_existing_file) ==
            existing_size + test::size_written_orig);
  }

  SECTION("append to new file") {
    test::test_open(info.shared_new_file.c_str(),
                    MPI_MODE_WRONLY | MPI_MODE_CREATE, MPI_COMM_WORLD);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_iwrite_all(info.write_data.c_str(), args.request_size, MPI_CHAR);
    REQUIRE((size_t)test::size_written_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
    REQUIRE(fs::file_size(info.shared_new_file) ==
            (size_t)test::size_written_orig);
  }
  SECTION("delete on close mode on new file") {
    test::test_open(
        info.shared_new_file.c_str(),
        MPI_MODE_WRONLY | MPI_MODE_CREATE | MPI_MODE_DELETE_ON_CLOSE,
        MPI_COMM_WORLD);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_seek(0, MPI_SEEK_SET);
    REQUIRE(test::status_orig == 0);
    test::test_iwrite(info.write_data.c_str(), args.request_size, MPI_CHAR);
    REQUIRE((size_t)test::size_written_orig == args.request_size);
    REQUIRE(fs::exists(info.shared_new_file.c_str()));
    REQUIRE(fs::file_size(info.shared_new_file) ==
            (size_t)test::size_written_orig);
    test::test_close();
    REQUIRE(!fs::exists(info.shared_new_file.c_str()));
    REQUIRE(test::status_orig == MPI_SUCCESS);
  }

  SECTION("delete on close mode on existing file") {
    auto original_size = fs::file_size(info.shared_existing_file);
    test::test_open(info.shared_existing_file.c_str(),
                    MPI_MODE_WRONLY | MPI_MODE_EXCL | MPI_MODE_DELETE_ON_CLOSE,
                    MPI_COMM_WORLD);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_seek(info.rank * args.request_size, MPI_SEEK_SET);
    REQUIRE(test::status_orig == 0);
    test::test_iwrite(info.write_data.c_str(), args.request_size, MPI_CHAR);
    REQUIRE((size_t)test::size_written_orig == args.request_size);
    REQUIRE(fs::exists(info.shared_existing_file.c_str()));
    auto new_size =
        original_size > (size_t)test::size_written_orig * info.comm_size
            ? original_size
            : test::size_written_orig * info.comm_size;
    REQUIRE(fs::file_size(info.shared_existing_file) == (size_t)new_size);
    test::test_close();
    REQUIRE(!fs::exists(info.shared_existing_file.c_str()));
    REQUIRE(test::status_orig == MPI_SUCCESS);
    check_bytes = false;
  }
  posttest(check_bytes);
}

TEST_CASE("SingleRead", "[process=" + std::to_string(info.comm_size) +
                            "]"
                            "[operation=single_read]"
                            "[synchronicity=sync]"
                            "[coordination=independent]"
                            "[request_size=type-fixed][repetition=1]"
                            "[file=1]") {
  pretest();
  SECTION("read from non-existing file") {
    test::test_open(info.new_file.c_str(), MPI_MODE_RDONLY | MPI_MODE_EXCL,
                    MPI_COMM_SELF);
    REQUIRE(test::status_orig != MPI_SUCCESS);
  }

  SECTION("read from existing file") {
    test::test_open(info.existing_file.c_str(), MPI_MODE_RDONLY, MPI_COMM_SELF);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_seek(info.rank * args.request_size, MPI_SEEK_SET);
    REQUIRE(test::status_orig == 0);
    test::test_read(info.read_data.data(), args.request_size, MPI_CHAR);
    REQUIRE((size_t)test::size_read_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
  }

  SECTION("read from existing file using shared ptr") {
    test::test_open(info.shared_existing_file.c_str(), MPI_MODE_RDONLY,
                    MPI_COMM_SELF);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_seek_shared(0, MPI_SEEK_SET);
    REQUIRE(test::status_orig == 0);
    test::test_read_shared(info.read_data.data(), args.request_size, MPI_CHAR);
    REQUIRE((size_t)test::size_read_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
  }

  SECTION("read at the end of existing file") {
    test::test_open(info.existing_file.c_str(), MPI_MODE_RDONLY, MPI_COMM_SELF);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_seek(0, MPI_SEEK_END);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    MPI_Offset offset;
    MPI_File_get_position(test::fh_orig, &offset);
    REQUIRE(offset == (long long)(args.request_size * info.num_iterations));
    test::test_read(info.read_data.data(), args.request_size, MPI_CHAR);
    REQUIRE(test::size_read_orig == 0);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
  }

  SECTION("read_at from existing file") {
    test::test_open(info.existing_file.c_str(), MPI_MODE_RDONLY, MPI_COMM_SELF);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_read_at(info.read_data.data(), args.request_size, MPI_CHAR,
                       info.rank * args.request_size);
    REQUIRE((size_t)test::size_read_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
  }

  posttest();
}

TEST_CASE("SingleReadCollective", "[process=" + std::to_string(info.comm_size) +
                                      "]"
                                      "[operation=single_read]"
                                      "[synchronicity=sync]"
                                      "[coordination=collective]"
                                      "[request_size=type-fixed][repetition=1]"
                                      "[file=1]") {
  pretest();
  SECTION("read from non-existing file") {
    test::test_open(info.shared_new_file.c_str(), MPI_MODE_RDONLY,
                    MPI_COMM_WORLD);
    REQUIRE(test::status_orig != MPI_SUCCESS);
  }

  SECTION("read from existing file") {
    test::test_open(info.shared_existing_file.c_str(), MPI_MODE_RDONLY,
                    MPI_COMM_WORLD);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_seek(info.rank * args.request_size, MPI_SEEK_SET);
    REQUIRE(test::status_orig == 0);
    test::test_read_all(info.read_data.data(), args.request_size, MPI_CHAR);
    REQUIRE((size_t)test::size_read_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
  }

  SECTION("read from existing file using shared ptr") {
    test::test_open(info.shared_existing_file.c_str(), MPI_MODE_RDONLY,
                    MPI_COMM_WORLD);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_seek_shared(0, MPI_SEEK_SET);
    REQUIRE(test::status_orig == 0);
    test::test_read_shared(info.read_data.data(), args.request_size, MPI_CHAR);
    REQUIRE((size_t)test::size_read_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
  }

  SECTION("read at the end of existing file") {
    test::test_open(info.shared_existing_file.c_str(), MPI_MODE_RDONLY,
                    MPI_COMM_WORLD);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_seek(0, MPI_SEEK_END);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    MPI_Offset offset;
    MPI_File_get_position(test::fh_orig, &offset);
    REQUIRE(offset == (long long)(args.request_size * info.num_iterations));
    test::test_read_all(info.read_data.data(), args.request_size, MPI_CHAR);
    REQUIRE((size_t)test::size_read_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
  }

  SECTION("read_at_all from existing file") {
    test::test_open(info.existing_file.c_str(), MPI_MODE_RDONLY, MPI_COMM_SELF);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_read_at_all(info.read_data.data(), args.request_size, MPI_CHAR,
                           info.rank * args.request_size);
    REQUIRE((size_t)test::size_read_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
  }

  SECTION("read_ordered from existing file") {
    test::test_open(info.existing_file.c_str(), MPI_MODE_RDONLY,
                    MPI_COMM_WORLD);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_read_ordered(info.read_data.data(), args.request_size, MPI_CHAR);
    REQUIRE((size_t)test::size_read_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
  }
  posttest();
}

TEST_CASE("SingleAsyncRead", "[process=" + std::to_string(info.comm_size) +
                                 "]"
                                 "[operation=single_read]"
                                 "[synchronicity=async]"
                                 "[coordination=independent]"
                                 "[request_size=type-fixed][repetition=1]"
                                 "[file=1]") {
  pretest();
  SECTION("read from non-existing file") {
    test::test_open(info.new_file.c_str(), MPI_MODE_RDONLY | MPI_MODE_EXCL,
                    MPI_COMM_SELF);
    REQUIRE(test::status_orig != MPI_SUCCESS);
  }

  SECTION("read from existing file") {
    test::test_open(info.existing_file.c_str(), MPI_MODE_RDONLY, MPI_COMM_SELF);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_seek(info.rank * args.request_size, MPI_SEEK_SET);
    REQUIRE(test::status_orig == 0);
    test::test_iread(info.read_data.data(), args.request_size, MPI_CHAR);
    REQUIRE((size_t)test::size_read_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
  }

  SECTION("read from existing file using shared ptr") {
    test::test_open(info.shared_existing_file.c_str(), MPI_MODE_RDONLY,
                    MPI_COMM_SELF);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_seek_shared(0, MPI_SEEK_SET);
    REQUIRE(test::status_orig == 0);
    test::test_iread_shared(info.read_data.data(), args.request_size, MPI_CHAR);
    REQUIRE((size_t)test::size_read_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
  }

  SECTION("read at the end of existing file") {
    test::test_open(info.existing_file.c_str(), MPI_MODE_RDONLY, MPI_COMM_SELF);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_seek(0, MPI_SEEK_END);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    MPI_Offset offset;
    MPI_File_get_position(test::fh_orig, &offset);
    REQUIRE(offset == (long long)(args.request_size * info.num_iterations));
    test::test_iread(info.read_data.data(), args.request_size, MPI_CHAR);
    REQUIRE(test::size_read_orig == 0);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
  }

  SECTION("read_at from existing file") {
    test::test_open(info.existing_file.c_str(), MPI_MODE_RDONLY, MPI_COMM_SELF);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_iread_at(info.read_data.data(), args.request_size, MPI_CHAR,
                        info.rank * args.request_size);
    REQUIRE((size_t)test::size_read_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
  }

  posttest();
}

TEST_CASE("SingleAsyncReadCollective",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=single_read]"
              "[synchronicity=async]"
              "[coordination=collective]"
              "[request_size=type-fixed][repetition=1]"
              "[file=1]") {
  pretest();
  SECTION("read from non-existing file") {
    test::test_open(info.shared_new_file.c_str(), MPI_MODE_RDONLY,
                    MPI_COMM_WORLD);
    REQUIRE(test::status_orig != MPI_SUCCESS);
  }

  SECTION("read from existing file") {
    test::test_open(info.shared_existing_file.c_str(), MPI_MODE_RDONLY,
                    MPI_COMM_WORLD);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_seek(info.rank * args.request_size, MPI_SEEK_SET);
    REQUIRE(test::status_orig == 0);
    test::test_iread_all(info.read_data.data(), args.request_size, MPI_CHAR);
    REQUIRE((size_t)test::size_read_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
  }

  SECTION("read from existing file using shared ptr") {
    test::test_open(info.shared_existing_file.c_str(), MPI_MODE_RDONLY,
                    MPI_COMM_WORLD);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_seek_shared(0, MPI_SEEK_SET);
    REQUIRE(test::status_orig == 0);
    test::test_iread_shared(info.read_data.data(), args.request_size, MPI_CHAR);
    REQUIRE((size_t)test::size_read_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
  }

  SECTION("read at the end of existing file") {
    test::test_open(info.shared_existing_file.c_str(), MPI_MODE_RDONLY,
                    MPI_COMM_WORLD);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_seek(0, MPI_SEEK_END);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    MPI_Offset offset;
    MPI_File_get_position(test::fh_orig, &offset);
    REQUIRE(offset == (long long)(args.request_size * info.num_iterations));
    test::test_iread_all(info.read_data.data(), args.request_size, MPI_CHAR);
    REQUIRE((size_t)test::size_read_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
  }

  SECTION("read_at_all from existing file") {
    test::test_open(info.existing_file.c_str(), MPI_MODE_RDONLY, MPI_COMM_SELF);
    REQUIRE(test::status_orig == MPI_SUCCESS);
    test::test_iread_at_all(info.read_data.data(), args.request_size, MPI_CHAR,
                            info.rank * args.request_size);
    REQUIRE((size_t)test::size_read_orig == args.request_size);
    test::test_close();
    REQUIRE(test::status_orig == MPI_SUCCESS);
  }
  posttest();
}
