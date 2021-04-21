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
                      "[operation=single_open]"
                      "[repetition=1][file=1]") {
  pretest();
  SECTION("open non-existant file") {
    test::test_open(info.new_file.c_str(), MPI_MODE_RDONLY, MPI_COMM_SELF);
    REQUIRE(test::fh_orig == nullptr);
    test::test_open(info.new_file.c_str(), MPI_MODE_RDWR | MPI_MODE_EXCL,
                    MPI_COMM_SELF);
    REQUIRE(test::fh_orig == nullptr);
  }

  SECTION("truncate existing file and write-only") {
    test::test_open(info.existing_file.c_str(), MPI_MODE_WRONLY | MPI_MODE_EXCL,
                    MPI_COMM_SELF);
    REQUIRE(test::fh_orig != nullptr);
    test::test_close();
    REQUIRE(test::status_orig == 0);
  }
  SECTION("truncate existing file and read/write") {
    test::test_open(info.existing_file.c_str(), MPI_MODE_RDWR | MPI_MODE_CREATE,
                    MPI_COMM_SELF);
    REQUIRE(test::fh_orig != nullptr);
    test::test_close();
    REQUIRE(test::status_orig == 0);
  }

  SECTION("open existing file") {
    test::test_open(info.existing_file.c_str(), MPI_MODE_RDWR | MPI_MODE_EXCL,
                    MPI_COMM_SELF);
    REQUIRE(test::fh_orig != nullptr);
    test::test_close();
    REQUIRE(test::status_orig == 0);
    test::test_open(info.existing_file.c_str(), MPI_MODE_RDONLY | MPI_MODE_EXCL,
                    MPI_COMM_SELF);
    REQUIRE(test::fh_orig != nullptr);
    test::test_close();
    REQUIRE(test::status_orig == 0);
  }

  SECTION("append write existing file") {
    test::test_open(info.existing_file.c_str(), MPI_MODE_APPEND | MPI_MODE_EXCL,
                    MPI_COMM_SELF);
    REQUIRE(test::fh_orig != nullptr);
    test::test_close();
    REQUIRE(test::status_orig == 0);
  }

  SECTION("append write and read existing file") {
    test::test_open(info.existing_file.c_str(),
                    MPI_MODE_APPEND | MPI_MODE_CREATE, MPI_COMM_SELF);
    REQUIRE(test::fh_orig != nullptr);
    test::test_close();
    REQUIRE(test::status_orig == 0);
  }

  SECTION("delete on close mode") {
    test::test_open(
        info.new_file.c_str(),
        MPI_MODE_WRONLY | MPI_MODE_CREATE | MPI_MODE_DELETE_ON_CLOSE,
        MPI_COMM_SELF);
    REQUIRE(test::fh_orig != nullptr);
    REQUIRE(fs::exists(info.new_file.c_str()));
    test::test_close();
    REQUIRE(!fs::exists(info.new_file.c_str()));
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("OpenCollective", "[process=" + std::to_string(info.comm_size) +
                  "]"
                  "[coordination=collective]"
                  "[operation=single_open]"
                  "[repetition=1][file=1]") {
  pretest();
  SECTION("open on non-existant shared file") {
    test::test_open(info.shared_new_file.c_str(), MPI_MODE_RDONLY, MPI_COMM_WORLD);
    REQUIRE(test::fh_orig == nullptr);
    test::test_open(info.shared_new_file.c_str(), MPI_MODE_RDWR | MPI_MODE_EXCL,
                    MPI_COMM_SELF);
    REQUIRE(test::fh_orig == nullptr);
  }

  SECTION("truncate on shared existing file and write-only") {
    test::test_open(info.shared_existing_file.c_str(), MPI_MODE_WRONLY | MPI_MODE_EXCL,
                    MPI_COMM_SELF);
    REQUIRE(test::fh_orig != nullptr);
    test::test_close();
    REQUIRE(test::status_orig == 0);
  }
  SECTION("truncate on shared existing file and read/write") {
    test::test_open(info.shared_existing_file.c_str(), MPI_MODE_RDWR | MPI_MODE_CREATE,
                    MPI_COMM_SELF);
    REQUIRE(test::fh_orig != nullptr);
    test::test_close();
    REQUIRE(test::status_orig == 0);
  }

  SECTION("open on shared existing file") {
    test::test_open(info.shared_existing_file.c_str(), MPI_MODE_RDWR | MPI_MODE_EXCL,
                    MPI_COMM_SELF);
    REQUIRE(test::fh_orig != nullptr);
    test::test_close();
    REQUIRE(test::status_orig == 0);
    test::test_open(info.shared_existing_file.c_str(), MPI_MODE_RDONLY | MPI_MODE_EXCL,
                    MPI_COMM_SELF);
    REQUIRE(test::fh_orig != nullptr);
    test::test_close();
    REQUIRE(test::status_orig == 0);
  }

  SECTION("append write on shared existing file") {
    test::test_open(info.shared_existing_file.c_str(), MPI_MODE_APPEND | MPI_MODE_EXCL,
                    MPI_COMM_SELF);
    REQUIRE(test::fh_orig != nullptr);
    test::test_close();
    REQUIRE(test::status_orig == 0);
  }

  SECTION("append write and read on shared existing file") {
    test::test_open(info.shared_existing_file.c_str(),
                    MPI_MODE_APPEND | MPI_MODE_CREATE, MPI_COMM_SELF);
    REQUIRE(test::fh_orig != nullptr);
    test::test_close();
    REQUIRE(test::status_orig == 0);
  }

  SECTION("delete on close mode on shared file") {
    test::test_open(
        info.new_file.c_str(),
        MPI_MODE_WRONLY | MPI_MODE_CREATE | MPI_MODE_DELETE_ON_CLOSE,
        MPI_COMM_SELF);
    REQUIRE(test::fh_orig != nullptr);
    REQUIRE(fs::exists(info.new_file.c_str()));
    test::test_close();
    REQUIRE(!fs::exists(info.new_file.c_str()));
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}
