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

#include "mpiio_adapter_test.h"

TEST_CASE("Open", "[process=" + std::to_string(TESTER->comm_size_) +
    "]"
    "[coordination=independent]"
    "[synchronicity=sync]"
    "[operation=single_open]"
    "[repetition=1][file=1]") {
  TESTER->Pretest();
  SECTION("open non-existant file") {
    TESTER->test_open(TESTER->new_file_, MPI_MODE_RDONLY, MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ != MPI_SUCCESS);
    TESTER->test_open(TESTER->new_file_, MPI_MODE_RDWR | MPI_MODE_EXCL,
                      MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ != MPI_SUCCESS);
  }

  SECTION("opening existing file write-only") {
    TESTER->test_open(TESTER->existing_file_, MPI_MODE_WRONLY | MPI_MODE_EXCL,
                      MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
  }
  SECTION("opening existing file and read/write") {
    TESTER->test_open(TESTER->existing_file_, MPI_MODE_RDWR | MPI_MODE_CREATE,
                      MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
  }

  SECTION("open existing file") {
    TESTER->test_open(TESTER->existing_file_, MPI_MODE_RDONLY | MPI_MODE_EXCL,
                      MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ != MPI_SUCCESS);
    TESTER->test_open(TESTER->existing_file_, MPI_MODE_RDWR, MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_open(TESTER->existing_file_, MPI_MODE_RDONLY, MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
  }

  SECTION("append write existing file") {
    TESTER->test_open(TESTER->existing_file_, MPI_MODE_APPEND | MPI_MODE_EXCL,
                      MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ != MPI_SUCCESS);
    TESTER->test_open(TESTER->existing_file_, MPI_MODE_APPEND, MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ != MPI_SUCCESS);
    TESTER->test_open(TESTER->existing_file_,
                      MPI_MODE_WRONLY | MPI_MODE_APPEND, MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_open(TESTER->existing_file_,
                      MPI_MODE_RDONLY | MPI_MODE_APPEND, MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_open(TESTER->existing_file_, MPI_MODE_RDWR | MPI_MODE_APPEND,
                      MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
  }

  SECTION("append write and read new file") {
    TESTER->test_open(TESTER->existing_file_,
                      MPI_MODE_RDWR | MPI_MODE_APPEND | MPI_MODE_CREATE,
                      MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
  }

  SECTION("delete on close mode") {
    TESTER->test_open(
        TESTER->new_file_,
        MPI_MODE_WRONLY | MPI_MODE_CREATE | MPI_MODE_DELETE_ON_CLOSE,
        MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    REQUIRE(stdfs::exists(TESTER->new_file_.hermes_));
    TESTER->test_close();
    REQUIRE(!stdfs::exists(TESTER->new_file_.hermes_));
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
  }
  TESTER->Posttest();
}

TEST_CASE("OpenCollective", "[process=" + std::to_string(TESTER->comm_size_) +
    "]"
    "[coordination=collective]"
    "[synchronicity=sync]"
    "[operation=single_open]"
    "[repetition=1][file=1]") {
  TESTER->Pretest();
  SECTION("open on non-existant shared file") {
    TESTER->test_open(TESTER->shared_new_file_,
                      MPI_MODE_RDONLY | MPI_MODE_EXCL, MPI_COMM_WORLD);
    REQUIRE(TESTER->status_orig_ != MPI_SUCCESS);
    TESTER->test_open(TESTER->shared_new_file_,
                      MPI_MODE_RDONLY | MPI_MODE_CREATE, MPI_COMM_WORLD);
    REQUIRE(TESTER->status_orig_ != MPI_SUCCESS);
    TESTER->test_open(TESTER->shared_new_file_, MPI_MODE_RDWR | MPI_MODE_EXCL,
                      MPI_COMM_WORLD);
    REQUIRE(TESTER->status_orig_ != MPI_SUCCESS);
    TESTER->test_open(TESTER->shared_new_file_,
                      MPI_MODE_WRONLY | MPI_MODE_CREATE, MPI_COMM_WORLD);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_open(TESTER->shared_new_file_,
                      MPI_MODE_RDWR | MPI_MODE_CREATE, MPI_COMM_WORLD);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
  }

  SECTION("open on shared existing file") {
    TESTER->test_open(TESTER->shared_existing_file_,
                      MPI_MODE_WRONLY | MPI_MODE_EXCL, MPI_COMM_WORLD);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_open(TESTER->shared_existing_file_,
                      MPI_MODE_RDWR | MPI_MODE_EXCL, MPI_COMM_WORLD);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_open(TESTER->shared_existing_file_,
                      MPI_MODE_RDONLY | MPI_MODE_EXCL, MPI_COMM_WORLD);
    REQUIRE(TESTER->status_orig_ != MPI_SUCCESS);
    TESTER->test_open(TESTER->shared_existing_file_, MPI_MODE_RDONLY,
                      MPI_COMM_WORLD);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_open(TESTER->shared_existing_file_, MPI_MODE_RDWR,
                      MPI_COMM_WORLD);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
  }

  SECTION("append write/read on shared existing file") {
    TESTER->test_open(TESTER->shared_existing_file_,
                      MPI_MODE_RDWR | MPI_MODE_APPEND, MPI_COMM_WORLD);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
  }

  SECTION("append write and read on shared new file") {
    TESTER->test_open(TESTER->shared_new_file_,
                      MPI_MODE_RDWR | MPI_MODE_APPEND | MPI_MODE_CREATE,
                      MPI_COMM_WORLD);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
  }

  SECTION("delete on close mode on new file") {
    TESTER->test_open(
        TESTER->shared_new_file_,
        MPI_MODE_WRONLY | MPI_MODE_CREATE | MPI_MODE_DELETE_ON_CLOSE,
        MPI_COMM_WORLD);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    REQUIRE(!stdfs::exists(TESTER->shared_new_file_.hermes_));
  }
  TESTER->Posttest();
}

TEST_CASE("SingleWrite", "[process=" + std::to_string(TESTER->comm_size_) +
    "]"
    "[operation=single_write]"
    "[synchronicity=sync]"
    "[coordination=independent]"
    "[request_size=type-fixed][repetition=1]"
    "[file=1]") {
  TESTER->Pretest();
  bool check_bytes = true;
  SECTION("write to existing file") {
    TESTER->test_open(TESTER->existing_file_, MPI_MODE_RDWR, MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_seek(TESTER->rank_ * TESTER->request_size_, MPI_SEEK_SET);
    REQUIRE(TESTER->status_orig_ == 0);
    TESTER->test_write(TESTER->write_data_.data(),
                       TESTER->request_size_, MPI_CHAR);
    REQUIRE((size_t)TESTER->size_written_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
  }

  SECTION("write to new  file") {
    TESTER->test_open(TESTER->new_file_, MPI_MODE_WRONLY | MPI_MODE_CREATE,
                      MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_write(TESTER->write_data_.data(),
                       TESTER->request_size_, MPI_CHAR);
    REQUIRE((size_t)TESTER->size_written_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    REQUIRE(stdfs::file_size(TESTER->new_file_.hermes_) ==
        (size_t)TESTER->size_written_orig_);
  }

  SECTION("write to new file with allocate") {
    TESTER->test_open(TESTER->new_file_,
                      MPI_MODE_WRONLY | MPI_MODE_CREATE, MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_preallocate(TESTER->request_size_ * TESTER->comm_size_);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_seek(TESTER->request_size_ * TESTER->rank_, MPI_SEEK_SET);
    REQUIRE(TESTER->status_orig_ == 0);
    TESTER->test_write(TESTER->write_data_.data(),
                       TESTER->request_size_, MPI_CHAR);
    REQUIRE((size_t)TESTER->size_written_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    MPI_Barrier(MPI_COMM_WORLD);
    REQUIRE(stdfs::file_size(TESTER->new_file_.hermes_) ==
        (size_t)TESTER->size_written_orig_ * TESTER->comm_size_);
  }

  SECTION("append to existing file") {
    auto existing_size = stdfs::file_size(TESTER->existing_file_.hermes_);
    TESTER->test_open(TESTER->existing_file_,
                      MPI_MODE_WRONLY | MPI_MODE_APPEND | MPI_MODE_EXCL,
                      MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_write(TESTER->write_data_.data(),
                       TESTER->request_size_, MPI_CHAR);
    REQUIRE((size_t)TESTER->size_written_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    REQUIRE(stdfs::file_size(TESTER->existing_file_.hermes_) ==
        existing_size + TESTER->size_written_orig_);
  }

  SECTION("append to new file") {
    TESTER->test_open(TESTER->new_file_,
                      MPI_MODE_WRONLY | MPI_MODE_APPEND | MPI_MODE_CREATE,
                      MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_write(TESTER->write_data_.data(),
                       TESTER->request_size_, MPI_CHAR);
    REQUIRE((size_t)TESTER->size_written_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    REQUIRE(stdfs::file_size(TESTER->new_file_.hermes_) ==
        (size_t)TESTER->size_written_orig_);
  }

  SECTION("write_at to existing file") {
    TESTER->test_open(TESTER->existing_file_, MPI_MODE_RDWR, MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_write_at(TESTER->write_data_.data(),
                          TESTER->request_size_, MPI_CHAR,
                          TESTER->rank_ * TESTER->request_size_);
    REQUIRE((size_t)TESTER->size_written_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
  }

  SECTION("write_at to new file") {
    TESTER->test_open(TESTER->new_file_, MPI_MODE_WRONLY | MPI_MODE_CREATE,
                      MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_write_at(TESTER->write_data_.data(),
                          TESTER->request_size_, MPI_CHAR,
                          0);
    REQUIRE((size_t)TESTER->size_written_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    REQUIRE(stdfs::file_size(TESTER->new_file_.hermes_) ==
        (size_t)TESTER->size_written_orig_);
  }

  SECTION("delete on close mode on new file") {
    TESTER->test_open(
        TESTER->new_file_,
        MPI_MODE_WRONLY | MPI_MODE_CREATE | MPI_MODE_DELETE_ON_CLOSE,
        MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_seek(0, MPI_SEEK_SET);
    REQUIRE(TESTER->status_orig_ == 0);
    TESTER->test_write(TESTER->write_data_.data(),
                       TESTER->request_size_, MPI_CHAR);
    REQUIRE((size_t)TESTER->size_written_orig_ == TESTER->request_size_);
    REQUIRE(stdfs::exists(TESTER->new_file_.hermes_));
    TESTER->test_close();
    REQUIRE(!stdfs::exists(TESTER->new_file_.hermes_));
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
  }

  SECTION("delete on close mode on existing file") {
    auto original_size = stdfs::file_size(TESTER->existing_file_.hermes_);
    TESTER->test_open(TESTER->existing_file_,
                      MPI_MODE_WRONLY | MPI_MODE_EXCL |
                          MPI_MODE_DELETE_ON_CLOSE, MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_seek(TESTER->rank_ * TESTER->request_size_, MPI_SEEK_SET);
    REQUIRE(TESTER->status_orig_ == 0);
    TESTER->test_write(TESTER->write_data_.data(),
                       TESTER->request_size_, MPI_CHAR);
    REQUIRE((size_t)TESTER->size_written_orig_ == TESTER->request_size_);
    REQUIRE(stdfs::exists(TESTER->existing_file_.hermes_));
    auto new_size =
        original_size > (size_t)TESTER->size_written_orig_ * TESTER->comm_size_
        ? original_size
        : TESTER->size_written_orig_ * TESTER->comm_size_;
    REQUIRE(stdfs::file_size(TESTER->existing_file_.hermes_) ==
        (size_t)new_size);
    TESTER->test_close();
    REQUIRE(!stdfs::exists(TESTER->existing_file_.hermes_));
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    check_bytes = false;
  }
  TESTER->Posttest(check_bytes);
}

TEST_CASE("SingleWriteCollective",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=single_write]"
              "[synchronicity=sync]"
              "[coordination=collective]"
              "[request_size=type-fixed][repetition=1]"
              "[file=1]") {
  TESTER->Pretest();
  bool check_bytes = true;
  SECTION("write to existing file") {
    TESTER->test_open(TESTER->shared_existing_file_, MPI_MODE_RDWR,
                      MPI_COMM_WORLD);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_seek(TESTER->rank_ * TESTER->request_size_, MPI_SEEK_SET);
    REQUIRE(TESTER->status_orig_ == 0);
    TESTER->test_write(TESTER->write_data_.data(),
                       TESTER->request_size_, MPI_CHAR);
    REQUIRE((size_t)TESTER->size_written_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
  }

  SECTION("write to new  file") {
    TESTER->test_open(TESTER->shared_new_file_,
                      MPI_MODE_WRONLY | MPI_MODE_CREATE, MPI_COMM_WORLD);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_seek(0, MPI_SEEK_SET);
    REQUIRE(TESTER->status_orig_ == 0);
    TESTER->test_write(TESTER->write_data_.data(),
                       TESTER->request_size_, MPI_CHAR);
    REQUIRE((size_t)TESTER->size_written_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    REQUIRE(stdfs::file_size(TESTER->shared_new_file_.hermes_) ==
        (size_t)TESTER->size_written_orig_);
  }

    // TODO(chogan): This test fails intermittently. Needs diagnosis.
    // https://github.com/HDFGroup/hermes/issues/209
  SECTION("write to new  file using shared ptr") {
    TESTER->test_open(TESTER->shared_new_file_,
                      MPI_MODE_WRONLY | MPI_MODE_CREATE, MPI_COMM_WORLD);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_seek_shared(0, MPI_SEEK_SET);
    REQUIRE(TESTER->status_orig_ == 0);
    TESTER->test_write_shared(TESTER->write_data_.data(),
                              TESTER->request_size_, MPI_CHAR);
    REQUIRE((size_t)TESTER->size_written_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    REQUIRE(stdfs::file_size(TESTER->shared_new_file_.hermes_) ==
        (size_t)TESTER->size_written_orig_ * TESTER->comm_size_);
  }

  SECTION("write to new file with allocate") {
    TESTER->test_open(TESTER->shared_new_file_,
                      MPI_MODE_WRONLY | MPI_MODE_CREATE, MPI_COMM_WORLD);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_preallocate(TESTER->request_size_ * TESTER->comm_size_);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_seek(TESTER->request_size_ * TESTER->rank_, MPI_SEEK_SET);
    REQUIRE(TESTER->status_orig_ == 0);
    TESTER->test_write(TESTER->write_data_.data(),
                       TESTER->request_size_, MPI_CHAR);
    REQUIRE((size_t)TESTER->size_written_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    MPI_Barrier(MPI_COMM_WORLD);
    REQUIRE(stdfs::file_size(TESTER->shared_new_file_.hermes_) ==
        (size_t)TESTER->size_written_orig_ * TESTER->comm_size_);
  }

  SECTION("write_at_all to existing file") {
    TESTER->test_open(TESTER->existing_file_, MPI_MODE_RDWR, MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_write_at_all(TESTER->write_data_.data(), TESTER->request_size_,
                              MPI_CHAR, TESTER->rank_ * TESTER->request_size_);
    REQUIRE((size_t)TESTER->size_written_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
  }

  SECTION("write_at_all to new file") {
    TESTER->test_open(TESTER->new_file_, MPI_MODE_WRONLY | MPI_MODE_CREATE,
                      MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_write_at_all(TESTER->write_data_.data(),
                              TESTER->request_size_,
                              MPI_CHAR, 0);
    REQUIRE((size_t)TESTER->size_written_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    REQUIRE(stdfs::file_size(TESTER->new_file_.hermes_) ==
        (size_t)TESTER->size_written_orig_);
  }

  SECTION("append to existing file") {
    auto existing_size = stdfs::file_size(TESTER->existing_file_.hermes_);
    TESTER->test_open(TESTER->shared_existing_file_,
                      MPI_MODE_WRONLY | MPI_MODE_APPEND | MPI_MODE_EXCL,
                      MPI_COMM_WORLD);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_write_all(TESTER->write_data_.data(),
                           TESTER->request_size_, MPI_CHAR);
    REQUIRE((size_t)TESTER->size_written_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    REQUIRE(stdfs::file_size(TESTER->shared_existing_file_.hermes_) ==
        existing_size + TESTER->size_written_orig_);
  }

  SECTION("append to new file") {
    TESTER->test_open(TESTER->shared_new_file_,
                      MPI_MODE_WRONLY | MPI_MODE_CREATE, MPI_COMM_WORLD);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_write_all(TESTER->write_data_.data(),
                           TESTER->request_size_, MPI_CHAR);
    REQUIRE((size_t)TESTER->size_written_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    REQUIRE(stdfs::file_size(TESTER->shared_new_file_.hermes_) ==
        (size_t)TESTER->size_written_orig_);
  }

  SECTION("write_ordered to existing file") {
    TESTER->test_open(TESTER->existing_file_, MPI_MODE_RDWR, MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_write_ordered(TESTER->write_data_.data(),
                               TESTER->request_size_, MPI_CHAR);
    REQUIRE((size_t)TESTER->size_written_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
  }

  SECTION("write_ordered to new file") {
    TESTER->test_open(TESTER->new_file_, MPI_MODE_WRONLY | MPI_MODE_CREATE,
                      MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_write_ordered(TESTER->write_data_.data(),
                               TESTER->request_size_, MPI_CHAR);
    REQUIRE((size_t)TESTER->size_written_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    REQUIRE(stdfs::file_size(TESTER->new_file_.hermes_) ==
        (size_t)TESTER->size_written_orig_);
  }
  SECTION("delete on close mode on new file") {
    TESTER->test_open(
        TESTER->shared_new_file_,
        MPI_MODE_WRONLY | MPI_MODE_CREATE | MPI_MODE_DELETE_ON_CLOSE,
        MPI_COMM_WORLD);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_seek(TESTER->rank_ * TESTER->request_size_, MPI_SEEK_SET);
    REQUIRE(TESTER->status_orig_ == 0);
    TESTER->test_write(TESTER->write_data_.data(),
                       TESTER->request_size_, MPI_CHAR);
    REQUIRE((size_t)TESTER->size_written_orig_ == TESTER->request_size_);
    REQUIRE(stdfs::exists(TESTER->shared_new_file_.hermes_));
    MPI_Barrier(MPI_COMM_WORLD);
    TESTER->test_close();
    REQUIRE(!stdfs::exists(TESTER->shared_new_file_.hermes_));
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
  }

  SECTION("delete on close mode on existing file") {
    auto original_size = stdfs::file_size(
        TESTER->shared_existing_file_.hermes_);
    TESTER->test_open(TESTER->shared_existing_file_,
                      MPI_MODE_WRONLY | MPI_MODE_EXCL |
                          MPI_MODE_DELETE_ON_CLOSE, MPI_COMM_WORLD);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_seek(TESTER->rank_ * TESTER->request_size_, MPI_SEEK_SET);
    REQUIRE(TESTER->status_orig_ == 0);
    TESTER->test_write(TESTER->write_data_.data(),
                       TESTER->request_size_, MPI_CHAR);
    REQUIRE((size_t)TESTER->size_written_orig_ == TESTER->request_size_);
    REQUIRE(stdfs::exists(TESTER->shared_existing_file_.hermes_));
    auto new_size =
        original_size > (size_t)TESTER->size_written_orig_ * TESTER->comm_size_
        ? original_size
        : TESTER->size_written_orig_ * TESTER->comm_size_;
    REQUIRE(stdfs::file_size(TESTER->shared_existing_file_.hermes_) ==
        (size_t)new_size);
    TESTER->test_close();
    REQUIRE(!stdfs::exists(TESTER->shared_existing_file_.hermes_));
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    check_bytes = false;
  }
  TESTER->Posttest(check_bytes);
}

TEST_CASE("SingleAsyncWrite", "[process=" + std::to_string(TESTER->comm_size_) +
    "]"
    "[operation=single_write]"
    "[coordination=independent]"
    "[synchronicity=async]"
    "[request_size=type-fixed][repetition=1]"
    "[file=1]") {
  TESTER->Pretest();
  bool check_bytes = true;
  SECTION("write to existing file") {
    TESTER->test_open(TESTER->existing_file_, MPI_MODE_RDWR, MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_seek(TESTER->rank_ * TESTER->request_size_, MPI_SEEK_SET);
    REQUIRE(TESTER->status_orig_ == 0);
    TESTER->test_iwrite(TESTER->write_data_.data(),
                        TESTER->request_size_, MPI_CHAR);
    REQUIRE((size_t)TESTER->size_written_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
  }

  SECTION("write to new  file") {
    TESTER->test_open(TESTER->new_file_, MPI_MODE_WRONLY | MPI_MODE_CREATE,
                      MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_seek(0, MPI_SEEK_SET);
    REQUIRE(TESTER->status_orig_ == 0);
    TESTER->test_iwrite(TESTER->write_data_.data(),
                        TESTER->request_size_, MPI_CHAR);
    REQUIRE((size_t)TESTER->size_written_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    REQUIRE(stdfs::file_size(TESTER->new_file_.hermes_) ==
        (size_t)TESTER->size_written_orig_);
  }

  SECTION("write to new  file using shared ptr") {
    TESTER->test_open(TESTER->shared_new_file_,
                      MPI_MODE_WRONLY | MPI_MODE_CREATE, MPI_COMM_WORLD);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_seek_shared(0, MPI_SEEK_SET);
    REQUIRE(TESTER->status_orig_ == 0);
    TESTER->test_iwrite_shared(TESTER->write_data_.data(),
                               TESTER->request_size_, MPI_CHAR);
    REQUIRE((size_t)TESTER->size_written_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    REQUIRE(stdfs::file_size(TESTER->shared_new_file_.hermes_) ==
        (size_t)TESTER->size_written_orig_ * TESTER->comm_size_);
  }

  SECTION("write to new file with allocate") {
    TESTER->test_open(TESTER->shared_new_file_,
                      MPI_MODE_WRONLY | MPI_MODE_CREATE, MPI_COMM_WORLD);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_preallocate(TESTER->request_size_ * TESTER->comm_size_);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_seek(TESTER->request_size_ * TESTER->rank_, MPI_SEEK_SET);
    REQUIRE(TESTER->status_orig_ == 0);
    TESTER->test_write(TESTER->write_data_.data(),
                       TESTER->request_size_, MPI_CHAR);
    REQUIRE((size_t)TESTER->size_written_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    MPI_Barrier(MPI_COMM_WORLD);
    REQUIRE(stdfs::file_size(TESTER->shared_new_file_.hermes_) ==
        (size_t)TESTER->size_written_orig_ * TESTER->comm_size_);
  }

  SECTION("append to existing file") {
    auto existing_size = stdfs::file_size(TESTER->existing_file_.hermes_);
    TESTER->test_open(TESTER->existing_file_,
                      MPI_MODE_WRONLY | MPI_MODE_APPEND | MPI_MODE_EXCL,
                      MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_iwrite(TESTER->write_data_.data(),
                        TESTER->request_size_, MPI_CHAR);
    REQUIRE((size_t)TESTER->size_written_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    REQUIRE(stdfs::file_size(TESTER->existing_file_.hermes_) ==
        existing_size + TESTER->size_written_orig_);
  }

  SECTION("append to new file") {
    TESTER->test_open(TESTER->new_file_,
                      MPI_MODE_WRONLY | MPI_MODE_APPEND | MPI_MODE_CREATE,
                      MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_iwrite(TESTER->write_data_.data(),
                        TESTER->request_size_, MPI_CHAR);
    REQUIRE((size_t)TESTER->size_written_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    REQUIRE(stdfs::file_size(TESTER->new_file_.hermes_) ==
        (size_t)TESTER->size_written_orig_);
  }

  SECTION("write_at to existing file") {
    TESTER->test_open(TESTER->existing_file_, MPI_MODE_RDWR, MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_iwrite_at(TESTER->write_data_.data(),
                           TESTER->request_size_, MPI_CHAR,
                           TESTER->rank_ * TESTER->request_size_);
    REQUIRE((size_t)TESTER->size_written_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
  }

  SECTION("write_at to new file") {
    TESTER->test_open(TESTER->new_file_, MPI_MODE_WRONLY | MPI_MODE_CREATE,
                      MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_iwrite_at(TESTER->write_data_.data(),
                           TESTER->request_size_, MPI_CHAR,
                           0);
    REQUIRE((size_t)TESTER->size_written_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    REQUIRE(stdfs::file_size(TESTER->new_file_.hermes_) ==
        (size_t)TESTER->size_written_orig_);
  }

  SECTION("delete on close mode on new file") {
    TESTER->test_open(
        TESTER->new_file_,
        MPI_MODE_WRONLY | MPI_MODE_CREATE | MPI_MODE_DELETE_ON_CLOSE,
        MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_seek(0, MPI_SEEK_SET);
    REQUIRE(TESTER->status_orig_ == 0);
    TESTER->test_iwrite(TESTER->write_data_.data(),
                        TESTER->request_size_, MPI_CHAR);
    REQUIRE((size_t)TESTER->size_written_orig_ == TESTER->request_size_);
    REQUIRE(stdfs::exists(TESTER->new_file_.hermes_));
    TESTER->test_close();
    REQUIRE(!stdfs::exists(TESTER->new_file_.hermes_));
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
  }

  SECTION("delete on close mode on existing file") {
    TESTER->test_open(TESTER->existing_file_,
                      MPI_MODE_WRONLY | MPI_MODE_EXCL |
                          MPI_MODE_DELETE_ON_CLOSE,
                      MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_seek(TESTER->rank_ * TESTER->request_size_, MPI_SEEK_SET);
    REQUIRE(TESTER->status_orig_ == 0);
    TESTER->test_iwrite(TESTER->write_data_.data(),
                        TESTER->request_size_, MPI_CHAR);
    REQUIRE((size_t)TESTER->size_written_orig_ == TESTER->request_size_);
    REQUIRE(stdfs::exists(TESTER->existing_file_.hermes_));
    TESTER->test_close();
    REQUIRE(!stdfs::exists(TESTER->existing_file_.hermes_));
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    check_bytes = false;
  }
  TESTER->Posttest(check_bytes);
}

TEST_CASE("SingleAsyncWriteCollective",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=single_write]"
              "[synchronicity=async]"
              "[coordination=collective]"
              "[request_size=type-fixed][repetition=1]"
              "[file=1]") {
  TESTER->Pretest();
  bool check_bytes = true;
  SECTION("write to existing file") {
    TESTER->test_open(TESTER->shared_existing_file_, MPI_MODE_RDWR,
                      MPI_COMM_WORLD);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_seek(TESTER->rank_ * TESTER->request_size_, MPI_SEEK_SET);
    REQUIRE(TESTER->status_orig_ == 0);
    TESTER->test_iwrite(TESTER->write_data_.data(),
                        TESTER->request_size_, MPI_CHAR);
    REQUIRE((size_t)TESTER->size_written_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
  }

  SECTION("write to new  file") {
    TESTER->test_open(TESTER->shared_new_file_,
                      MPI_MODE_WRONLY | MPI_MODE_CREATE, MPI_COMM_WORLD);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_seek(0, MPI_SEEK_SET);
    REQUIRE(TESTER->status_orig_ == 0);
    TESTER->test_iwrite(TESTER->write_data_.data(),
                        TESTER->request_size_, MPI_CHAR);
    REQUIRE((size_t)TESTER->size_written_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    REQUIRE(stdfs::file_size(TESTER->shared_new_file_.hermes_) ==
        (size_t)TESTER->size_written_orig_);
  }

  SECTION("write to new  file using shared ptr") {
    TESTER->test_open(TESTER->shared_new_file_,
                      MPI_MODE_WRONLY | MPI_MODE_CREATE, MPI_COMM_WORLD);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_seek_shared(0, MPI_SEEK_SET);
    REQUIRE(TESTER->status_orig_ == 0);
    TESTER->test_iwrite_shared(TESTER->write_data_.data(),
                               TESTER->request_size_, MPI_CHAR);
    REQUIRE((size_t)TESTER->size_written_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    REQUIRE(stdfs::file_size(TESTER->shared_new_file_.hermes_) ==
        (size_t)TESTER->size_written_orig_ * TESTER->comm_size_);
  }

  SECTION("write to new file with allocate") {
    TESTER->test_open(TESTER->shared_new_file_,
                      MPI_MODE_WRONLY | MPI_MODE_CREATE, MPI_COMM_WORLD);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_preallocate(TESTER->request_size_ * TESTER->comm_size_);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_seek(TESTER->request_size_ * TESTER->rank_, MPI_SEEK_SET);
    REQUIRE(TESTER->status_orig_ == 0);
    TESTER->test_iwrite(TESTER->write_data_.data(),
                        TESTER->request_size_, MPI_CHAR);
    REQUIRE((size_t)TESTER->size_written_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    MPI_Barrier(MPI_COMM_WORLD);
    REQUIRE(stdfs::file_size(TESTER->shared_new_file_.hermes_) ==
        (size_t)TESTER->size_written_orig_ * TESTER->comm_size_);
  }

  SECTION("write_at_all to existing file") {
    TESTER->test_open(TESTER->existing_file_, MPI_MODE_RDWR, MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_iwrite_at_all(TESTER->write_data_.data(),
                               TESTER->request_size_,
                               MPI_CHAR, TESTER->rank_ * TESTER->request_size_);
    REQUIRE((size_t)TESTER->size_written_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
  }

  SECTION("write_at_all to new file") {
    TESTER->test_open(TESTER->new_file_, MPI_MODE_WRONLY | MPI_MODE_CREATE,
                      MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_iwrite_at_all(TESTER->write_data_.data(),
                               TESTER->request_size_, MPI_CHAR, 0);
    REQUIRE((size_t)TESTER->size_written_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    REQUIRE(stdfs::file_size(TESTER->new_file_.hermes_) ==
        (size_t)TESTER->size_written_orig_);
  }
  SECTION("append to existing file") {
    auto existing_size = stdfs::file_size(TESTER->existing_file_.hermes_);
    TESTER->test_open(TESTER->shared_existing_file_,
                      MPI_MODE_WRONLY | MPI_MODE_APPEND | MPI_MODE_EXCL,
                      MPI_COMM_WORLD);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_iwrite_all(TESTER->write_data_.data(),
                            TESTER->request_size_, MPI_CHAR);
    REQUIRE((size_t)TESTER->size_written_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    REQUIRE(stdfs::file_size(TESTER->shared_existing_file_.hermes_) ==
        existing_size + TESTER->size_written_orig_);
  }

  SECTION("append to new file") {
    TESTER->test_open(TESTER->shared_new_file_,
                      MPI_MODE_WRONLY | MPI_MODE_CREATE, MPI_COMM_WORLD);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_iwrite_all(TESTER->write_data_.data(),
                            TESTER->request_size_, MPI_CHAR);
    REQUIRE((size_t)TESTER->size_written_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    REQUIRE(stdfs::file_size(TESTER->shared_new_file_.hermes_) ==
        (size_t)TESTER->size_written_orig_);
  }
  SECTION("delete on close mode on new file") {
    TESTER->test_open(
        TESTER->shared_new_file_,
        MPI_MODE_WRONLY | MPI_MODE_CREATE | MPI_MODE_DELETE_ON_CLOSE,
        MPI_COMM_WORLD);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_seek(0, MPI_SEEK_SET);
    REQUIRE(TESTER->status_orig_ == 0);
    TESTER->test_iwrite(TESTER->write_data_.data(),
                        TESTER->request_size_, MPI_CHAR);
    REQUIRE((size_t)TESTER->size_written_orig_ == TESTER->request_size_);
    REQUIRE(stdfs::exists(TESTER->shared_new_file_.hermes_));
    TESTER->test_close();
    REQUIRE(!stdfs::exists(TESTER->shared_new_file_.hermes_));
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
  }

  SECTION("delete on close mode on existing file") {
    auto original_size = stdfs::file_size(
        TESTER->shared_existing_file_.hermes_);
    TESTER->test_open(TESTER->shared_existing_file_,
                      MPI_MODE_WRONLY | MPI_MODE_EXCL |
                          MPI_MODE_DELETE_ON_CLOSE,
                      MPI_COMM_WORLD);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_seek(TESTER->rank_ * TESTER->request_size_, MPI_SEEK_SET);
    REQUIRE(TESTER->status_orig_ == 0);
    TESTER->test_iwrite(TESTER->write_data_.data(),
                        TESTER->request_size_, MPI_CHAR);
    REQUIRE((size_t)TESTER->size_written_orig_ == TESTER->request_size_);
    REQUIRE(stdfs::exists(TESTER->shared_existing_file_.hermes_));
    auto new_size =
        original_size > (size_t)TESTER->size_written_orig_ * TESTER->comm_size_
        ? original_size
        : TESTER->size_written_orig_ * TESTER->comm_size_;
    REQUIRE(stdfs::file_size(TESTER->shared_existing_file_.hermes_) ==
        (size_t)new_size);
    TESTER->test_close();
    REQUIRE(!stdfs::exists(TESTER->shared_existing_file_.hermes_));
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    check_bytes = false;
  }
  TESTER->Posttest(check_bytes);
}

TEST_CASE("SingleRead", "[process=" + std::to_string(TESTER->comm_size_) +
    "]"
    "[operation=single_read]"
    "[synchronicity=sync]"
    "[coordination=independent]"
    "[request_size=type-fixed][repetition=1]"
    "[file=1]") {
  TESTER->Pretest();
  SECTION("read from non-existing file") {
    TESTER->test_open(TESTER->new_file_, MPI_MODE_RDONLY | MPI_MODE_EXCL,
                      MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ != MPI_SUCCESS);
  }

  SECTION("read from existing file") {
    TESTER->test_open(TESTER->existing_file_, MPI_MODE_RDONLY, MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_seek(TESTER->rank_ * TESTER->request_size_, MPI_SEEK_SET);
    REQUIRE(TESTER->status_orig_ == 0);
    TESTER->test_read(TESTER->read_data_.data(),
                      TESTER->request_size_, MPI_CHAR);
    REQUIRE((size_t)TESTER->size_read_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
  }

  SECTION("read from existing file using shared ptr") {
    TESTER->test_open(TESTER->shared_existing_file_, MPI_MODE_RDONLY,
                      MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_seek_shared(0, MPI_SEEK_SET);
    REQUIRE(TESTER->status_orig_ == 0);
    TESTER->test_read_shared(TESTER->read_data_.data(),
                             TESTER->request_size_, MPI_CHAR);
    REQUIRE((size_t)TESTER->size_read_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
  }

  SECTION("read at the end of existing file") {
    TESTER->test_open(TESTER->existing_file_, MPI_MODE_RDONLY, MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_seek(0, MPI_SEEK_END);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    MPI_Offset offset;
    MPI_File_get_position(TESTER->fh_orig_, &offset);
    REQUIRE(offset ==
        (long long)(TESTER->request_size_ * TESTER->num_iterations_));
    TESTER->test_read(TESTER->read_data_.data(),
                      TESTER->request_size_, MPI_CHAR);
    REQUIRE(TESTER->size_read_orig_ == 0);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
  }

  SECTION("read_at from existing file") {
    TESTER->test_open(TESTER->existing_file_, MPI_MODE_RDONLY, MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_read_at(TESTER->read_data_.data(),
                         TESTER->request_size_, MPI_CHAR,
                         TESTER->rank_ * TESTER->request_size_);
    REQUIRE((size_t)TESTER->size_read_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
  }

  TESTER->Posttest();
}

TEST_CASE("SingleReadCollective", "[process=" +
    std::to_string(TESTER->comm_size_) +
    "]"
    "[operation=single_read]"
    "[synchronicity=sync]"
    "[coordination=collective]"
    "[request_size=type-fixed][repetition=1]"
    "[file=1]") {
  TESTER->Pretest();
  SECTION("read from non-existing file") {
    TESTER->test_open(TESTER->shared_new_file_, MPI_MODE_RDONLY,
                      MPI_COMM_WORLD);
    REQUIRE(TESTER->status_orig_ != MPI_SUCCESS);
  }

  SECTION("read from existing file") {
    TESTER->test_open(TESTER->shared_existing_file_, MPI_MODE_RDONLY,
                      MPI_COMM_WORLD);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_seek(TESTER->rank_ * TESTER->request_size_, MPI_SEEK_SET);
    REQUIRE(TESTER->status_orig_ == 0);
    TESTER->test_read_all(TESTER->read_data_.data(),
                          TESTER->request_size_, MPI_CHAR);
    REQUIRE((size_t)TESTER->size_read_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
  }

  SECTION("read from existing file using shared ptr") {
    TESTER->test_open(TESTER->shared_existing_file_, MPI_MODE_RDONLY,
                      MPI_COMM_WORLD);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_seek_shared(0, MPI_SEEK_SET);
    REQUIRE(TESTER->status_orig_ == 0);
    TESTER->test_read_shared(TESTER->read_data_.data(),
                             TESTER->request_size_, MPI_CHAR);
    REQUIRE((size_t)TESTER->size_read_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
  }

  SECTION("read_at_all from existing file") {
    TESTER->test_open(TESTER->existing_file_, MPI_MODE_RDONLY, MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_read_at_all(TESTER->read_data_.data(),
                             TESTER->request_size_, MPI_CHAR,
                             TESTER->rank_ * TESTER->request_size_);
    REQUIRE((size_t)TESTER->size_read_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
  }

  SECTION("read_ordered from existing file") {
    TESTER->test_open(TESTER->existing_file_, MPI_MODE_RDONLY,
                      MPI_COMM_WORLD);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_read_ordered(TESTER->read_data_.data(),
                              TESTER->request_size_, MPI_CHAR);
    REQUIRE((size_t)TESTER->size_read_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
  }
  TESTER->Posttest();
}

TEST_CASE("SingleAsyncRead", "[process=" + std::to_string(TESTER->comm_size_) +
    "]"
    "[operation=single_read]"
    "[synchronicity=async]"
    "[coordination=independent]"
    "[request_size=type-fixed][repetition=1]"
    "[file=1]") {
  TESTER->Pretest();
  SECTION("read from non-existing file") {
    TESTER->test_open(TESTER->new_file_, MPI_MODE_RDONLY | MPI_MODE_EXCL,
                      MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ != MPI_SUCCESS);
  }

  SECTION("read from existing file") {
    TESTER->test_open(TESTER->existing_file_, MPI_MODE_RDONLY, MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_seek(TESTER->rank_ * TESTER->request_size_, MPI_SEEK_SET);
    REQUIRE(TESTER->status_orig_ == 0);
    TESTER->test_iread(TESTER->read_data_.data(),
                       TESTER->request_size_, MPI_CHAR);
    REQUIRE((size_t)TESTER->size_read_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
  }

  SECTION("read from existing file using shared ptr") {
    TESTER->test_open(TESTER->shared_existing_file_, MPI_MODE_RDONLY,
                      MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_seek_shared(0, MPI_SEEK_SET);
    REQUIRE(TESTER->status_orig_ == 0);
    TESTER->test_iread_shared(TESTER->read_data_.data(),
                              TESTER->request_size_, MPI_CHAR);
    REQUIRE((size_t)TESTER->size_read_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
  }

  SECTION("read at the end of existing file") {
    TESTER->test_open(TESTER->existing_file_, MPI_MODE_RDONLY, MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_seek(0, MPI_SEEK_END);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    MPI_Offset offset;
    MPI_File_get_position(TESTER->fh_orig_, &offset);
    REQUIRE(offset ==
        (long long)(TESTER->request_size_ * TESTER->num_iterations_));
    TESTER->test_iread(TESTER->read_data_.data(),
                       TESTER->request_size_, MPI_CHAR);
    REQUIRE(TESTER->size_read_orig_ == 0);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
  }

  SECTION("read_at from existing file") {
    TESTER->test_open(TESTER->existing_file_, MPI_MODE_RDONLY, MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_iread_at(TESTER->read_data_.data(),
                          TESTER->request_size_, MPI_CHAR,
                          TESTER->rank_ * TESTER->request_size_);
    REQUIRE((size_t)TESTER->size_read_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
  }

  TESTER->Posttest();
}

// TODO(chogan): This test fails sporadically.
// https://github.com/HDFGroup/hermes/issues/413
TEST_CASE("SingleAsyncReadCollective",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=single_read]"
              "[synchronicity=async]"
              "[coordination=collective]"
              "[request_size=type-fixed][repetition=1]"
              "[file=1]") {
  TESTER->Pretest();
  SECTION("read from non-existing file") {
    TESTER->test_open(TESTER->shared_new_file_, MPI_MODE_RDONLY,
                      MPI_COMM_WORLD);
    REQUIRE(TESTER->status_orig_ != MPI_SUCCESS);
  }

  SECTION("read from existing file") {
    TESTER->test_open(TESTER->shared_existing_file_, MPI_MODE_RDONLY,
                      MPI_COMM_WORLD);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_seek(TESTER->rank_ * TESTER->request_size_, MPI_SEEK_SET);
    REQUIRE(TESTER->status_orig_ == 0);
    TESTER->test_iread_all(TESTER->read_data_.data(),
                           TESTER->request_size_, MPI_CHAR);
    REQUIRE((size_t)TESTER->size_read_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
  }

  SECTION("read from existing file using shared ptr") {
    TESTER->test_open(TESTER->shared_existing_file_, MPI_MODE_RDONLY,
                      MPI_COMM_WORLD);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_seek_shared(0, MPI_SEEK_SET);
    REQUIRE(TESTER->status_orig_ == 0);
    TESTER->test_iread_shared(TESTER->read_data_.data(),
                              TESTER->request_size_, MPI_CHAR);
    REQUIRE((size_t)TESTER->size_read_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
  }

  SECTION("read_at_all from existing file") {
    TESTER->test_open(TESTER->existing_file_, MPI_MODE_RDONLY, MPI_COMM_SELF);
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
    TESTER->test_iread_at_all(TESTER->read_data_.data(),
                              TESTER->request_size_, MPI_CHAR,
                              TESTER->rank_ * TESTER->request_size_);
    REQUIRE((size_t)TESTER->size_read_orig_ == TESTER->request_size_);
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == MPI_SUCCESS);
  }
  TESTER->Posttest();
}
