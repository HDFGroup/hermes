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

TEST_CASE("Open", "[process=" + std::to_string(TEST_INFO->comm_size_) +
                      "]"
                      "[coordination=independent]"
                      "[synchronicity=sync]"
                      "[operation=single_open]"
                      "[repetition=1][file=1]") {
  TEST_INFO->Pretest();
  SECTION("open non-existant file") {
    TEST_INFO->test_open(TEST_INFO->new_file_, MPI_MODE_RDONLY, MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ != MPI_SUCCESS);
    TEST_INFO->test_open(TEST_INFO->new_file_, MPI_MODE_RDWR | MPI_MODE_EXCL,
                    MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ != MPI_SUCCESS);
  }

  SECTION("opening existing file write-only") {
    TEST_INFO->test_open(TEST_INFO->existing_file_, MPI_MODE_WRONLY | MPI_MODE_EXCL,
                    MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
  }
  SECTION("opening existing file and read/write") {
    TEST_INFO->test_open(TEST_INFO->existing_file_, MPI_MODE_RDWR | MPI_MODE_CREATE,
                    MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
  }

  SECTION("open existing file") {
    TEST_INFO->test_open(TEST_INFO->existing_file_, MPI_MODE_RDONLY | MPI_MODE_EXCL,
                    MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ != MPI_SUCCESS);
    TEST_INFO->test_open(TEST_INFO->existing_file_, MPI_MODE_RDWR, MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_open(TEST_INFO->existing_file_, MPI_MODE_RDONLY, MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
  }

  SECTION("append write existing file") {
    TEST_INFO->test_open(TEST_INFO->existing_file_, MPI_MODE_APPEND | MPI_MODE_EXCL,
                    MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ != MPI_SUCCESS);
    TEST_INFO->test_open(TEST_INFO->existing_file_, MPI_MODE_APPEND, MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ != MPI_SUCCESS);
    TEST_INFO->test_open(TEST_INFO->existing_file_,
                    MPI_MODE_WRONLY | MPI_MODE_APPEND, MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_open(TEST_INFO->existing_file_,
                    MPI_MODE_RDONLY | MPI_MODE_APPEND, MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_open(TEST_INFO->existing_file_, MPI_MODE_RDWR | MPI_MODE_APPEND,
                    MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
  }

  SECTION("append write and read new file") {
    TEST_INFO->test_open(TEST_INFO->existing_file_,
                    MPI_MODE_RDWR | MPI_MODE_APPEND | MPI_MODE_CREATE,
                    MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
  }

  SECTION("delete on close mode") {
    TEST_INFO->test_open(
        TEST_INFO->new_file_,
        MPI_MODE_WRONLY | MPI_MODE_CREATE | MPI_MODE_DELETE_ON_CLOSE,
        MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    REQUIRE(stdfs::exists(TEST_INFO->new_file_.hermes_));
    TEST_INFO->test_close();
    REQUIRE(!stdfs::exists(TEST_INFO->new_file_.hermes_));
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("OpenCollective", "[process=" + std::to_string(TEST_INFO->comm_size_) +
                                "]"
                                "[coordination=collective]"
                                "[synchronicity=sync]"
                                "[operation=single_open]"
                                "[repetition=1][file=1]") {
  TEST_INFO->Pretest();
  SECTION("open on non-existant shared file") {
    TEST_INFO->test_open(TEST_INFO->shared_new_file_,
                    MPI_MODE_RDONLY | MPI_MODE_EXCL, MPI_COMM_WORLD);
    REQUIRE(TEST_INFO->status_orig_ != MPI_SUCCESS);
    TEST_INFO->test_open(TEST_INFO->shared_new_file_,
                    MPI_MODE_RDONLY | MPI_MODE_CREATE, MPI_COMM_WORLD);
    REQUIRE(TEST_INFO->status_orig_ != MPI_SUCCESS);
    TEST_INFO->test_open(TEST_INFO->shared_new_file_, MPI_MODE_RDWR | MPI_MODE_EXCL,
                    MPI_COMM_WORLD);
    REQUIRE(TEST_INFO->status_orig_ != MPI_SUCCESS);
    TEST_INFO->test_open(TEST_INFO->shared_new_file_,
                    MPI_MODE_WRONLY | MPI_MODE_CREATE, MPI_COMM_WORLD);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_open(TEST_INFO->shared_new_file_,
                    MPI_MODE_RDWR | MPI_MODE_CREATE, MPI_COMM_WORLD);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
  }

  SECTION("open on shared existing file") {
    TEST_INFO->test_open(TEST_INFO->shared_existing_file_,
                    MPI_MODE_WRONLY | MPI_MODE_EXCL, MPI_COMM_WORLD);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_open(TEST_INFO->shared_existing_file_,
                    MPI_MODE_RDWR | MPI_MODE_EXCL, MPI_COMM_WORLD);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_open(TEST_INFO->shared_existing_file_,
                    MPI_MODE_RDONLY | MPI_MODE_EXCL, MPI_COMM_WORLD);
    REQUIRE(TEST_INFO->status_orig_ != MPI_SUCCESS);
    TEST_INFO->test_open(TEST_INFO->shared_existing_file_, MPI_MODE_RDONLY,
                    MPI_COMM_WORLD);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_open(TEST_INFO->shared_existing_file_, MPI_MODE_RDWR,
                    MPI_COMM_WORLD);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
  }

  SECTION("append write/read on shared existing file") {
    TEST_INFO->test_open(TEST_INFO->shared_existing_file_,
                    MPI_MODE_RDWR | MPI_MODE_APPEND, MPI_COMM_WORLD);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
  }

  SECTION("append write and read on shared new file") {
    TEST_INFO->test_open(TEST_INFO->shared_new_file_,
                    MPI_MODE_RDWR | MPI_MODE_APPEND | MPI_MODE_CREATE,
                    MPI_COMM_WORLD);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
  }

  SECTION("delete on close mode on new file") {
    TEST_INFO->test_open(
        TEST_INFO->shared_new_file_,
        MPI_MODE_WRONLY | MPI_MODE_CREATE | MPI_MODE_DELETE_ON_CLOSE,
        MPI_COMM_WORLD);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    REQUIRE(!stdfs::exists(TEST_INFO->shared_new_file_.hermes_));
  }
  TEST_INFO->Posttest();
}

TEST_CASE("SingleWrite", "[process=" + std::to_string(TEST_INFO->comm_size_) +
                             "]"
                             "[operation=single_write]"
                             "[synchronicity=sync]"
                             "[coordination=independent]"
                             "[request_size=type-fixed][repetition=1]"
                             "[file=1]") {
  TEST_INFO->Pretest();
  bool check_bytes = true;
  SECTION("write to existing file") {
    TEST_INFO->test_open(TEST_INFO->existing_file_, MPI_MODE_RDWR, MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_seek(TEST_INFO->rank_ * TEST_INFO->request_size_, MPI_SEEK_SET);
    REQUIRE(TEST_INFO->status_orig_ == 0);
    TEST_INFO->test_write(TEST_INFO->write_data_.data(), TEST_INFO->request_size_, MPI_CHAR);
    REQUIRE((size_t)TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
  }

  SECTION("write to new  file") {
    TEST_INFO->test_open(TEST_INFO->new_file_, MPI_MODE_WRONLY | MPI_MODE_CREATE,
                    MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_write(TEST_INFO->write_data_.data(), TEST_INFO->request_size_, MPI_CHAR);
    REQUIRE((size_t)TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    REQUIRE(stdfs::file_size(TEST_INFO->new_file_.hermes_) == (size_t)TEST_INFO->size_written_orig_);
  }

  SECTION("write to new file with allocate") {
    TEST_INFO->test_open(TEST_INFO->shared_new_file_,
                    MPI_MODE_WRONLY | MPI_MODE_CREATE, MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_preallocate(TEST_INFO->request_size_ * TEST_INFO->comm_size_);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_seek(TEST_INFO->request_size_ * TEST_INFO->rank_, MPI_SEEK_SET);
    REQUIRE(TEST_INFO->status_orig_ == 0);
    TEST_INFO->test_write(TEST_INFO->write_data_.data(), TEST_INFO->request_size_, MPI_CHAR);
    REQUIRE((size_t)TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    MPI_Barrier(MPI_COMM_WORLD);
    REQUIRE(stdfs::file_size(TEST_INFO->shared_new_file_.hermes_) ==
            (size_t)TEST_INFO->size_written_orig_ * TEST_INFO->comm_size_);
  }

  SECTION("append to existing file") {
    auto existing_size = stdfs::file_size(TEST_INFO->existing_file_.hermes_);
    TEST_INFO->test_open(TEST_INFO->existing_file_,
                    MPI_MODE_WRONLY | MPI_MODE_APPEND | MPI_MODE_EXCL,
                    MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_write(TEST_INFO->write_data_.data(), TEST_INFO->request_size_, MPI_CHAR);
    REQUIRE((size_t)TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    REQUIRE(stdfs::file_size(TEST_INFO->existing_file_.hermes_) ==
            existing_size + TEST_INFO->size_written_orig_);
  }

  SECTION("append to new file") {
    TEST_INFO->test_open(TEST_INFO->new_file_,
                    MPI_MODE_WRONLY | MPI_MODE_APPEND | MPI_MODE_CREATE,
                    MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_write(TEST_INFO->write_data_.data(), TEST_INFO->request_size_, MPI_CHAR);
    REQUIRE((size_t)TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    REQUIRE(stdfs::file_size(TEST_INFO->new_file_.hermes_) == (size_t)TEST_INFO->size_written_orig_);
  }

  SECTION("write_at to existing file") {
    TEST_INFO->test_open(TEST_INFO->existing_file_, MPI_MODE_RDWR, MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_write_at(TEST_INFO->write_data_.data(), TEST_INFO->request_size_, MPI_CHAR,
                        TEST_INFO->rank_ * TEST_INFO->request_size_);
    REQUIRE((size_t)TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
  }

  SECTION("write_at to new file") {
    TEST_INFO->test_open(TEST_INFO->new_file_, MPI_MODE_WRONLY | MPI_MODE_CREATE,
                    MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_write_at(TEST_INFO->write_data_.data(), TEST_INFO->request_size_, MPI_CHAR,
                        0);
    REQUIRE((size_t)TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    REQUIRE(stdfs::file_size(TEST_INFO->new_file_.hermes_) == (size_t)TEST_INFO->size_written_orig_);
  }

  SECTION("delete on close mode on new file") {
    TEST_INFO->test_open(
        TEST_INFO->new_file_,
        MPI_MODE_WRONLY | MPI_MODE_CREATE | MPI_MODE_DELETE_ON_CLOSE,
        MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_seek(0, MPI_SEEK_SET);
    REQUIRE(TEST_INFO->status_orig_ == 0);
    TEST_INFO->test_write(TEST_INFO->write_data_.data(), TEST_INFO->request_size_, MPI_CHAR);
    REQUIRE((size_t)TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    REQUIRE(stdfs::exists(TEST_INFO->new_file_.hermes_));
    TEST_INFO->test_close();
    REQUIRE(!stdfs::exists(TEST_INFO->new_file_.hermes_));
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
  }

  SECTION("delete on close mode on existing file") {
    auto original_size = stdfs::file_size(TEST_INFO->existing_file_.hermes_);
    TEST_INFO->test_open(TEST_INFO->existing_file_,
                    MPI_MODE_WRONLY | MPI_MODE_EXCL | MPI_MODE_DELETE_ON_CLOSE,
                    MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_seek(TEST_INFO->rank_ * TEST_INFO->request_size_, MPI_SEEK_SET);
    REQUIRE(TEST_INFO->status_orig_ == 0);
    TEST_INFO->test_write(TEST_INFO->write_data_.data(), TEST_INFO->request_size_, MPI_CHAR);
    REQUIRE((size_t)TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    REQUIRE(stdfs::exists(TEST_INFO->existing_file_.hermes_));
    auto new_size =
        original_size > (size_t)TEST_INFO->size_written_orig_ * TEST_INFO->comm_size_
            ? original_size
            : TEST_INFO->size_written_orig_ * TEST_INFO->comm_size_;
    REQUIRE(stdfs::file_size(TEST_INFO->existing_file_.hermes_) == (size_t)new_size);
    TEST_INFO->test_close();
    REQUIRE(!stdfs::exists(TEST_INFO->existing_file_.hermes_));
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    check_bytes = false;
  }
  TEST_INFO->Posttest(check_bytes);
}

TEST_CASE("SingleWriteCollective",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=single_write]"
              "[synchronicity=sync]"
              "[coordination=collective]"
              "[request_size=type-fixed][repetition=1]"
              "[file=1]") {
  TEST_INFO->Pretest();
  bool check_bytes = true;
  SECTION("write to existing file") {
    TEST_INFO->test_open(TEST_INFO->shared_existing_file_, MPI_MODE_RDWR,
                    MPI_COMM_WORLD);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_seek(TEST_INFO->rank_ * TEST_INFO->request_size_, MPI_SEEK_SET);
    REQUIRE(TEST_INFO->status_orig_ == 0);
    TEST_INFO->test_write(TEST_INFO->write_data_.data(), TEST_INFO->request_size_, MPI_CHAR);
    REQUIRE((size_t)TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
  }

  SECTION("write to new  file") {
    TEST_INFO->test_open(TEST_INFO->shared_new_file_,
                    MPI_MODE_WRONLY | MPI_MODE_CREATE, MPI_COMM_WORLD);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_seek(0, MPI_SEEK_SET);
    REQUIRE(TEST_INFO->status_orig_ == 0);
    TEST_INFO->test_write(TEST_INFO->write_data_.data(), TEST_INFO->request_size_, MPI_CHAR);
    REQUIRE((size_t)TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    REQUIRE(stdfs::file_size(TEST_INFO->shared_new_file_.hermes_) ==
            (size_t)TEST_INFO->size_written_orig_);
  }

  // TODO(chogan): This test fails intermittently. Needs diagnosis.
  // https://github.com/HDFGroup/hermes/issues/209
  SECTION("write to new  file using shared ptr") {
    TEST_INFO->test_open(TEST_INFO->shared_new_file_,
                    MPI_MODE_WRONLY | MPI_MODE_CREATE, MPI_COMM_WORLD);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_seek_shared(0, MPI_SEEK_SET);
    REQUIRE(TEST_INFO->status_orig_ == 0);
    TEST_INFO->test_write_shared(TEST_INFO->write_data_.data(), TEST_INFO->request_size_,
                            MPI_CHAR);
    REQUIRE((size_t)TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    REQUIRE(stdfs::file_size(TEST_INFO->shared_new_file_.hermes_) ==
            (size_t)TEST_INFO->size_written_orig_ * TEST_INFO->comm_size_);
  }

  SECTION("write to new file with allocate") {
    TEST_INFO->test_open(TEST_INFO->shared_new_file_,
                    MPI_MODE_WRONLY | MPI_MODE_CREATE, MPI_COMM_WORLD);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_preallocate(TEST_INFO->request_size_ * TEST_INFO->comm_size_);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_seek(TEST_INFO->request_size_ * TEST_INFO->rank_, MPI_SEEK_SET);
    REQUIRE(TEST_INFO->status_orig_ == 0);
    TEST_INFO->test_write(TEST_INFO->write_data_.data(), TEST_INFO->request_size_, MPI_CHAR);
    REQUIRE((size_t)TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    MPI_Barrier(MPI_COMM_WORLD);
    REQUIRE(stdfs::file_size(TEST_INFO->shared_new_file_.hermes_) ==
            (size_t)TEST_INFO->size_written_orig_ * TEST_INFO->comm_size_);
  }

  SECTION("write_at_all to existing file") {
    TEST_INFO->test_open(TEST_INFO->existing_file_, MPI_MODE_RDWR, MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_write_at_all(TEST_INFO->write_data_.data(), TEST_INFO->request_size_,
                            MPI_CHAR, TEST_INFO->rank_ * TEST_INFO->request_size_);
    REQUIRE((size_t)TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
  }

  SECTION("write_at_all to new file") {
    TEST_INFO->test_open(TEST_INFO->new_file_, MPI_MODE_WRONLY | MPI_MODE_CREATE,
                    MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_write_at_all(TEST_INFO->write_data_.data(), TEST_INFO->request_size_,
                            MPI_CHAR, 0);
    REQUIRE((size_t)TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    REQUIRE(stdfs::file_size(TEST_INFO->new_file_.hermes_) == (size_t)TEST_INFO->size_written_orig_);
  }

  SECTION("append to existing file") {
    auto existing_size = stdfs::file_size(TEST_INFO->existing_file_.hermes_);
    TEST_INFO->test_open(TEST_INFO->shared_existing_file_,
                    MPI_MODE_WRONLY | MPI_MODE_APPEND | MPI_MODE_EXCL,
                    MPI_COMM_WORLD);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_write_all(TEST_INFO->write_data_.data(), TEST_INFO->request_size_, MPI_CHAR);
    REQUIRE((size_t)TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    REQUIRE(stdfs::file_size(TEST_INFO->shared_existing_file_.hermes_) ==
            existing_size + TEST_INFO->size_written_orig_);
  }

  SECTION("append to new file") {
    TEST_INFO->test_open(TEST_INFO->shared_new_file_,
                    MPI_MODE_WRONLY | MPI_MODE_CREATE, MPI_COMM_WORLD);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_write_all(TEST_INFO->write_data_.data(), TEST_INFO->request_size_, MPI_CHAR);
    REQUIRE((size_t)TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    REQUIRE(stdfs::file_size(TEST_INFO->shared_new_file_.hermes_) ==
            (size_t)TEST_INFO->size_written_orig_);
  }

  SECTION("write_ordered to existing file") {
    TEST_INFO->test_open(TEST_INFO->existing_file_, MPI_MODE_RDWR, MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_write_ordered(TEST_INFO->write_data_.data(), TEST_INFO->request_size_,
                             MPI_CHAR);
    REQUIRE((size_t)TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
  }

  SECTION("write_ordered to new file") {
    TEST_INFO->test_open(TEST_INFO->new_file_, MPI_MODE_WRONLY | MPI_MODE_CREATE,
                    MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_write_ordered(TEST_INFO->write_data_.data(), TEST_INFO->request_size_,
                             MPI_CHAR);
    REQUIRE((size_t)TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    REQUIRE(stdfs::file_size(TEST_INFO->new_file_.hermes_) == (size_t)TEST_INFO->size_written_orig_);
  }
  SECTION("delete on close mode on new file") {
    TEST_INFO->test_open(
        TEST_INFO->shared_new_file_,
        MPI_MODE_WRONLY | MPI_MODE_CREATE | MPI_MODE_DELETE_ON_CLOSE,
        MPI_COMM_WORLD);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_seek(TEST_INFO->rank_ * TEST_INFO->request_size_, MPI_SEEK_SET);
    REQUIRE(TEST_INFO->status_orig_ == 0);
    TEST_INFO->test_write(TEST_INFO->write_data_.data(), TEST_INFO->request_size_, MPI_CHAR);
    REQUIRE((size_t)TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    REQUIRE(stdfs::exists(TEST_INFO->shared_new_file_.hermes_));
    MPI_Barrier(MPI_COMM_WORLD);
    TEST_INFO->test_close();
    REQUIRE(!stdfs::exists(TEST_INFO->shared_new_file_.hermes_));
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
  }

  SECTION("delete on close mode on existing file") {
    auto original_size = stdfs::file_size(TEST_INFO->shared_existing_file_.hermes_);
    TEST_INFO->test_open(TEST_INFO->shared_existing_file_,
                    MPI_MODE_WRONLY | MPI_MODE_EXCL | MPI_MODE_DELETE_ON_CLOSE,
                    MPI_COMM_WORLD);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_seek(TEST_INFO->rank_ * TEST_INFO->request_size_, MPI_SEEK_SET);
    REQUIRE(TEST_INFO->status_orig_ == 0);
    TEST_INFO->test_write(TEST_INFO->write_data_.data(), TEST_INFO->request_size_, MPI_CHAR);
    REQUIRE((size_t)TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    REQUIRE(stdfs::exists(TEST_INFO->shared_existing_file_.hermes_));
    auto new_size =
        original_size > (size_t)TEST_INFO->size_written_orig_ * TEST_INFO->comm_size_
            ? original_size
            : TEST_INFO->size_written_orig_ * TEST_INFO->comm_size_;
    REQUIRE(stdfs::file_size(TEST_INFO->shared_existing_file_.hermes_) == (size_t)new_size);
    TEST_INFO->test_close();
    REQUIRE(!stdfs::exists(TEST_INFO->shared_existing_file_.hermes_));
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    check_bytes = false;
  }
  TEST_INFO->Posttest(check_bytes);
}

TEST_CASE("SingleAsyncWrite", "[process=" + std::to_string(TEST_INFO->comm_size_) +
                                  "]"
                                  "[operation=single_write]"
                                  "[coordination=independent]"
                                  "[synchronicity=async]"
                                  "[request_size=type-fixed][repetition=1]"
                                  "[file=1]") {
  TEST_INFO->Pretest();
  bool check_bytes = true;
  SECTION("write to existing file") {
    TEST_INFO->test_open(TEST_INFO->existing_file_, MPI_MODE_RDWR, MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_seek(TEST_INFO->rank_ * TEST_INFO->request_size_, MPI_SEEK_SET);
    REQUIRE(TEST_INFO->status_orig_ == 0);
    TEST_INFO->test_iwrite(TEST_INFO->write_data_.data(), TEST_INFO->request_size_, MPI_CHAR);
    REQUIRE((size_t)TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
  }

  SECTION("write to new  file") {
    TEST_INFO->test_open(TEST_INFO->new_file_, MPI_MODE_WRONLY | MPI_MODE_CREATE,
                    MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_seek(0, MPI_SEEK_SET);
    REQUIRE(TEST_INFO->status_orig_ == 0);
    TEST_INFO->test_iwrite(TEST_INFO->write_data_.data(), TEST_INFO->request_size_, MPI_CHAR);
    REQUIRE((size_t)TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    REQUIRE(stdfs::file_size(TEST_INFO->new_file_.hermes_) == (size_t)TEST_INFO->size_written_orig_);
  }

  SECTION("write to new  file using shared ptr") {
    TEST_INFO->test_open(TEST_INFO->shared_new_file_,
                    MPI_MODE_WRONLY | MPI_MODE_CREATE, MPI_COMM_WORLD);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_seek_shared(0, MPI_SEEK_SET);
    REQUIRE(TEST_INFO->status_orig_ == 0);
    TEST_INFO->test_iwrite_shared(TEST_INFO->write_data_.data(), TEST_INFO->request_size_,
                             MPI_CHAR);
    REQUIRE((size_t)TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    REQUIRE(stdfs::file_size(TEST_INFO->shared_new_file_.hermes_) ==
            (size_t)TEST_INFO->size_written_orig_ * TEST_INFO->comm_size_);
  }

  SECTION("write to new file with allocate") {
    TEST_INFO->test_open(TEST_INFO->shared_new_file_,
                    MPI_MODE_WRONLY | MPI_MODE_CREATE, MPI_COMM_WORLD);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_preallocate(TEST_INFO->request_size_ * TEST_INFO->comm_size_);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_seek(TEST_INFO->request_size_ * TEST_INFO->rank_, MPI_SEEK_SET);
    REQUIRE(TEST_INFO->status_orig_ == 0);
    TEST_INFO->test_write(TEST_INFO->write_data_.data(), TEST_INFO->request_size_, MPI_CHAR);
    REQUIRE((size_t)TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    MPI_Barrier(MPI_COMM_WORLD);
    REQUIRE(stdfs::file_size(TEST_INFO->shared_new_file_.hermes_) ==
            (size_t)TEST_INFO->size_written_orig_ * TEST_INFO->comm_size_);
  }

  SECTION("append to existing file") {
    auto existing_size = stdfs::file_size(TEST_INFO->existing_file_.hermes_);
    TEST_INFO->test_open(TEST_INFO->existing_file_,
                    MPI_MODE_WRONLY | MPI_MODE_APPEND | MPI_MODE_EXCL,
                    MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_iwrite(TEST_INFO->write_data_.data(), TEST_INFO->request_size_, MPI_CHAR);
    REQUIRE((size_t)TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    REQUIRE(stdfs::file_size(TEST_INFO->existing_file_.hermes_) ==
            existing_size + TEST_INFO->size_written_orig_);
  }

  SECTION("append to new file") {
    TEST_INFO->test_open(TEST_INFO->new_file_,
                    MPI_MODE_WRONLY | MPI_MODE_APPEND | MPI_MODE_CREATE,
                    MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_iwrite(TEST_INFO->write_data_.data(), TEST_INFO->request_size_, MPI_CHAR);
    REQUIRE((size_t)TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    REQUIRE(stdfs::file_size(TEST_INFO->new_file_.hermes_) == (size_t)TEST_INFO->size_written_orig_);
  }

  SECTION("write_at to existing file") {
    TEST_INFO->test_open(TEST_INFO->existing_file_, MPI_MODE_RDWR, MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_iwrite_at(TEST_INFO->write_data_.data(), TEST_INFO->request_size_, MPI_CHAR,
                         TEST_INFO->rank_ * TEST_INFO->request_size_);
    REQUIRE((size_t)TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
  }

  SECTION("write_at to new file") {
    TEST_INFO->test_open(TEST_INFO->new_file_, MPI_MODE_WRONLY | MPI_MODE_CREATE,
                    MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_iwrite_at(TEST_INFO->write_data_.data(), TEST_INFO->request_size_, MPI_CHAR,
                         0);
    REQUIRE((size_t)TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    REQUIRE(stdfs::file_size(TEST_INFO->new_file_.hermes_) == (size_t)TEST_INFO->size_written_orig_);
  }

  SECTION("delete on close mode on new file") {
    TEST_INFO->test_open(
        TEST_INFO->new_file_,
        MPI_MODE_WRONLY | MPI_MODE_CREATE | MPI_MODE_DELETE_ON_CLOSE,
        MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_seek(0, MPI_SEEK_SET);
    REQUIRE(TEST_INFO->status_orig_ == 0);
    TEST_INFO->test_iwrite(TEST_INFO->write_data_.data(), TEST_INFO->request_size_, MPI_CHAR);
    REQUIRE((size_t)TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    REQUIRE(stdfs::exists(TEST_INFO->new_file_.hermes_));
    TEST_INFO->test_close();
    REQUIRE(!stdfs::exists(TEST_INFO->new_file_.hermes_));
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
  }

  SECTION("delete on close mode on existing file") {
    TEST_INFO->test_open(TEST_INFO->existing_file_,
                    MPI_MODE_WRONLY | MPI_MODE_EXCL | MPI_MODE_DELETE_ON_CLOSE,
                    MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_seek(TEST_INFO->rank_ * TEST_INFO->request_size_, MPI_SEEK_SET);
    REQUIRE(TEST_INFO->status_orig_ == 0);
    TEST_INFO->test_iwrite(TEST_INFO->write_data_.data(), TEST_INFO->request_size_, MPI_CHAR);
    REQUIRE((size_t)TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    REQUIRE(stdfs::exists(TEST_INFO->existing_file_.hermes_));
    TEST_INFO->test_close();
    REQUIRE(!stdfs::exists(TEST_INFO->existing_file_.hermes_));
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    check_bytes = false;
  }
  TEST_INFO->Posttest(check_bytes);
}

TEST_CASE("SingleAsyncWriteCollective",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=single_write]"
              "[synchronicity=async]"
              "[coordination=collective]"
              "[request_size=type-fixed][repetition=1]"
              "[file=1]") {
  TEST_INFO->Pretest();
  bool check_bytes = true;
  SECTION("write to existing file") {
    TEST_INFO->test_open(TEST_INFO->shared_existing_file_, MPI_MODE_RDWR,
                    MPI_COMM_WORLD);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_seek(TEST_INFO->rank_ * TEST_INFO->request_size_, MPI_SEEK_SET);
    REQUIRE(TEST_INFO->status_orig_ == 0);
    TEST_INFO->test_iwrite(TEST_INFO->write_data_.data(), TEST_INFO->request_size_, MPI_CHAR);
    REQUIRE((size_t)TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
  }

  SECTION("write to new  file") {
    TEST_INFO->test_open(TEST_INFO->shared_new_file_,
                    MPI_MODE_WRONLY | MPI_MODE_CREATE, MPI_COMM_WORLD);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_seek(0, MPI_SEEK_SET);
    REQUIRE(TEST_INFO->status_orig_ == 0);
    TEST_INFO->test_iwrite(TEST_INFO->write_data_.data(), TEST_INFO->request_size_, MPI_CHAR);
    REQUIRE((size_t)TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    REQUIRE(stdfs::file_size(TEST_INFO->shared_new_file_.hermes_) ==
            (size_t)TEST_INFO->size_written_orig_);
  }

  SECTION("write to new  file using shared ptr") {
    TEST_INFO->test_open(TEST_INFO->shared_new_file_,
                    MPI_MODE_WRONLY | MPI_MODE_CREATE, MPI_COMM_WORLD);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_seek_shared(0, MPI_SEEK_SET);
    REQUIRE(TEST_INFO->status_orig_ == 0);
    TEST_INFO->test_iwrite_shared(TEST_INFO->write_data_.data(), TEST_INFO->request_size_,
                             MPI_CHAR);
    REQUIRE((size_t)TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    REQUIRE(stdfs::file_size(TEST_INFO->shared_new_file_.hermes_) ==
            (size_t)TEST_INFO->size_written_orig_ * TEST_INFO->comm_size_);
  }

  SECTION("write to new file with allocate") {
    TEST_INFO->test_open(TEST_INFO->shared_new_file_,
                    MPI_MODE_WRONLY | MPI_MODE_CREATE, MPI_COMM_WORLD);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_preallocate(TEST_INFO->request_size_ * TEST_INFO->comm_size_);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_seek(TEST_INFO->request_size_ * TEST_INFO->rank_, MPI_SEEK_SET);
    REQUIRE(TEST_INFO->status_orig_ == 0);
    TEST_INFO->test_iwrite(TEST_INFO->write_data_.data(), TEST_INFO->request_size_, MPI_CHAR);
    REQUIRE((size_t)TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    MPI_Barrier(MPI_COMM_WORLD);
    REQUIRE(stdfs::file_size(TEST_INFO->shared_new_file_.hermes_) ==
            (size_t)TEST_INFO->size_written_orig_ * TEST_INFO->comm_size_);
  }

  SECTION("write_at_all to existing file") {
    TEST_INFO->test_open(TEST_INFO->existing_file_, MPI_MODE_RDWR, MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_iwrite_at_all(TEST_INFO->write_data_.data(), TEST_INFO->request_size_,
                             MPI_CHAR, TEST_INFO->rank_ * TEST_INFO->request_size_);
    REQUIRE((size_t)TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
  }

  SECTION("write_at_all to new file") {
    TEST_INFO->test_open(TEST_INFO->new_file_, MPI_MODE_WRONLY | MPI_MODE_CREATE,
                    MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_iwrite_at_all(TEST_INFO->write_data_.data(), TEST_INFO->request_size_,
                             MPI_CHAR, 0);
    REQUIRE((size_t)TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    REQUIRE(stdfs::file_size(TEST_INFO->new_file_.hermes_) == (size_t)TEST_INFO->size_written_orig_);
  }
  SECTION("append to existing file") {
    auto existing_size = stdfs::file_size(TEST_INFO->existing_file_.hermes_);
    TEST_INFO->test_open(TEST_INFO->shared_existing_file_,
                    MPI_MODE_WRONLY | MPI_MODE_APPEND | MPI_MODE_EXCL,
                    MPI_COMM_WORLD);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_iwrite_all(TEST_INFO->write_data_.data(), TEST_INFO->request_size_, MPI_CHAR);
    REQUIRE((size_t)TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    REQUIRE(stdfs::file_size(TEST_INFO->shared_existing_file_.hermes_) ==
            existing_size + TEST_INFO->size_written_orig_);
  }

  SECTION("append to new file") {
    TEST_INFO->test_open(TEST_INFO->shared_new_file_,
                    MPI_MODE_WRONLY | MPI_MODE_CREATE, MPI_COMM_WORLD);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_iwrite_all(TEST_INFO->write_data_.data(), TEST_INFO->request_size_, MPI_CHAR);
    REQUIRE((size_t)TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    REQUIRE(stdfs::file_size(TEST_INFO->shared_new_file_.hermes_) ==
            (size_t)TEST_INFO->size_written_orig_);
  }
  SECTION("delete on close mode on new file") {
    TEST_INFO->test_open(
        TEST_INFO->shared_new_file_,
        MPI_MODE_WRONLY | MPI_MODE_CREATE | MPI_MODE_DELETE_ON_CLOSE,
        MPI_COMM_WORLD);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_seek(0, MPI_SEEK_SET);
    REQUIRE(TEST_INFO->status_orig_ == 0);
    TEST_INFO->test_iwrite(TEST_INFO->write_data_.data(), TEST_INFO->request_size_, MPI_CHAR);
    REQUIRE((size_t)TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    REQUIRE(stdfs::exists(TEST_INFO->shared_new_file_.hermes_));
    TEST_INFO->test_close();
    REQUIRE(!stdfs::exists(TEST_INFO->shared_new_file_.hermes_));
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
  }

  SECTION("delete on close mode on existing file") {
    auto original_size = stdfs::file_size(TEST_INFO->shared_existing_file_.hermes_);
    TEST_INFO->test_open(TEST_INFO->shared_existing_file_,
                    MPI_MODE_WRONLY | MPI_MODE_EXCL | MPI_MODE_DELETE_ON_CLOSE,
                    MPI_COMM_WORLD);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_seek(TEST_INFO->rank_ * TEST_INFO->request_size_, MPI_SEEK_SET);
    REQUIRE(TEST_INFO->status_orig_ == 0);
    TEST_INFO->test_iwrite(TEST_INFO->write_data_.data(), TEST_INFO->request_size_, MPI_CHAR);
    REQUIRE((size_t)TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    REQUIRE(stdfs::exists(TEST_INFO->shared_existing_file_.hermes_));
    auto new_size =
        original_size > (size_t)TEST_INFO->size_written_orig_ * TEST_INFO->comm_size_
            ? original_size
            : TEST_INFO->size_written_orig_ * TEST_INFO->comm_size_;
    REQUIRE(stdfs::file_size(TEST_INFO->shared_existing_file_.hermes_) == (size_t)new_size);
    TEST_INFO->test_close();
    REQUIRE(!stdfs::exists(TEST_INFO->shared_existing_file_.hermes_));
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    check_bytes = false;
  }
  TEST_INFO->Posttest(check_bytes);
}

TEST_CASE("SingleRead", "[process=" + std::to_string(TEST_INFO->comm_size_) +
                            "]"
                            "[operation=single_read]"
                            "[synchronicity=sync]"
                            "[coordination=independent]"
                            "[request_size=type-fixed][repetition=1]"
                            "[file=1]") {
  TEST_INFO->Pretest();
  SECTION("read from non-existing file") {
    TEST_INFO->test_open(TEST_INFO->new_file_, MPI_MODE_RDONLY | MPI_MODE_EXCL,
                    MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ != MPI_SUCCESS);
  }

  SECTION("read from existing file") {
    TEST_INFO->test_open(TEST_INFO->existing_file_, MPI_MODE_RDONLY, MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_seek(TEST_INFO->rank_ * TEST_INFO->request_size_, MPI_SEEK_SET);
    REQUIRE(TEST_INFO->status_orig_ == 0);
    TEST_INFO->test_read(TEST_INFO->read_data_.data(), TEST_INFO->request_size_, MPI_CHAR);
    REQUIRE((size_t)TEST_INFO->size_read_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
  }

  SECTION("read from existing file using shared ptr") {
    TEST_INFO->test_open(TEST_INFO->shared_existing_file_, MPI_MODE_RDONLY,
                    MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_seek_shared(0, MPI_SEEK_SET);
    REQUIRE(TEST_INFO->status_orig_ == 0);
    TEST_INFO->test_read_shared(TEST_INFO->read_data_.data(), TEST_INFO->request_size_, MPI_CHAR);
    REQUIRE((size_t)TEST_INFO->size_read_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
  }

  SECTION("read at the end of existing file") {
    TEST_INFO->test_open(TEST_INFO->existing_file_, MPI_MODE_RDONLY, MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_seek(0, MPI_SEEK_END);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    MPI_Offset offset;
    MPI_File_get_position(TEST_INFO->fh_orig_, &offset);
    REQUIRE(offset == (long long)(TEST_INFO->request_size_ * TEST_INFO->num_iterations_));
    TEST_INFO->test_read(TEST_INFO->read_data_.data(), TEST_INFO->request_size_, MPI_CHAR);
    REQUIRE(TEST_INFO->size_read_orig_ == 0);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
  }

  SECTION("read_at from existing file") {
    TEST_INFO->test_open(TEST_INFO->existing_file_, MPI_MODE_RDONLY, MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_read_at(TEST_INFO->read_data_.data(), TEST_INFO->request_size_, MPI_CHAR,
                       TEST_INFO->rank_ * TEST_INFO->request_size_);
    REQUIRE((size_t)TEST_INFO->size_read_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
  }

  TEST_INFO->Posttest();
}

TEST_CASE("SingleReadCollective", "[process=" + std::to_string(TEST_INFO->comm_size_) +
                                      "]"
                                      "[operation=single_read]"
                                      "[synchronicity=sync]"
                                      "[coordination=collective]"
                                      "[request_size=type-fixed][repetition=1]"
                                      "[file=1]") {
  TEST_INFO->Pretest();
  SECTION("read from non-existing file") {
    TEST_INFO->test_open(TEST_INFO->shared_new_file_, MPI_MODE_RDONLY,
                    MPI_COMM_WORLD);
    REQUIRE(TEST_INFO->status_orig_ != MPI_SUCCESS);
  }

  SECTION("read from existing file") {
    TEST_INFO->test_open(TEST_INFO->shared_existing_file_, MPI_MODE_RDONLY,
                    MPI_COMM_WORLD);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_seek(TEST_INFO->rank_ * TEST_INFO->request_size_, MPI_SEEK_SET);
    REQUIRE(TEST_INFO->status_orig_ == 0);
    TEST_INFO->test_read_all(TEST_INFO->read_data_.data(), TEST_INFO->request_size_, MPI_CHAR);
    REQUIRE((size_t)TEST_INFO->size_read_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
  }

  SECTION("read from existing file using shared ptr") {
    TEST_INFO->test_open(TEST_INFO->shared_existing_file_, MPI_MODE_RDONLY,
                    MPI_COMM_WORLD);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_seek_shared(0, MPI_SEEK_SET);
    REQUIRE(TEST_INFO->status_orig_ == 0);
    TEST_INFO->test_read_shared(TEST_INFO->read_data_.data(), TEST_INFO->request_size_, MPI_CHAR);
    REQUIRE((size_t)TEST_INFO->size_read_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
  }

  SECTION("read_at_all from existing file") {
    TEST_INFO->test_open(TEST_INFO->existing_file_, MPI_MODE_RDONLY, MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_read_at_all(TEST_INFO->read_data_.data(), TEST_INFO->request_size_, MPI_CHAR,
                           TEST_INFO->rank_ * TEST_INFO->request_size_);
    REQUIRE((size_t)TEST_INFO->size_read_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
  }

  SECTION("read_ordered from existing file") {
    TEST_INFO->test_open(TEST_INFO->existing_file_, MPI_MODE_RDONLY,
                    MPI_COMM_WORLD);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_read_ordered(TEST_INFO->read_data_.data(), TEST_INFO->request_size_, MPI_CHAR);
    REQUIRE((size_t)TEST_INFO->size_read_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("SingleAsyncRead", "[process=" + std::to_string(TEST_INFO->comm_size_) +
                                 "]"
                                 "[operation=single_read]"
                                 "[synchronicity=async]"
                                 "[coordination=independent]"
                                 "[request_size=type-fixed][repetition=1]"
                                 "[file=1]") {
  TEST_INFO->Pretest();
  SECTION("read from non-existing file") {
    TEST_INFO->test_open(TEST_INFO->new_file_, MPI_MODE_RDONLY | MPI_MODE_EXCL,
                    MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ != MPI_SUCCESS);
  }

  SECTION("read from existing file") {
    TEST_INFO->test_open(TEST_INFO->existing_file_, MPI_MODE_RDONLY, MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_seek(TEST_INFO->rank_ * TEST_INFO->request_size_, MPI_SEEK_SET);
    REQUIRE(TEST_INFO->status_orig_ == 0);
    TEST_INFO->test_iread(TEST_INFO->read_data_.data(), TEST_INFO->request_size_, MPI_CHAR);
    REQUIRE((size_t)TEST_INFO->size_read_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
  }

  SECTION("read from existing file using shared ptr") {
    TEST_INFO->test_open(TEST_INFO->shared_existing_file_, MPI_MODE_RDONLY,
                    MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_seek_shared(0, MPI_SEEK_SET);
    REQUIRE(TEST_INFO->status_orig_ == 0);
    TEST_INFO->test_iread_shared(TEST_INFO->read_data_.data(), TEST_INFO->request_size_, MPI_CHAR);
    REQUIRE((size_t)TEST_INFO->size_read_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
  }

  SECTION("read at the end of existing file") {
    TEST_INFO->test_open(TEST_INFO->existing_file_, MPI_MODE_RDONLY, MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_seek(0, MPI_SEEK_END);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    MPI_Offset offset;
    MPI_File_get_position(TEST_INFO->fh_orig_, &offset);
    REQUIRE(offset == (long long)(TEST_INFO->request_size_ * TEST_INFO->num_iterations_));
    TEST_INFO->test_iread(TEST_INFO->read_data_.data(), TEST_INFO->request_size_, MPI_CHAR);
    REQUIRE(TEST_INFO->size_read_orig_ == 0);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
  }

  SECTION("read_at from existing file") {
    TEST_INFO->test_open(TEST_INFO->existing_file_, MPI_MODE_RDONLY, MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_iread_at(TEST_INFO->read_data_.data(), TEST_INFO->request_size_, MPI_CHAR,
                        TEST_INFO->rank_ * TEST_INFO->request_size_);
    REQUIRE((size_t)TEST_INFO->size_read_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
  }

  TEST_INFO->Posttest();
}

// TODO(chogan): This test fails sporadically.
// https://github.com/HDFGroup/hermes/issues/413
TEST_CASE("SingleAsyncReadCollective",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=single_read]"
              "[synchronicity=async]"
              "[coordination=collective]"
              "[request_size=type-fixed][repetition=1]"
              "[file=1]") {
  TEST_INFO->Pretest();
  SECTION("read from non-existing file") {
    TEST_INFO->test_open(TEST_INFO->shared_new_file_, MPI_MODE_RDONLY,
                    MPI_COMM_WORLD);
    REQUIRE(TEST_INFO->status_orig_ != MPI_SUCCESS);
  }

  SECTION("read from existing file") {
    TEST_INFO->test_open(TEST_INFO->shared_existing_file_, MPI_MODE_RDONLY,
                    MPI_COMM_WORLD);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_seek(TEST_INFO->rank_ * TEST_INFO->request_size_, MPI_SEEK_SET);
    REQUIRE(TEST_INFO->status_orig_ == 0);
    TEST_INFO->test_iread_all(TEST_INFO->read_data_.data(), TEST_INFO->request_size_, MPI_CHAR);
    REQUIRE((size_t)TEST_INFO->size_read_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
  }

  SECTION("read from existing file using shared ptr") {
    TEST_INFO->test_open(TEST_INFO->shared_existing_file_, MPI_MODE_RDONLY,
                    MPI_COMM_WORLD);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_seek_shared(0, MPI_SEEK_SET);
    REQUIRE(TEST_INFO->status_orig_ == 0);
    TEST_INFO->test_iread_shared(TEST_INFO->read_data_.data(), TEST_INFO->request_size_, MPI_CHAR);
    REQUIRE((size_t)TEST_INFO->size_read_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
  }

  SECTION("read_at_all from existing file") {
    TEST_INFO->test_open(TEST_INFO->existing_file_, MPI_MODE_RDONLY, MPI_COMM_SELF);
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
    TEST_INFO->test_iread_at_all(TEST_INFO->read_data_.data(), TEST_INFO->request_size_, MPI_CHAR,
                            TEST_INFO->rank_ * TEST_INFO->request_size_);
    REQUIRE((size_t)TEST_INFO->size_read_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_close();
    REQUIRE(TEST_INFO->status_orig_ == MPI_SUCCESS);
  }
  TEST_INFO->Posttest();
}
