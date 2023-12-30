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

#include "stdio_adapter_test.h"

TEST_CASE("Open", "[process=" + std::to_string(TESTER->comm_size_) +
    "]"
    "[operation=single_open]"
    "[repetition=1][file=1]") {
  TESTER->Pretest();
  SECTION("open non-existant file") {
    TESTER->test_fopen(TESTER->new_file_, "r");
    REQUIRE(TESTER->fh_orig_ == nullptr);
    TESTER->test_fopen(TESTER->new_file_, "r+");
    REQUIRE(TESTER->fh_orig_ == nullptr);
  }

  SECTION("truncate existing file and write-only") {
    TESTER->test_fopen(TESTER->existing_file_, "w");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  SECTION("truncate existing file and read/write") {
    TESTER->test_fopen(TESTER->existing_file_, "w+");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
  }

  SECTION("open existing file") {
    TESTER->test_fopen(TESTER->existing_file_, "r+");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
    TESTER->test_fopen(TESTER->existing_file_, "r");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
  }

  SECTION("append write existing file") {
    TESTER->test_fopen(TESTER->existing_file_, "a");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
  }

  SECTION("append write and read existing file") {
    TESTER->test_fopen(TESTER->existing_file_, "a+");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("SingleWrite", "[process=" + std::to_string(TESTER->comm_size_) +
    "]"
    "[operation=single_write]"
    "[request_size=type-fixed][repetition=1]"
    "[file=1]") {
  TESTER->Pretest();
  SECTION("write to existing file") {
    TESTER->test_fopen(TESTER->existing_file_, "r+");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    TESTER->test_fseek(0, SEEK_SET);
    REQUIRE(TESTER->status_orig_ == 0);
    TESTER->test_fwrite(TESTER->write_data_.data(), TESTER->request_size_);
    REQUIRE(TESTER->size_written_orig_ == TESTER->request_size_);
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
  }

  SECTION("write to new file") {
    TESTER->test_fopen(TESTER->new_file_, "w+");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    TESTER->test_fwrite(TESTER->write_data_.data(),
                        TESTER->request_size_);
    REQUIRE(TESTER->size_written_orig_ == TESTER->request_size_);
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
    REQUIRE(stdfs::file_size(TESTER->new_file_.hermes_) ==
            TESTER->size_written_orig_);
  }

  SECTION("write to existing file with truncate") {
    TESTER->test_fopen(TESTER->existing_file_, "w");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    TESTER->test_fwrite(TESTER->write_data_.data(), TESTER->request_size_);
    REQUIRE(TESTER->size_written_orig_ == TESTER->request_size_);
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
    REQUIRE(stdfs::file_size(TESTER->existing_file_.hermes_) ==
            TESTER->size_written_orig_);
  }

  SECTION("write to existing file at the end") {
    TESTER->test_fopen(TESTER->existing_file_, "r+");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    TESTER->test_fseek(0, SEEK_END);
    REQUIRE(TESTER->status_orig_ == 0);
    size_t offset = ftell(TESTER->fh_orig_);
    REQUIRE(offset == TESTER->request_size_ * TESTER->num_iterations_);
    TESTER->test_fwrite(TESTER->write_data_.data(), TESTER->request_size_);
    REQUIRE(TESTER->size_written_orig_ == TESTER->request_size_);
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
    REQUIRE(stdfs::file_size(TESTER->existing_file_.hermes_) ==
        TESTER->size_written_orig_ + offset);
  }

  SECTION("append to existing file") {
    auto existing_size = stdfs::file_size(TESTER->existing_file_.hermes_);
    TESTER->test_fopen(TESTER->existing_file_, "a+");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    TESTER->test_fwrite(TESTER->write_data_.data(), TESTER->request_size_);
    REQUIRE(TESTER->size_written_orig_ == TESTER->request_size_);
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
    REQUIRE(stdfs::file_size(TESTER->existing_file_.hermes_) ==
        existing_size + TESTER->size_written_orig_);
  }

  SECTION("append to new file") {
    TESTER->test_fopen(TESTER->new_file_, "w+");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    TESTER->test_fwrite(TESTER->write_data_.data(), TESTER->request_size_);
    REQUIRE(TESTER->size_written_orig_ == TESTER->request_size_);
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
    REQUIRE(stdfs::file_size(TESTER->new_file_.hermes_) ==
            TESTER->size_written_orig_);
  }
  TESTER->Posttest();
}

TEST_CASE("SingleRead", "[process=" + std::to_string(TESTER->comm_size_) +
    "]"
    "[operation=single_read]"
    "[request_size=type-fixed][repetition=1]"
    "[file=1]") {
  TESTER->Pretest();
  SECTION("read from non-existing file") {
    TESTER->test_fopen(TESTER->new_file_, "r");
    REQUIRE(TESTER->fh_orig_ == nullptr);
  }

  SECTION("read from existing file") {
    TESTER->test_fopen(TESTER->existing_file_, "r");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    size_t offset = ftell(TESTER->fh_orig_);
    REQUIRE(offset == 0);
    TESTER->test_fread(TESTER->read_data_.data(), TESTER->request_size_);
    REQUIRE(TESTER->size_read_orig_ == TESTER->request_size_);
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  SECTION("read at the end of existing file") {
    TESTER->test_fopen(TESTER->existing_file_, "r");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    TESTER->test_fseek(0, SEEK_END);
    REQUIRE(TESTER->status_orig_ == 0);
    size_t offset = ftell(TESTER->fh_orig_);
    REQUIRE(offset == TESTER->request_size_ * TESTER->num_iterations_);
    TESTER->test_fread(TESTER->read_data_.data(), TESTER->request_size_);
    REQUIRE(TESTER->size_read_orig_ == 0);
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedWriteSequential",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=sequential][file=1]") {
  TESTER->Pretest();
  SECTION("write to new file always at beginning") {
    TESTER->test_fopen(TESTER->new_file_, "w+");
    REQUIRE(TESTER->fh_orig_ != nullptr);

    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      TESTER->test_fseek(0, SEEK_SET);
      REQUIRE(TESTER->status_orig_ == 0);
      size_t offset = ftell(TESTER->fh_orig_);
      REQUIRE(offset == 0);
      TESTER->test_fwrite(TESTER->write_data_.data(), TESTER->request_size_);
      REQUIRE(TESTER->size_written_orig_ == TESTER->request_size_);
    }
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
    REQUIRE(stdfs::file_size(TESTER->new_file_.hermes_) ==
            TESTER->request_size_);
  }

  SECTION("write to new file sequentially") {
    TESTER->test_fopen(TESTER->new_file_, "w+");
    REQUIRE(TESTER->fh_orig_ != nullptr);

    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      TESTER->test_fwrite(TESTER->write_data_.data(), TESTER->request_size_);
      REQUIRE(TESTER->size_written_orig_ == TESTER->request_size_);
    }
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
    REQUIRE(stdfs::file_size(TESTER->new_file_.hermes_) ==
        TESTER->num_iterations_ * TESTER->request_size_);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedReadSequential",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=sequential][file=1]") {
  TESTER->Pretest();

  SECTION("read from existing file") {
    TESTER->test_fopen(TESTER->existing_file_, "r+");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    std::string data(TESTER->request_size_, '1');
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      TESTER->test_fread(data.data(), TESTER->request_size_);
      REQUIRE(TESTER->size_read_orig_ == TESTER->request_size_);
    }
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
  }

  SECTION("read from existing file always at start") {
    TESTER->test_fopen(TESTER->existing_file_, "w+");
    REQUIRE(TESTER->fh_orig_ != nullptr);

    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      TESTER->test_fseek(0, SEEK_SET);
      REQUIRE(TESTER->status_orig_ == 0);
      size_t offset = ftell(TESTER->fh_orig_);
      REQUIRE(offset == 0);
      TESTER->test_fwrite(TESTER->write_data_.data(),
                          TESTER->request_size_);
      REQUIRE(TESTER->size_written_orig_ == TESTER->request_size_);
    }
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedReadRandom", "[process=" +
    std::to_string(TESTER->comm_size_) +
    "]"
    "[operation=batched_read]"
    "[request_size=type-fixed]"
    "[repetition=" +
    std::to_string(TESTER->num_iterations_) +
    "][pattern=random][file=1]") {
  TESTER->Pretest();
  SECTION("read from existing file") {
    TESTER->test_fopen(TESTER->existing_file_, "r+");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    std::string data(TESTER->request_size_, '1');
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      auto offset =
          rand_r(&TESTER->offset_seed_) %
              (TESTER->total_size_ - TESTER->request_size_);
      TESTER->test_fseek(offset, SEEK_SET);
      REQUIRE(TESTER->status_orig_ == 0);
      TESTER->test_fread(data.data(), TESTER->request_size_);
      REQUIRE(TESTER->size_read_orig_ == TESTER->request_size_);
    }
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedUpdateRandom", "[process=" +
    std::to_string(TESTER->comm_size_) +
    "]"
    "[operation=batched_update]"
    "[request_size=type-fixed][repetition=" +
    std::to_string(TESTER->num_iterations_) +
    "]"
    "[pattern=random][file=1]") {
  TESTER->Pretest();
  SECTION("update into existing file") {
    TESTER->test_fopen(TESTER->existing_file_, "r+");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      auto offset =
          rand_r(&TESTER->offset_seed_) %
              (TESTER->total_size_ - TESTER->request_size_);
      TESTER->test_fseek(offset, SEEK_SET);
      REQUIRE(TESTER->status_orig_ == 0);
      TESTER->test_fwrite(TESTER->write_data_.data(),
                          TESTER->request_size_);
      REQUIRE(TESTER->size_written_orig_ == TESTER->request_size_);
      fflush(TESTER->fh_orig_);
    }
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedReadStrideFixed",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=stride_fixed][file=1]") {
  TESTER->Pretest();

  SECTION("read from existing file") {
    TESTER->test_fopen(TESTER->existing_file_, "r+");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    std::string data(TESTER->request_size_, '1');
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      auto offset = (i * TESTER->stride_size_) % TESTER->total_size_;
      TESTER->test_fseek(offset, SEEK_SET);
      REQUIRE(TESTER->status_orig_ == 0);
      TESTER->test_fread(data.data(), TESTER->request_size_);
      REQUIRE(TESTER->size_read_orig_ == TESTER->request_size_);
    }
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedUpdateStrideFixed",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_update]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=stride_fixed][file=1]") {
  TESTER->Pretest();

  SECTION("update from existing file") {
    TESTER->test_fopen(TESTER->existing_file_, "r+");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    std::string data(TESTER->request_size_, '1');
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      auto offset = (i * TESTER->stride_size_) % TESTER->total_size_;
      TESTER->test_fseek(offset, SEEK_SET);
      REQUIRE(TESTER->status_orig_ == 0);
      TESTER->test_fread(data.data(), TESTER->request_size_);
      REQUIRE(TESTER->size_read_orig_ == TESTER->request_size_);
    }
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedReadStrideDynamic",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=stride_dynamic][file=1]") {
  TESTER->Pretest();

  SECTION("read from existing file") {
    TESTER->test_fopen(TESTER->existing_file_, "r+");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    std::string data(TESTER->request_size_, '1');
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      auto offset = TESTER->GetRandomOffset(
          i, TESTER->offset_seed_, TESTER->stride_size_,
          TESTER->total_size_);
      TESTER->test_fseek(offset, SEEK_SET);
      REQUIRE(TESTER->status_orig_ == 0);
      TESTER->test_fread(data.data(), TESTER->request_size_);
      REQUIRE(TESTER->size_read_orig_ == TESTER->request_size_);
    }
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedUpdateStrideDynamic",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_update]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=stride_dynamic][file=1]") {
  TESTER->Pretest();
  SECTION("update from existing file") {
    TESTER->test_fopen(TESTER->existing_file_, "r+");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    std::string data(TESTER->request_size_, '1');
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      auto offset = TESTER->GetRandomOffset(i, TESTER->offset_seed_,
                                            TESTER->stride_size_,
                                            TESTER->total_size_);
      TESTER->test_fseek(offset, SEEK_SET);
      REQUIRE(TESTER->status_orig_ == 0);
      TESTER->test_fread(data.data(), TESTER->request_size_);
      REQUIRE(TESTER->size_read_orig_ == TESTER->request_size_);
    }
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedWriteRSVariable",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=type-variable][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=sequential][file=1]") {
  TESTER->Pretest();

  SECTION("write to new file always at the start") {
    TESTER->test_fopen(TESTER->new_file_, "w+");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    size_t biggest_written = 0;
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      TESTER->test_fseek(0, SEEK_SET);
      REQUIRE(TESTER->status_orig_ == 0);
      size_t offset = ftell(TESTER->fh_orig_);
      REQUIRE(offset == 0);
      size_t request_size =
          TESTER->request_size_ +
              (rand_r(&TESTER->offset_seed_) % TESTER->request_size_);
      std::string data(request_size, '1');
      TESTER->test_fwrite(data.c_str(), request_size);
      REQUIRE(TESTER->size_written_orig_ == request_size);
      if (biggest_written < request_size) biggest_written = request_size;
    }
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
    REQUIRE(stdfs::file_size(TESTER->new_file_.hermes_) == biggest_written);
  }

  SECTION("write to new file") {
    TESTER->test_fopen(TESTER->new_file_, "w+");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    size_t total_test_written = 0;
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t request_size =
          TESTER->request_size_ +
              (rand_r(&TESTER->offset_seed_) % TESTER->request_size_);
      std::string data(request_size, '1');
      TESTER->test_fwrite(data.c_str(), request_size);
      REQUIRE(TESTER->size_written_orig_ == request_size);
      total_test_written += TESTER->size_written_orig_;
    }
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
    REQUIRE(stdfs::file_size(TESTER->new_file_.hermes_) ==
        total_test_written);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedReadSequentialRSVariable",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=type-variable][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=sequential][file=1]") {
  TESTER->Pretest();
  SECTION("read from existing file") {
    TESTER->test_fopen(TESTER->existing_file_, "r");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    std::string data(TESTER->request_size_, '1');
    size_t current_offset = 0;
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t request_size = (TESTER->request_size_ +
          (rand_r(&TESTER->offset_seed_) % TESTER->request_size_)) %
          (TESTER->total_size_ - current_offset);
      std::string data(request_size, '1');
      TESTER->test_fread(data.data(), request_size);
      REQUIRE(TESTER->size_read_orig_ == request_size);
      current_offset += TESTER->size_read_orig_;
    }
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
  }

  SECTION("read from existing file always at start") {
    TESTER->test_fopen(TESTER->existing_file_, "r");
    REQUIRE(TESTER->fh_orig_ != nullptr);

    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      TESTER->test_fseek(0, SEEK_SET);
      REQUIRE(TESTER->status_orig_ == 0);
      size_t offset = ftell(TESTER->fh_orig_);
      REQUIRE(offset == 0);
      size_t request_size =
          TESTER->request_size_ +
              (rand_r(&TESTER->offset_seed_) % TESTER->request_size_);
      std::string data(request_size, '1');
      TESTER->test_fread(data.data(), request_size);
      REQUIRE(TESTER->size_read_orig_ == request_size);
    }
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedReadRandomRSVariable",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=type-variable]"
              "[repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "][pattern=random][file=1]") {
  TESTER->Pretest();

  SECTION("read from existing file") {
    TESTER->test_fopen(TESTER->existing_file_, "r+");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    std::string data(TESTER->request_size_, '1');
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      auto offset =
          rand_r(&TESTER->offset_seed_) %
              (TESTER->total_size_ - TESTER->request_size_);
      TESTER->test_fseek(offset, SEEK_SET);
      REQUIRE(TESTER->status_orig_ == 0);
      size_t request_size =
          (TESTER->request_size_ +
              (rand_r(&TESTER->rs_seed_) % TESTER->request_size_)) %
              (TESTER->total_size_ - offset);
      std::string data(request_size, '1');
      TESTER->test_fread(data.data(), request_size);
      REQUIRE(TESTER->size_read_orig_ == request_size);
    }
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedUpdateRandomRSVariable",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_update]"
              "[request_size=type-variable][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=random][file=1]") {
  TESTER->Pretest();

  SECTION("read from existing file") {
    TESTER->test_fopen(TESTER->existing_file_, "r+");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    std::string data(TESTER->request_size_, '1');
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      auto offset =
          rand_r(&TESTER->offset_seed_) %
              (TESTER->total_size_ - TESTER->request_size_);
      TESTER->test_fseek(offset, SEEK_SET);
      REQUIRE(TESTER->status_orig_ == 0);
      size_t request_size =
          TESTER->request_size_ +
              (rand_r(&TESTER->rs_seed_) % TESTER->request_size_);
      std::string data(request_size, '1');
      TESTER->test_fwrite(data.c_str(), request_size);
      REQUIRE(TESTER->size_written_orig_ == request_size);
    }
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedReadStrideFixedRSVariable",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=type-variable][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=stride_fixed][file=1]") {
  TESTER->Pretest();

  SECTION("read from existing file") {
    TESTER->test_fopen(TESTER->existing_file_, "r+");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      auto offset = (i * TESTER->stride_size_) % TESTER->total_size_;
      TESTER->test_fseek(offset, SEEK_SET);
      REQUIRE(TESTER->status_orig_ == 0);
      size_t request_size =
          (TESTER->request_size_ +
              (rand_r(&TESTER->rs_seed_) % TESTER->request_size_)) %
              (TESTER->total_size_ - offset);
      std::string data(request_size, '1');
      TESTER->test_fread(data.data(), request_size);
      REQUIRE(TESTER->size_read_orig_ == request_size);
    }
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedUpdateStrideFixedRSVariable",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_update]"
              "[request_size=type-variable][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=stride_fixed][file=1]") {
  TESTER->Pretest();

  SECTION("write to existing file") {
    TESTER->test_fopen(TESTER->existing_file_, "r+");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    std::string data(TESTER->request_size_, '1');
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      auto offset = (i * TESTER->stride_size_) % TESTER->total_size_;
      TESTER->test_fseek(offset, SEEK_SET);
      REQUIRE(TESTER->status_orig_ == 0);
      size_t request_size =
          TESTER->request_size_ +
              (rand_r(&TESTER->rs_seed_) % TESTER->request_size_);
      std::string data(request_size, '1');
      TESTER->test_fwrite(data.c_str(), request_size);
      REQUIRE(TESTER->size_written_orig_ == request_size);
    }
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedReadStrideDynamicRSVariable",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=type-variable][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=stride_dynamic][file=1]") {
  TESTER->Pretest();

  SECTION("read from existing file") {
    TESTER->test_fopen(TESTER->existing_file_, "r+");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      auto offset = TESTER->GetRandomOffset(i, TESTER->offset_seed_,
                                            TESTER->stride_size_,
                                            TESTER->total_size_);
      TESTER->test_fseek(offset, SEEK_SET);
      REQUIRE(TESTER->status_orig_ == 0);
      size_t request_size =
          TESTER->request_size_ +
              (rand_r(&TESTER->rs_seed_) % TESTER->request_size_);
      std::string data(request_size, '1');
      TESTER->test_fread(data.data(), request_size);
      REQUIRE(TESTER->size_read_orig_ == request_size);
    }
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedUpdateStrideDynamicRSVariable",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_update]"
              "[request_size=type-variable][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=stride_dynamic][file=1]") {
  TESTER->Pretest();
  SECTION("read from existing file") {
    TESTER->test_fopen(TESTER->existing_file_, "r+");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      auto offset = TESTER->GetRandomOffset(i, TESTER->offset_seed_,
                                            TESTER->stride_size_,
                                            TESTER->total_size_);
      TESTER->test_fseek(offset, SEEK_SET);
      REQUIRE(TESTER->status_orig_ == 0);
      size_t request_size =
          TESTER->request_size_ +
              (rand_r(&TESTER->rs_seed_) % TESTER->request_size_);
      std::string data(request_size, '1');
      TESTER->test_fwrite(data.c_str(), request_size);
      REQUIRE(TESTER->size_written_orig_ == request_size);
    }
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedReadStrideNegative",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=stride_negative][file=1]") {
  TESTER->Pretest();
  SECTION("read from existing file") {
    TESTER->test_fopen(TESTER->existing_file_, "r+");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    std::string data(TESTER->request_size_, '1');
    size_t prev_offset = TESTER->total_size_ + 1;
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      auto stride_offset = TESTER->total_size_ - i * TESTER->stride_size_;
      REQUIRE(prev_offset > stride_offset);
      prev_offset = stride_offset;
      auto offset = (stride_offset) %
          (TESTER->total_size_ - TESTER->request_size_);
      TESTER->test_fseek(offset, SEEK_SET);
      REQUIRE(TESTER->status_orig_ == 0);
      TESTER->test_fread(data.data(), TESTER->request_size_);
      REQUIRE(TESTER->size_read_orig_ == TESTER->request_size_);
    }
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedUpdateStrideNegative",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_update]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=stride_negative][file=1]") {
  TESTER->Pretest();
  SECTION("read from existing file") {
    TESTER->test_fopen(TESTER->existing_file_, "r+");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    std::string data(TESTER->request_size_, '1');
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      auto offset =
          TESTER->total_size_ - TESTER->request_size_ -
              ((i * TESTER->stride_size_) % TESTER->total_size_);
      TESTER->test_fseek(offset, SEEK_SET);
      REQUIRE(TESTER->status_orig_ == 0);
      TESTER->test_fread(data.data(), TESTER->request_size_);
      REQUIRE(TESTER->size_read_orig_ == TESTER->request_size_);
    }
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedReadStrideNegativeRSVariable",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=type-variable][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=stride_negative][file=1]") {
  TESTER->Pretest();

  SECTION("read from existing file") {
    TESTER->test_fopen(TESTER->existing_file_, "r+");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      auto offset = (TESTER->total_size_ - i * TESTER->stride_size_) %
          (TESTER->total_size_ - 2 * TESTER->request_size_);
      TESTER->test_fseek(offset, SEEK_SET);
      REQUIRE(TESTER->status_orig_ == 0);
      size_t request_size =
          (TESTER->request_size_ + (rand_r(&TESTER->rs_seed_) %
              TESTER->request_size_)) %
              (TESTER->total_size_ - offset);
      std::string data(request_size, '1');
      TESTER->test_fread(data.data(), request_size);
      REQUIRE(TESTER->size_read_orig_ == request_size);
    }
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedUpdateStrideNegativeRSVariable",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_update]"
              "[request_size=type-variable][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=stride_negative][file=1]") {
  TESTER->Pretest();

  SECTION("write to existing file") {
    TESTER->test_fopen(TESTER->existing_file_, "r+");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    std::string data(TESTER->request_size_, '1');
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      auto offset =
          TESTER->total_size_ - ((i * TESTER->stride_size_) %
              TESTER->total_size_);
      TESTER->test_fseek(offset, SEEK_SET);
      REQUIRE(TESTER->status_orig_ == 0);
      size_t request_size =
          TESTER->request_size_ + (rand_r(&TESTER->rs_seed_) %
              TESTER->request_size_);
      std::string data(request_size, '1');
      TESTER->test_fwrite(data.c_str(), request_size);
      REQUIRE(TESTER->size_written_orig_ == request_size);
    }
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedReadStride2D", "[process=" +
    std::to_string(TESTER->comm_size_) +
    "]"
    "[operation=batched_read]"
    "[request_size=type-fixed][repetition=" +
    std::to_string(TESTER->num_iterations_) +
    "]"
    "[pattern=stride_2d][file=1]") {
  TESTER->Pretest();
  size_t rows = sqrt(TESTER->total_size_);
  size_t cols = rows;
  REQUIRE(rows * cols == TESTER->total_size_);
  size_t cell_size = 128;
  size_t cell_stride = rows * cols / cell_size / TESTER->num_iterations_;
  SECTION("read from existing file") {
    TESTER->test_fopen(TESTER->existing_file_, "r+");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    std::string data(TESTER->request_size_, '1');
    size_t prev_cell_col = 0, prev_cell_row = 0;
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t current_cell_col = (prev_cell_col + cell_stride) % cols;
      size_t current_cell_row = prev_cell_col + cell_stride > cols
                                ? prev_cell_row + 1
                                : prev_cell_row;
      prev_cell_row = current_cell_row;
      auto offset = (current_cell_col * cell_stride + prev_cell_row * cols) %
          (TESTER->total_size_ - TESTER->request_size_);
      TESTER->test_fseek(offset, SEEK_SET);
      REQUIRE(TESTER->status_orig_ == 0);
      TESTER->test_fread(data.data(), TESTER->request_size_);
      REQUIRE(TESTER->size_read_orig_ == TESTER->request_size_);
    }
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedUpdateStride2D",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_update]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=stride_2d][file=1]") {
  TESTER->Pretest();

  size_t rows = sqrt(TESTER->total_size_);
  size_t cols = rows;
  REQUIRE(rows * cols == TESTER->total_size_);
  size_t cell_size = 128;
  size_t cell_stride = rows * cols / cell_size / TESTER->num_iterations_;
  SECTION("read from existing file") {
    TESTER->test_fopen(TESTER->existing_file_, "r+");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    std::string data(TESTER->request_size_, '1');
    size_t prev_cell_col = 0, prev_cell_row = 0;
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t current_cell_col = (prev_cell_col + cell_stride) % cols;
      size_t current_cell_row = prev_cell_col + cell_stride > cols
                                ? prev_cell_row + 1
                                : prev_cell_row;
      prev_cell_row = current_cell_row;
      auto offset = (current_cell_col * cell_stride + prev_cell_row * cols) %
          (TESTER->total_size_ - TESTER->request_size_);
      TESTER->test_fseek(offset, SEEK_SET);
      REQUIRE(TESTER->status_orig_ == 0);
      TESTER->test_fread(data.data(), TESTER->request_size_);
      REQUIRE(TESTER->size_read_orig_ == TESTER->request_size_);
    }
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedReadStride2DRSVariable",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              ""
              "[operation=batched_read]"
              "[request_size=type-variable][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=stride_2d][file=1]") {
  TESTER->Pretest();
  size_t rows = sqrt(TESTER->total_size_);
  size_t cols = rows;
  REQUIRE(rows * cols == TESTER->total_size_);
  size_t cell_size = 128;
  size_t cell_stride = rows * cols / cell_size / TESTER->num_iterations_;
  SECTION("read from existing file") {
    TESTER->test_fopen(TESTER->existing_file_, "r+");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    size_t prev_cell_col = 0, prev_cell_row = 0;
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t current_cell_col = (prev_cell_col + cell_stride) % cols;
      size_t current_cell_row = prev_cell_col + cell_stride > cols
                                ? prev_cell_row + 1
                                : prev_cell_row;
      prev_cell_row = current_cell_row;
      auto offset = (current_cell_col * cell_stride + prev_cell_row * cols) %
          (TESTER->total_size_ - 2 * TESTER->request_size_);
      TESTER->test_fseek(offset, SEEK_SET);
      REQUIRE(TESTER->status_orig_ == 0);
      size_t request_size =
          (TESTER->request_size_ + (rand_r(&TESTER->rs_seed_) %
              TESTER->request_size_)) %
              (TESTER->total_size_ - offset);
      std::string data(request_size, '1');
      TESTER->test_fread(data.data(), request_size);
      REQUIRE(TESTER->size_read_orig_ == request_size);
    }
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedUpdateStride2DRSVariable",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_update]"
              "[request_size=type-variable][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=stride_2d][file=1]") {
  TESTER->Pretest();
  size_t rows = sqrt(TESTER->total_size_);
  size_t cols = rows;
  REQUIRE(rows * cols == TESTER->total_size_);
  size_t cell_size = 128;
  size_t cell_stride = rows * cols / cell_size / TESTER->num_iterations_;
  SECTION("write to existing file") {
    TESTER->test_fopen(TESTER->existing_file_, "r+");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    std::string data(TESTER->request_size_, '1');
    size_t prev_cell_col = 0, prev_cell_row = 0;
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t current_cell_col = (prev_cell_col + cell_stride) % cols;
      size_t current_cell_row = prev_cell_col + cell_stride > cols
                                ? prev_cell_row + 1
                                : prev_cell_row;
      prev_cell_row = current_cell_row;
      auto offset = (current_cell_col * cell_stride + prev_cell_row * cols) %
          (TESTER->total_size_ - 2 * TESTER->request_size_);
      TESTER->test_fseek(offset, SEEK_SET);
      REQUIRE(TESTER->status_orig_ == 0);
      size_t request_size =
          TESTER->request_size_ + (rand_r(&TESTER->rs_seed_) %
              TESTER->request_size_);
      std::string data(request_size, '1');
      TESTER->test_fwrite(data.c_str(), request_size);
      REQUIRE(TESTER->size_written_orig_ == request_size);
    }
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

/**
 * Temporal Fixed
 */

TEST_CASE("BatchedWriteTemporalFixed",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=sequential][file=1][temporal=fixed]") {
  TESTER->Pretest();

  SECTION("write to existing file always at start") {
    TESTER->test_fopen(TESTER->existing_file_, "w+");
    REQUIRE(TESTER->fh_orig_ != nullptr);

    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      usleep(TESTER->temporal_interval_ms_ * 1000);
      TESTER->test_fseek(0, SEEK_SET);
      REQUIRE(TESTER->status_orig_ == 0);
      size_t offset = ftell(TESTER->fh_orig_);
      REQUIRE(offset == 0);
      TESTER->test_fwrite(TESTER->write_data_.data(),
                          TESTER->request_size_);
      REQUIRE(TESTER->size_written_orig_ == TESTER->request_size_);
    }
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
    REQUIRE(stdfs::file_size(TESTER->existing_file_.hermes_) ==
        TESTER->request_size_);
  }

  SECTION("write to new file always at start") {
    TESTER->test_fopen(TESTER->new_file_, "w+");
    REQUIRE(TESTER->fh_orig_ != nullptr);

    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      usleep(TESTER->temporal_interval_ms_ * 1000);
      TESTER->test_fseek(0, SEEK_SET);
      REQUIRE(TESTER->status_orig_ == 0);
      size_t offset = ftell(TESTER->fh_orig_);
      REQUIRE(offset == 0);
      TESTER->test_fwrite(TESTER->write_data_.data(),
                          TESTER->request_size_);
      REQUIRE(TESTER->size_written_orig_ == TESTER->request_size_);
    }
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
    REQUIRE(stdfs::file_size(TESTER->new_file_.hermes_) ==
        TESTER->request_size_);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedReadSequentialTemporalFixed",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=sequential][file=1][temporal=fixed]") {
  TESTER->Pretest();

  SECTION("read from existing file") {
    TESTER->test_fopen(TESTER->existing_file_, "r+");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    std::string data(TESTER->request_size_, '1');
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      usleep(TESTER->temporal_interval_ms_ * 1000);
      TESTER->test_fread(data.data(), TESTER->request_size_);
      REQUIRE(TESTER->size_read_orig_ == TESTER->request_size_);
    }
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
  }

  SECTION("read from existing file always at start") {
    TESTER->test_fopen(TESTER->existing_file_, "r+");
    REQUIRE(TESTER->fh_orig_ != nullptr);

    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      usleep(TESTER->temporal_interval_ms_ * 1000);
      TESTER->test_fseek(0, SEEK_SET);
      REQUIRE(TESTER->status_orig_ == 0);
      size_t offset = ftell(TESTER->fh_orig_);
      REQUIRE(offset == 0);
      TESTER->test_fread(TESTER->read_data_.data(),
                         TESTER->request_size_);
      REQUIRE(TESTER->size_read_orig_ == TESTER->request_size_);
    }
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedWriteTemporalVariable",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=sequential][file=1][temporal=variable]") {
  TESTER->Pretest();

  SECTION("write to existing file always at start") {
    TESTER->test_fopen(TESTER->existing_file_, "w+");
    REQUIRE(TESTER->fh_orig_ != nullptr);

    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      TESTER->temporal_interval_ms_ =
          rand_r(&TESTER->temporal_interval_seed_) %
              TESTER->temporal_interval_ms_ + 1;
      usleep(TESTER->temporal_interval_ms_ * 1000);
      TESTER->test_fseek(0, SEEK_SET);
      REQUIRE(TESTER->status_orig_ == 0);
      size_t offset = ftell(TESTER->fh_orig_);
      REQUIRE(offset == 0);
      TESTER->test_fwrite(TESTER->write_data_.data(),
                          TESTER->request_size_);
      REQUIRE(TESTER->size_written_orig_ == TESTER->request_size_);
    }
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
    REQUIRE(stdfs::file_size(TESTER->existing_file_.hermes_) ==
        TESTER->request_size_);
  }

  SECTION("write to new file") {
    TESTER->test_fopen(TESTER->new_file_, "w+");
    REQUIRE(TESTER->fh_orig_ != nullptr);

    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      TESTER->temporal_interval_ms_ =
          rand_r(&TESTER->temporal_interval_seed_) %
              TESTER->temporal_interval_ms_ + 1;
      usleep(TESTER->temporal_interval_ms_ * 1000);
      TESTER->test_fwrite(TESTER->write_data_.data(),
                          TESTER->request_size_);
      REQUIRE(TESTER->size_written_orig_ == TESTER->request_size_);
    }
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
    REQUIRE(stdfs::file_size(TESTER->new_file_.hermes_) ==
        TESTER->num_iterations_ * TESTER->request_size_);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedReadSequentialTemporalVariable",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=sequential][file=1][temporal=variable]") {
  TESTER->Pretest();

  SECTION("read from existing file") {
    TESTER->test_fopen(TESTER->existing_file_, "r+");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    std::string data(TESTER->request_size_, '1');
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      TESTER->temporal_interval_ms_ =
          rand_r(&TESTER->temporal_interval_seed_) %
              TESTER->temporal_interval_ms_ + 1;
      usleep(TESTER->temporal_interval_ms_ * 1000);
      TESTER->test_fread(data.data(), TESTER->request_size_);
      REQUIRE(TESTER->size_read_orig_ == TESTER->request_size_);
    }
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
  }

  SECTION("read from existing file always at start") {
    TESTER->test_fopen(TESTER->existing_file_, "r+");
    REQUIRE(TESTER->fh_orig_ != nullptr);

    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      TESTER->temporal_interval_ms_ =
          rand_r(&TESTER->temporal_interval_seed_) %
              TESTER->temporal_interval_ms_ + 1;
      usleep(TESTER->temporal_interval_ms_ * 1000);
      TESTER->test_fseek(0, SEEK_SET);
      REQUIRE(TESTER->status_orig_ == 0);
      size_t offset = ftell(TESTER->fh_orig_);
      REQUIRE(offset == 0);
      TESTER->test_fread(TESTER->read_data_.data(),
                         TESTER->request_size_);
      REQUIRE(TESTER->size_read_orig_ == TESTER->request_size_);
    }
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedMixedSequential",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_mixed]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=sequential][file=1]") {
  TESTER->Pretest();
  SECTION("read after write on new file") {
    TESTER->test_fopen(TESTER->new_file_, "w+");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    size_t last_offset = 0;
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      TESTER->test_fwrite(TESTER->write_data_.data(),
                          TESTER->request_size_);
      REQUIRE(TESTER->size_written_orig_ == TESTER->request_size_);
      TESTER->test_fseek(last_offset, SEEK_SET);
      REQUIRE(TESTER->status_orig_ == 0);
      TESTER->test_fread(TESTER->read_data_.data(),
                         TESTER->request_size_);
      REQUIRE(TESTER->size_read_orig_ == TESTER->request_size_);
      last_offset += TESTER->request_size_;
    }
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
  }

  SECTION("write and read alternative existing file") {
    TESTER->test_fopen(TESTER->existing_file_, "r+");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      if (i % 2 == 0) {
        TESTER->test_fwrite(TESTER->write_data_.data(),
                            TESTER->request_size_);
        REQUIRE(TESTER->size_written_orig_ == TESTER->request_size_);
      } else {
        TESTER->test_fread(TESTER->read_data_.data(),
                           TESTER->request_size_);
        REQUIRE(TESTER->size_read_orig_ == TESTER->request_size_);
      }
    }
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  SECTION("update after read existing file") {
    TESTER->test_fopen(TESTER->existing_file_, "r+");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    size_t last_offset = 0;
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      TESTER->test_fread(TESTER->read_data_.data(),
                         TESTER->request_size_);
      REQUIRE(TESTER->size_read_orig_ == TESTER->request_size_);
      TESTER->test_fseek(last_offset, SEEK_SET);
      REQUIRE(TESTER->status_orig_ == 0);
      TESTER->test_fwrite(TESTER->write_data_.data(),
                          TESTER->request_size_);
      REQUIRE(TESTER->size_written_orig_ == TESTER->request_size_);
      last_offset += TESTER->request_size_;
    }

    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  SECTION("read all after write all on new file in single open") {
    TESTER->test_fopen(TESTER->new_file_, "w+");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      TESTER->test_fwrite(TESTER->write_data_.data(),
                          TESTER->request_size_);
      REQUIRE(TESTER->size_written_orig_ == TESTER->request_size_);
    }
    TESTER->test_fseek(0, SEEK_SET);
    REQUIRE(TESTER->status_orig_ == 0);
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      TESTER->test_fread(TESTER->read_data_.data(),
                         TESTER->request_size_);
      REQUIRE(TESTER->size_read_orig_ == TESTER->request_size_);
    }
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  SECTION("read all after write all on new file in different open") {
    TESTER->test_fopen(TESTER->new_file_, "w+");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      TESTER->test_fwrite(TESTER->write_data_.data(),
                          TESTER->request_size_);
      REQUIRE(TESTER->size_written_orig_ == TESTER->request_size_);
    }
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
    TESTER->test_fopen(TESTER->new_file_, "r");
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      TESTER->test_fread(TESTER->read_data_.data(),
                         TESTER->request_size_);
      REQUIRE(TESTER->size_read_orig_ == TESTER->request_size_);
    }
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("SingleMixed", "[process=" + std::to_string(TESTER->comm_size_) +
    "][operation=single_mixed]"
    "[request_size=type-fixed][repetition=1]"
    "[file=1]") {
  TESTER->Pretest();
  SECTION("read after write from new file") {
    TESTER->test_fopen(TESTER->new_file_, "w+");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    size_t offset = ftell(TESTER->fh_orig_);
    REQUIRE(offset == 0);
    TESTER->test_fwrite(TESTER->write_data_.data(),
                        TESTER->request_size_);
    REQUIRE(TESTER->size_written_orig_ == TESTER->request_size_);
    TESTER->test_fseek(0, SEEK_SET);
    REQUIRE(TESTER->status_orig_ == 0);
    TESTER->test_fread(TESTER->read_data_.data(),
                       TESTER->request_size_);
    REQUIRE(TESTER->size_read_orig_ == TESTER->request_size_);
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  SECTION("update after read from existing file") {
    TESTER->test_fopen(TESTER->existing_file_, "r+");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    size_t offset = ftell(TESTER->fh_orig_);
    REQUIRE(offset == 0);
    TESTER->test_fread(TESTER->read_data_.data(),
                       TESTER->request_size_);
    REQUIRE(TESTER->size_read_orig_ == TESTER->request_size_);
    TESTER->test_fseek(0, SEEK_SET);
    REQUIRE(TESTER->status_orig_ == 0);
    TESTER->test_fwrite(TESTER->write_data_.data(),
                        TESTER->request_size_);
    REQUIRE(TESTER->size_written_orig_ == TESTER->request_size_);

    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  SECTION("read after write from new file different opens") {
    TESTER->test_fopen(TESTER->new_file_, "w+");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    size_t offset = ftell(TESTER->fh_orig_);
    REQUIRE(offset == 0);
    TESTER->test_fwrite(TESTER->write_data_.data(),
                        TESTER->request_size_);
    REQUIRE(TESTER->size_written_orig_ == TESTER->request_size_);
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
    TESTER->test_fopen(TESTER->new_file_, "r+");
    TESTER->test_fread(TESTER->read_data_.data(),
                       TESTER->request_size_);
    REQUIRE(TESTER->size_read_orig_ == TESTER->request_size_);
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}
