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

TEST_CASE("Open", "[process=" + std::to_string(TEST_INFO->comm_size_) +
    "]"
    "[operation=single_open]"
    "[repetition=1][file=1]") {
  TEST_INFO->Pretest();
  SECTION("open non-existant file") {
    TEST_INFO->test_fopen(TEST_INFO->new_file_, "r");
    REQUIRE(TEST_INFO->fh_orig_ == nullptr);
    TEST_INFO->test_fopen(TEST_INFO->new_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ == nullptr);
  }

  SECTION("truncate existing file and write-only") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "w");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  SECTION("truncate existing file and read/write") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "w+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }

  SECTION("open existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }

  SECTION("append write existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "a");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }

  SECTION("append write and read existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "a+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("SingleWrite", "[process=" + std::to_string(TEST_INFO->comm_size_) +
    "]"
    "[operation=single_write]"
    "[request_size=type-fixed][repetition=1]"
    "[file=1]") {
  TEST_INFO->Pretest();
  SECTION("write to existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    TEST_INFO->test_fseek(0, SEEK_SET);
    REQUIRE(TEST_INFO->status_orig_ == 0);
    TEST_INFO->test_fwrite(TEST_INFO->write_data_.data(), TEST_INFO->request_size_);
    REQUIRE(TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }

  SECTION("write to new  file") {
    TEST_INFO->test_fopen(TEST_INFO->new_file_, "w+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    TEST_INFO->test_fwrite(TEST_INFO->write_data_.data(), TEST_INFO->request_size_);
    REQUIRE(TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
    REQUIRE(stdfs::file_size(TEST_INFO->new_file_.hermes_) == TEST_INFO->size_written_orig_);
  }

  SECTION("write to existing file with truncate") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "w");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    TEST_INFO->test_fwrite(TEST_INFO->write_data_.data(), TEST_INFO->request_size_);
    REQUIRE(TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
    REQUIRE(stdfs::file_size(TEST_INFO->existing_file_.hermes_) == TEST_INFO->size_written_orig_);
  }

  SECTION("write to existing file at the end") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    TEST_INFO->test_fseek(0, SEEK_END);
    REQUIRE(TEST_INFO->status_orig_ == 0);
    size_t offset = ftell(TEST_INFO->fh_orig_);
    REQUIRE(offset == TEST_INFO->request_size_ * TEST_INFO->num_iterations_);
    TEST_INFO->test_fwrite(TEST_INFO->write_data_.data(), TEST_INFO->request_size_);
    REQUIRE(TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
    REQUIRE(stdfs::file_size(TEST_INFO->existing_file_.hermes_) ==
        TEST_INFO->size_written_orig_ + offset);
  }

  SECTION("append to existing file") {
    auto existing_size = stdfs::file_size(TEST_INFO->existing_file_.hermes_);
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "a+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    TEST_INFO->test_fwrite(TEST_INFO->write_data_.data(), TEST_INFO->request_size_);
    REQUIRE(TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
    REQUIRE(stdfs::file_size(TEST_INFO->existing_file_.hermes_) ==
        existing_size + TEST_INFO->size_written_orig_);
  }

  SECTION("append to new file") {
    TEST_INFO->test_fopen(TEST_INFO->new_file_, "w+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    TEST_INFO->test_fwrite(TEST_INFO->write_data_.data(), TEST_INFO->request_size_);
    REQUIRE(TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
    REQUIRE(stdfs::file_size(TEST_INFO->new_file_.hermes_) == TEST_INFO->size_written_orig_);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("SingleRead", "[process=" + std::to_string(TEST_INFO->comm_size_) +
    "]"
    "[operation=single_read]"
    "[request_size=type-fixed][repetition=1]"
    "[file=1]") {
  TEST_INFO->Pretest();
  SECTION("read from non-existing file") {
    TEST_INFO->test_fopen(TEST_INFO->new_file_, "r");
    REQUIRE(TEST_INFO->fh_orig_ == nullptr);
  }

  SECTION("read from existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    size_t offset = ftell(TEST_INFO->fh_orig_);
    REQUIRE(offset == 0);
    TEST_INFO->test_fread(TEST_INFO->read_data_.data(), TEST_INFO->request_size_);
    REQUIRE(TEST_INFO->size_read_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  SECTION("read at the end of existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    TEST_INFO->test_fseek(0, SEEK_END);
    REQUIRE(TEST_INFO->status_orig_ == 0);
    size_t offset = ftell(TEST_INFO->fh_orig_);
    REQUIRE(offset == TEST_INFO->request_size_ * TEST_INFO->num_iterations_);
    TEST_INFO->test_fread(TEST_INFO->read_data_.data(), TEST_INFO->request_size_);
    REQUIRE(TEST_INFO->size_read_orig_ == 0);
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedWriteSequential",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=sequential][file=1]") {
  TEST_INFO->Pretest();
  SECTION("write to new file always at beginning") {
    TEST_INFO->test_fopen(TEST_INFO->new_file_, "w+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);

    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      TEST_INFO->test_fseek(0, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t offset = ftell(TEST_INFO->fh_orig_);
      REQUIRE(offset == 0);
      TEST_INFO->test_fwrite(TEST_INFO->write_data_.data(), TEST_INFO->request_size_);
      REQUIRE(TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
    REQUIRE(stdfs::file_size(TEST_INFO->new_file_.hermes_) == TEST_INFO->request_size_);
  }

  SECTION("write to new file sequentially") {
    TEST_INFO->test_fopen(TEST_INFO->new_file_, "w+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);

    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      TEST_INFO->test_fwrite(TEST_INFO->write_data_.data(), TEST_INFO->request_size_);
      REQUIRE(TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
    REQUIRE(stdfs::file_size(TEST_INFO->new_file_.hermes_) ==
        TEST_INFO->num_iterations_ * TEST_INFO->request_size_);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedReadSequential",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=sequential][file=1]") {
  TEST_INFO->Pretest();

  SECTION("read from existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    std::string data(TEST_INFO->request_size_, '1');
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      TEST_INFO->test_fread(data.data(), TEST_INFO->request_size_);
      REQUIRE(TEST_INFO->size_read_orig_ == TEST_INFO->request_size_);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }

  SECTION("read from existing file always at start") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "w+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);

    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      TEST_INFO->test_fseek(0, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t offset = ftell(TEST_INFO->fh_orig_);
      REQUIRE(offset == 0);
      TEST_INFO->test_fwrite(TEST_INFO->write_data_.data(), TEST_INFO->request_size_);
      REQUIRE(TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedReadRandom", "[process=" + std::to_string(TEST_INFO->comm_size_) +
    "]"
    "[operation=batched_read]"
    "[request_size=type-fixed]"
    "[repetition=" +
    std::to_string(TEST_INFO->num_iterations_) +
    "][pattern=random][file=1]") {
  TEST_INFO->Pretest();
  SECTION("read from existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    std::string data(TEST_INFO->request_size_, '1');
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      auto offset =
          rand_r(&TEST_INFO->offset_seed_) % (TEST_INFO->total_size_ - TEST_INFO->request_size_);
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      TEST_INFO->test_fread(data.data(), TEST_INFO->request_size_);
      REQUIRE(TEST_INFO->size_read_orig_ == TEST_INFO->request_size_);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedUpdateRandom", "[process=" + std::to_string(TEST_INFO->comm_size_) +
    "]"
    "[operation=batched_update]"
    "[request_size=type-fixed][repetition=" +
    std::to_string(TEST_INFO->num_iterations_) +
    "]"
    "[pattern=random][file=1]") {
  TEST_INFO->Pretest();
  SECTION("update into existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      auto offset =
          rand_r(&TEST_INFO->offset_seed_) % (TEST_INFO->total_size_ - TEST_INFO->request_size_);
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      TEST_INFO->test_fwrite(TEST_INFO->write_data_.data(), TEST_INFO->request_size_);
      REQUIRE(TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
      fflush(TEST_INFO->fh_orig_);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedReadStrideFixed",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=stride_fixed][file=1]") {
  TEST_INFO->Pretest();

  SECTION("read from existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    std::string data(TEST_INFO->request_size_, '1');
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      auto offset = (i * TEST_INFO->stride_size_) % TEST_INFO->total_size_;
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      TEST_INFO->test_fread(data.data(), TEST_INFO->request_size_);
      REQUIRE(TEST_INFO->size_read_orig_ == TEST_INFO->request_size_);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedUpdateStrideFixed",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_update]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=stride_fixed][file=1]") {
  TEST_INFO->Pretest();

  SECTION("update from existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    std::string data(TEST_INFO->request_size_, '1');
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      auto offset = (i * TEST_INFO->stride_size_) % TEST_INFO->total_size_;
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      TEST_INFO->test_fread(data.data(), TEST_INFO->request_size_);
      REQUIRE(TEST_INFO->size_read_orig_ == TEST_INFO->request_size_);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedReadStrideDynamic",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=stride_dynamic][file=1]") {
  TEST_INFO->Pretest();

  SECTION("read from existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    std::string data(TEST_INFO->request_size_, '1');
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      auto offset = TEST_INFO->GetRandomOffset(
          i, TEST_INFO->offset_seed_, TEST_INFO->stride_size_,
          TEST_INFO->total_size_);
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      TEST_INFO->test_fread(data.data(), TEST_INFO->request_size_);
      REQUIRE(TEST_INFO->size_read_orig_ == TEST_INFO->request_size_);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedUpdateStrideDynamic",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_update]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=stride_dynamic][file=1]") {
  TEST_INFO->Pretest();
  SECTION("update from existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    std::string data(TEST_INFO->request_size_, '1');
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      auto offset = TEST_INFO->GetRandomOffset(i, TEST_INFO->offset_seed_, TEST_INFO->stride_size_,
                                               TEST_INFO->total_size_);
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      TEST_INFO->test_fread(data.data(), TEST_INFO->request_size_);
      REQUIRE(TEST_INFO->size_read_orig_ == TEST_INFO->request_size_);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedWriteRSVariable",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=type-variable][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=sequential][file=1]") {
  TEST_INFO->Pretest();

  SECTION("write to new file always at the start") {
    TEST_INFO->test_fopen(TEST_INFO->new_file_, "w+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    size_t biggest_written = 0;
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      TEST_INFO->test_fseek(0, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t offset = ftell(TEST_INFO->fh_orig_);
      REQUIRE(offset == 0);
      size_t request_size =
          TEST_INFO->request_size_ + (rand_r(&TEST_INFO->offset_seed_) % TEST_INFO->request_size_);
      std::string data(request_size, '1');
      TEST_INFO->test_fwrite(data.c_str(), request_size);
      REQUIRE(TEST_INFO->size_written_orig_ == request_size);
      if (biggest_written < request_size) biggest_written = request_size;
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
    REQUIRE(stdfs::file_size(TEST_INFO->new_file_.hermes_) == biggest_written);
  }

  SECTION("write to new file") {
    TEST_INFO->test_fopen(TEST_INFO->new_file_, "w+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    size_t total_test_written = 0;
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      size_t request_size =
          TEST_INFO->request_size_ + (rand_r(&TEST_INFO->offset_seed_) % TEST_INFO->request_size_);
      std::string data(request_size, '1');
      TEST_INFO->test_fwrite(data.c_str(), request_size);
      REQUIRE(TEST_INFO->size_written_orig_ == request_size);
      total_test_written += TEST_INFO->size_written_orig_;
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
    REQUIRE(stdfs::file_size(TEST_INFO->new_file_.hermes_) == total_test_written);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedReadSequentialRSVariable",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=type-variable][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=sequential][file=1]") {
  TEST_INFO->Pretest();
  SECTION("read from existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    std::string data(TEST_INFO->request_size_, '1');
    size_t current_offset = 0;
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      size_t request_size = (TEST_INFO->request_size_ +
          (rand_r(&TEST_INFO->offset_seed_) % TEST_INFO->request_size_)) %
          (TEST_INFO->total_size_ - current_offset);
      std::string data(request_size, '1');
      TEST_INFO->test_fread(data.data(), request_size);
      REQUIRE(TEST_INFO->size_read_orig_ == request_size);
      current_offset += TEST_INFO->size_read_orig_;
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }

  SECTION("read from existing file always at start") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);

    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      TEST_INFO->test_fseek(0, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t offset = ftell(TEST_INFO->fh_orig_);
      REQUIRE(offset == 0);
      size_t request_size =
          TEST_INFO->request_size_ + (rand_r(&TEST_INFO->offset_seed_) % TEST_INFO->request_size_);
      std::string data(request_size, '1');
      TEST_INFO->test_fread(data.data(), request_size);
      REQUIRE(TEST_INFO->size_read_orig_ == request_size);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedReadRandomRSVariable",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=type-variable]"
              "[repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "][pattern=random][file=1]") {
  TEST_INFO->Pretest();

  SECTION("read from existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    std::string data(TEST_INFO->request_size_, '1');
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      auto offset =
          rand_r(&TEST_INFO->offset_seed_) % (TEST_INFO->total_size_ - TEST_INFO->request_size_);
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t request_size =
          (TEST_INFO->request_size_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->request_size_)) %
              (TEST_INFO->total_size_ - offset);
      std::string data(request_size, '1');
      TEST_INFO->test_fread(data.data(), request_size);
      REQUIRE(TEST_INFO->size_read_orig_ == request_size);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedUpdateRandomRSVariable",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_update]"
              "[request_size=type-variable][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=random][file=1]") {
  TEST_INFO->Pretest();

  SECTION("read from existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    std::string data(TEST_INFO->request_size_, '1');
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      auto offset =
          rand_r(&TEST_INFO->offset_seed_) % (TEST_INFO->total_size_ - TEST_INFO->request_size_);
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t request_size =
          TEST_INFO->request_size_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->request_size_);
      std::string data(request_size, '1');
      TEST_INFO->test_fwrite(data.c_str(), request_size);
      REQUIRE(TEST_INFO->size_written_orig_ == request_size);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedReadStrideFixedRSVariable",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=type-variable][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=stride_fixed][file=1]") {
  TEST_INFO->Pretest();

  SECTION("read from existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      auto offset = (i * TEST_INFO->stride_size_) % TEST_INFO->total_size_;
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t request_size =
          (TEST_INFO->request_size_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->request_size_)) %
              (TEST_INFO->total_size_ - offset);
      std::string data(request_size, '1');
      TEST_INFO->test_fread(data.data(), request_size);
      REQUIRE(TEST_INFO->size_read_orig_ == request_size);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedUpdateStrideFixedRSVariable",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_update]"
              "[request_size=type-variable][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=stride_fixed][file=1]") {
  TEST_INFO->Pretest();

  SECTION("write to existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    std::string data(TEST_INFO->request_size_, '1');
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      auto offset = (i * TEST_INFO->stride_size_) % TEST_INFO->total_size_;
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t request_size =
          TEST_INFO->request_size_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->request_size_);
      std::string data(request_size, '1');
      TEST_INFO->test_fwrite(data.c_str(), request_size);
      REQUIRE(TEST_INFO->size_written_orig_ == request_size);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedReadStrideDynamicRSVariable",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=type-variable][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=stride_dynamic][file=1]") {
  TEST_INFO->Pretest();

  SECTION("read from existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      auto offset = TEST_INFO->GetRandomOffset(i, TEST_INFO->offset_seed_, TEST_INFO->stride_size_,
                                               TEST_INFO->total_size_);
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t request_size =
          TEST_INFO->request_size_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->request_size_);
      std::string data(request_size, '1');
      TEST_INFO->test_fread(data.data(), request_size);
      REQUIRE(TEST_INFO->size_read_orig_ == request_size);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedUpdateStrideDynamicRSVariable",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_update]"
              "[request_size=type-variable][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=stride_dynamic][file=1]") {
  TEST_INFO->Pretest();
  SECTION("read from existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      auto offset = TEST_INFO->GetRandomOffset(i, TEST_INFO->offset_seed_, TEST_INFO->stride_size_,
                                               TEST_INFO->total_size_);
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t request_size =
          TEST_INFO->request_size_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->request_size_);
      std::string data(request_size, '1');
      TEST_INFO->test_fwrite(data.c_str(), request_size);
      REQUIRE(TEST_INFO->size_written_orig_ == request_size);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedReadStrideNegative",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=stride_negative][file=1]") {
  TEST_INFO->Pretest();
  SECTION("read from existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    std::string data(TEST_INFO->request_size_, '1');
    size_t prev_offset = TEST_INFO->total_size_ + 1;
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      auto stride_offset = TEST_INFO->total_size_ - i * TEST_INFO->stride_size_;
      REQUIRE(prev_offset > stride_offset);
      prev_offset = stride_offset;
      auto offset = (stride_offset) % (TEST_INFO->total_size_ - TEST_INFO->request_size_);
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      TEST_INFO->test_fread(data.data(), TEST_INFO->request_size_);
      REQUIRE(TEST_INFO->size_read_orig_ == TEST_INFO->request_size_);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedUpdateStrideNegative",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_update]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=stride_negative][file=1]") {
  TEST_INFO->Pretest();
  SECTION("read from existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    std::string data(TEST_INFO->request_size_, '1');
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      auto offset =
          TEST_INFO->total_size_ - TEST_INFO->request_size_ -
              ((i * TEST_INFO->stride_size_) % TEST_INFO->total_size_);
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      TEST_INFO->test_fread(data.data(), TEST_INFO->request_size_);
      REQUIRE(TEST_INFO->size_read_orig_ == TEST_INFO->request_size_);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedReadStrideNegativeRSVariable",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=type-variable][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=stride_negative][file=1]") {
  TEST_INFO->Pretest();

  SECTION("read from existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      auto offset = (TEST_INFO->total_size_ - i * TEST_INFO->stride_size_) %
          (TEST_INFO->total_size_ - 2 * TEST_INFO->request_size_);
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t request_size =
          (TEST_INFO->request_size_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->request_size_)) %
              (TEST_INFO->total_size_ - offset);
      std::string data(request_size, '1');
      TEST_INFO->test_fread(data.data(), request_size);
      REQUIRE(TEST_INFO->size_read_orig_ == request_size);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedUpdateStrideNegativeRSVariable",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_update]"
              "[request_size=type-variable][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=stride_negative][file=1]") {
  TEST_INFO->Pretest();

  SECTION("write to existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    std::string data(TEST_INFO->request_size_, '1');
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      auto offset =
          TEST_INFO->total_size_ - ((i * TEST_INFO->stride_size_) % TEST_INFO->total_size_);
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t request_size =
          TEST_INFO->request_size_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->request_size_);
      std::string data(request_size, '1');
      TEST_INFO->test_fwrite(data.c_str(), request_size);
      REQUIRE(TEST_INFO->size_written_orig_ == request_size);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedReadStride2D", "[process=" + std::to_string(TEST_INFO->comm_size_) +
    "]"
    "[operation=batched_read]"
    "[request_size=type-fixed][repetition=" +
    std::to_string(TEST_INFO->num_iterations_) +
    "]"
    "[pattern=stride_2d][file=1]") {
  TEST_INFO->Pretest();
  size_t rows = sqrt(TEST_INFO->total_size_);
  size_t cols = rows;
  REQUIRE(rows * cols == TEST_INFO->total_size_);
  size_t cell_size = 128;
  size_t cell_stride = rows * cols / cell_size / TEST_INFO->num_iterations_;
  SECTION("read from existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    std::string data(TEST_INFO->request_size_, '1');
    size_t prev_cell_col = 0, prev_cell_row = 0;
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      size_t current_cell_col = (prev_cell_col + cell_stride) % cols;
      size_t current_cell_row = prev_cell_col + cell_stride > cols
                                ? prev_cell_row + 1
                                : prev_cell_row;
      prev_cell_row = current_cell_row;
      auto offset = (current_cell_col * cell_stride + prev_cell_row * cols) %
          (TEST_INFO->total_size_ - TEST_INFO->request_size_);
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      TEST_INFO->test_fread(data.data(), TEST_INFO->request_size_);
      REQUIRE(TEST_INFO->size_read_orig_ == TEST_INFO->request_size_);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedUpdateStride2D",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_update]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=stride_2d][file=1]") {
  TEST_INFO->Pretest();

  size_t rows = sqrt(TEST_INFO->total_size_);
  size_t cols = rows;
  REQUIRE(rows * cols == TEST_INFO->total_size_);
  size_t cell_size = 128;
  size_t cell_stride = rows * cols / cell_size / TEST_INFO->num_iterations_;
  SECTION("read from existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    std::string data(TEST_INFO->request_size_, '1');
    size_t prev_cell_col = 0, prev_cell_row = 0;
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      size_t current_cell_col = (prev_cell_col + cell_stride) % cols;
      size_t current_cell_row = prev_cell_col + cell_stride > cols
                                ? prev_cell_row + 1
                                : prev_cell_row;
      prev_cell_row = current_cell_row;
      auto offset = (current_cell_col * cell_stride + prev_cell_row * cols) %
          (TEST_INFO->total_size_ - TEST_INFO->request_size_);
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      TEST_INFO->test_fread(data.data(), TEST_INFO->request_size_);
      REQUIRE(TEST_INFO->size_read_orig_ == TEST_INFO->request_size_);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedReadStride2DRSVariable",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              ""
              "[operation=batched_read]"
              "[request_size=type-variable][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=stride_2d][file=1]") {
  TEST_INFO->Pretest();
  size_t rows = sqrt(TEST_INFO->total_size_);
  size_t cols = rows;
  REQUIRE(rows * cols == TEST_INFO->total_size_);
  size_t cell_size = 128;
  size_t cell_stride = rows * cols / cell_size / TEST_INFO->num_iterations_;
  SECTION("read from existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    size_t prev_cell_col = 0, prev_cell_row = 0;
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      size_t current_cell_col = (prev_cell_col + cell_stride) % cols;
      size_t current_cell_row = prev_cell_col + cell_stride > cols
                                ? prev_cell_row + 1
                                : prev_cell_row;
      prev_cell_row = current_cell_row;
      auto offset = (current_cell_col * cell_stride + prev_cell_row * cols) %
          (TEST_INFO->total_size_ - 2 * TEST_INFO->request_size_);
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t request_size =
          (TEST_INFO->request_size_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->request_size_)) %
              (TEST_INFO->total_size_ - offset);
      std::string data(request_size, '1');
      TEST_INFO->test_fread(data.data(), request_size);
      REQUIRE(TEST_INFO->size_read_orig_ == request_size);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedUpdateStride2DRSVariable",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_update]"
              "[request_size=type-variable][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=stride_2d][file=1]") {
  TEST_INFO->Pretest();
  size_t rows = sqrt(TEST_INFO->total_size_);
  size_t cols = rows;
  REQUIRE(rows * cols == TEST_INFO->total_size_);
  size_t cell_size = 128;
  size_t cell_stride = rows * cols / cell_size / TEST_INFO->num_iterations_;
  SECTION("write to existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    std::string data(TEST_INFO->request_size_, '1');
    size_t prev_cell_col = 0, prev_cell_row = 0;
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      size_t current_cell_col = (prev_cell_col + cell_stride) % cols;
      size_t current_cell_row = prev_cell_col + cell_stride > cols
                                ? prev_cell_row + 1
                                : prev_cell_row;
      prev_cell_row = current_cell_row;
      auto offset = (current_cell_col * cell_stride + prev_cell_row * cols) %
          (TEST_INFO->total_size_ - 2 * TEST_INFO->request_size_);
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t request_size =
          TEST_INFO->request_size_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->request_size_);
      std::string data(request_size, '1');
      TEST_INFO->test_fwrite(data.c_str(), request_size);
      REQUIRE(TEST_INFO->size_written_orig_ == request_size);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

/**
 * Temporal Fixed
 */

TEST_CASE("BatchedWriteTemporalFixed",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=sequential][file=1][temporal=fixed]") {
  TEST_INFO->Pretest();

  SECTION("write to existing file") {
    TEST_INFO->test_fopen(TEST_INFO->new_file_, "w+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);

    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      usleep(TEST_INFO->temporal_interval_ms_ * 1000);
      TEST_INFO->test_fseek(0, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t offset = ftell(TEST_INFO->fh_orig_);
      REQUIRE(offset == 0);
      TEST_INFO->test_fwrite(TEST_INFO->write_data_.data(), TEST_INFO->request_size_);
      REQUIRE(TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
    REQUIRE(stdfs::file_size(TEST_INFO->new_file_.hermes_) == TEST_INFO->request_size_);
  }

  SECTION("write to new file always at start") {
    TEST_INFO->test_fopen(TEST_INFO->new_file_, "w+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);

    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      usleep(TEST_INFO->temporal_interval_ms_ * 1000);
      TEST_INFO->test_fwrite(TEST_INFO->write_data_.data(), TEST_INFO->request_size_);
      REQUIRE(TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
    REQUIRE(stdfs::file_size(TEST_INFO->new_file_.hermes_) ==
        TEST_INFO->num_iterations_ * TEST_INFO->request_size_);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedReadSequentialTemporalFixed",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=sequential][file=1][temporal=fixed]") {
  TEST_INFO->Pretest();

  SECTION("read from existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    std::string data(TEST_INFO->request_size_, '1');
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      usleep(TEST_INFO->temporal_interval_ms_ * 1000);
      TEST_INFO->test_fread(data.data(), TEST_INFO->request_size_);
      REQUIRE(TEST_INFO->size_read_orig_ == TEST_INFO->request_size_);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }

  SECTION("read from existing file always at start") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "w+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);

    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      usleep(TEST_INFO->temporal_interval_ms_ * 1000);
      TEST_INFO->test_fseek(0, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t offset = ftell(TEST_INFO->fh_orig_);
      REQUIRE(offset == 0);
      TEST_INFO->test_fwrite(TEST_INFO->write_data_.data(), TEST_INFO->request_size_);
      REQUIRE(TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedWriteTemporalVariable",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=sequential][file=1][temporal=variable]") {
  TEST_INFO->Pretest();

  SECTION("write to existing file") {
    TEST_INFO->test_fopen(TEST_INFO->new_file_, "w+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);

    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      TEST_INFO->temporal_interval_ms_ =
          rand_r(&TEST_INFO->temporal_interval_seed_) % TEST_INFO->temporal_interval_ms_ + 1;
      usleep(TEST_INFO->temporal_interval_ms_ * 1000);
      TEST_INFO->test_fseek(0, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t offset = ftell(TEST_INFO->fh_orig_);
      REQUIRE(offset == 0);
      TEST_INFO->test_fwrite(TEST_INFO->write_data_.data(), TEST_INFO->request_size_);
      REQUIRE(TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
    REQUIRE(stdfs::file_size(TEST_INFO->new_file_.hermes_) == TEST_INFO->request_size_);
  }

  SECTION("write to new file always at start") {
    TEST_INFO->test_fopen(TEST_INFO->new_file_, "w+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);

    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      TEST_INFO->temporal_interval_ms_ =
          rand_r(&TEST_INFO->temporal_interval_seed_) % TEST_INFO->temporal_interval_ms_ + 1;
      usleep(TEST_INFO->temporal_interval_ms_ * 1000);
      TEST_INFO->test_fwrite(TEST_INFO->write_data_.data(), TEST_INFO->request_size_);
      REQUIRE(TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
    REQUIRE(stdfs::file_size(TEST_INFO->new_file_.hermes_) ==
        TEST_INFO->num_iterations_ * TEST_INFO->request_size_);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedReadSequentialTemporalVariable",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=sequential][file=1][temporal=variable]") {
  TEST_INFO->Pretest();

  SECTION("read from existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    std::string data(TEST_INFO->request_size_, '1');
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      TEST_INFO->temporal_interval_ms_ =
          rand_r(&TEST_INFO->temporal_interval_seed_) % TEST_INFO->temporal_interval_ms_ + 1;
      usleep(TEST_INFO->temporal_interval_ms_ * 1000);
      TEST_INFO->test_fread(data.data(), TEST_INFO->request_size_);
      REQUIRE(TEST_INFO->size_read_orig_ == TEST_INFO->request_size_);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }

  SECTION("read from existing file always at start") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "w+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);

    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      TEST_INFO->temporal_interval_ms_ =
          rand_r(&TEST_INFO->temporal_interval_seed_) % TEST_INFO->temporal_interval_ms_ + 1;
      usleep(TEST_INFO->temporal_interval_ms_ * 1000);
      TEST_INFO->test_fseek(0, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t offset = ftell(TEST_INFO->fh_orig_);
      REQUIRE(offset == 0);
      TEST_INFO->test_fwrite(TEST_INFO->write_data_.data(), TEST_INFO->request_size_);
      REQUIRE(TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedMixedSequential",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_mixed]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=sequential][file=1]") {
  TEST_INFO->Pretest();
  SECTION("read after write on new file") {
    TEST_INFO->test_fopen(TEST_INFO->new_file_, "w+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    size_t last_offset = 0;
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      TEST_INFO->test_fwrite(TEST_INFO->write_data_.data(), TEST_INFO->request_size_);
      REQUIRE(TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
      TEST_INFO->test_fseek(last_offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      TEST_INFO->test_fread(TEST_INFO->read_data_.data(), TEST_INFO->request_size_);
      REQUIRE(TEST_INFO->size_read_orig_ == TEST_INFO->request_size_);
      last_offset += TEST_INFO->request_size_;
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }

  SECTION("write and read alternative existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      if (i % 2 == 0) {
        TEST_INFO->test_fwrite(TEST_INFO->write_data_.data(), TEST_INFO->request_size_);
        REQUIRE(TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
      } else {
        TEST_INFO->test_fread(TEST_INFO->read_data_.data(), TEST_INFO->request_size_);
        REQUIRE(TEST_INFO->size_read_orig_ == TEST_INFO->request_size_);
      }
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  SECTION("update after read existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    size_t last_offset = 0;
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      TEST_INFO->test_fread(TEST_INFO->read_data_.data(), TEST_INFO->request_size_);
      REQUIRE(TEST_INFO->size_read_orig_ == TEST_INFO->request_size_);
      TEST_INFO->test_fseek(last_offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      TEST_INFO->test_fwrite(TEST_INFO->write_data_.data(), TEST_INFO->request_size_);
      REQUIRE(TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
      last_offset += TEST_INFO->request_size_;
    }

    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  SECTION("read all after write all on new file in single open") {
    TEST_INFO->test_fopen(TEST_INFO->new_file_, "w+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      TEST_INFO->test_fwrite(TEST_INFO->write_data_.data(), TEST_INFO->request_size_);
      REQUIRE(TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    }
    TEST_INFO->test_fseek(0, SEEK_SET);
    REQUIRE(TEST_INFO->status_orig_ == 0);
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      TEST_INFO->test_fread(TEST_INFO->read_data_.data(), TEST_INFO->request_size_);
      REQUIRE(TEST_INFO->size_read_orig_ == TEST_INFO->request_size_);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  SECTION("read all after write all on new file in different open") {
    TEST_INFO->test_fopen(TEST_INFO->new_file_, "w+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      TEST_INFO->test_fwrite(TEST_INFO->write_data_.data(), TEST_INFO->request_size_);
      REQUIRE(TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
    TEST_INFO->test_fopen(TEST_INFO->new_file_, "r");
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      TEST_INFO->test_fread(TEST_INFO->read_data_.data(), TEST_INFO->request_size_);
      REQUIRE(TEST_INFO->size_read_orig_ == TEST_INFO->request_size_);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("SingleMixed", "[process=" + std::to_string(TEST_INFO->comm_size_) +
    "][operation=single_mixed]"
    "[request_size=type-fixed][repetition=1]"
    "[file=1]") {
  TEST_INFO->Pretest();
  SECTION("read after write from new file") {
    TEST_INFO->test_fopen(TEST_INFO->new_file_, "w+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    size_t offset = ftell(TEST_INFO->fh_orig_);
    REQUIRE(offset == 0);
    TEST_INFO->test_fwrite(TEST_INFO->write_data_.data(), TEST_INFO->request_size_);
    REQUIRE(TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_fseek(0, SEEK_SET);
    REQUIRE(TEST_INFO->status_orig_ == 0);
    TEST_INFO->test_fread(TEST_INFO->read_data_.data(), TEST_INFO->request_size_);
    REQUIRE(TEST_INFO->size_read_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  SECTION("update after read from existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    size_t offset = ftell(TEST_INFO->fh_orig_);
    REQUIRE(offset == 0);
    TEST_INFO->test_fread(TEST_INFO->read_data_.data(), TEST_INFO->request_size_);
    REQUIRE(TEST_INFO->size_read_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_fseek(0, SEEK_SET);
    REQUIRE(TEST_INFO->status_orig_ == 0);
    TEST_INFO->test_fwrite(TEST_INFO->write_data_.data(), TEST_INFO->request_size_);
    REQUIRE(TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);

    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  SECTION("read after write from new file different opens") {
    TEST_INFO->test_fopen(TEST_INFO->new_file_, "w+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    size_t offset = ftell(TEST_INFO->fh_orig_);
    REQUIRE(offset == 0);
    TEST_INFO->test_fwrite(TEST_INFO->write_data_.data(), TEST_INFO->request_size_);
    REQUIRE(TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
    TEST_INFO->test_fopen(TEST_INFO->new_file_, "r+");
    TEST_INFO->test_fread(TEST_INFO->read_data_.data(), TEST_INFO->request_size_);
    REQUIRE(TEST_INFO->size_read_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}
