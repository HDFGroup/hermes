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

TEST_CASE("BatchedWriteRSRangeSmall",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              ""
              "[operation=batched_write]"
              "[request_size=range-small][repetition=" +
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
          TEST_INFO->small_min_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->small_max_);
      std::string data(request_size, '1');
      TEST_INFO->test_fwrite(data.data(), request_size);
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
    size_t total_written = 0;
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      size_t request_size =
          TEST_INFO->small_min_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->small_max_);
      std::string data(request_size, '1');
      TEST_INFO->test_fwrite(data.data(), request_size);
      REQUIRE(TEST_INFO->size_written_orig_ == request_size);
      total_written += TEST_INFO->size_written_orig_;
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
    REQUIRE(stdfs::file_size(TEST_INFO->new_file_.hermes_) == total_written);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedReadSequentialRSRangeSmall",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-small][repetition=" +
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
      size_t request_size =
          TEST_INFO->small_min_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->small_max_);
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
          TEST_INFO->small_min_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->small_max_);
      std::string data(request_size, '1');
      TEST_INFO->test_fread(data.data(), request_size);
      REQUIRE(TEST_INFO->size_read_orig_ == request_size);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedReadRandomRSRangeSmall",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-small]"
              "[repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "][pattern=random][file=1]") {
  TEST_INFO->Pretest();

  SECTION("read from existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      auto offset =
          rand_r(&TEST_INFO->offset_seed_) % (TEST_INFO->total_size_ - TEST_INFO->small_max_);
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t request_size =
          TEST_INFO->small_min_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->small_max_);
      std::string data(request_size, '1');
      TEST_INFO->test_fread(data.data(), request_size);
      REQUIRE(TEST_INFO->size_read_orig_ == request_size);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedUpdateRandomRSRangeSmall",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-small][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=random][file=1]") {
  TEST_INFO->Pretest();

  SECTION("read from existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      auto offset =
          rand_r(&TEST_INFO->offset_seed_) % (TEST_INFO->total_size_ - TEST_INFO->small_max_);
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t request_size =
          TEST_INFO->small_min_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->small_max_);
      std::string data(request_size, '1');
      TEST_INFO->test_fwrite(data.data(), request_size);
      REQUIRE(TEST_INFO->size_written_orig_ == request_size);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedReadStrideFixedRSRangeSmall",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-small][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=stride_fixed][file=1]") {
  TEST_INFO->Pretest();
  SECTION("read from existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      auto offset = (i * TEST_INFO->stride_size_) % (TEST_INFO->total_size_ - TEST_INFO->small_max_);
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t request_size =
          TEST_INFO->small_min_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->small_max_);
      std::string data(request_size, '1');
      TEST_INFO->test_fread(data.data(), request_size);
      REQUIRE(TEST_INFO->size_read_orig_ == request_size);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedUpdateStrideFixedRSRangeSmall",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-small][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=stride_fixed][file=1]") {
  TEST_INFO->Pretest();
  SECTION("write to existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      auto offset = (i * TEST_INFO->stride_size_) % (TEST_INFO->total_size_ - TEST_INFO->small_max_);
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t request_size =
          TEST_INFO->small_min_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->small_max_);
      std::string data(request_size, '1');
      TEST_INFO->test_fwrite(data.data(), request_size);
      REQUIRE(TEST_INFO->size_written_orig_ == request_size);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedReadStrideDynamicRSRangeSmall",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-small][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=stride_dynamic][file=1]") {
  TEST_INFO->Pretest();
  SECTION("read from existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      auto offset = TEST_INFO->GetRandomOffset(i, TEST_INFO->offset_seed_, TEST_INFO->stride_size_,
                                    TEST_INFO->total_size_ - TEST_INFO->small_max_);
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t request_size =
          TEST_INFO->small_min_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->small_max_);
      std::string data(request_size, '1');
      TEST_INFO->test_fread(data.data(), request_size);
      REQUIRE(TEST_INFO->size_read_orig_ == request_size);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedUpdateStrideDynamicRSRangeSmall",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-small][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=stride_dynamic][file=1]") {
  TEST_INFO->Pretest();
  SECTION("read from existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      auto offset = TEST_INFO->GetRandomOffset(i, TEST_INFO->offset_seed_, TEST_INFO->stride_size_,
                                    TEST_INFO->total_size_ - TEST_INFO->small_max_);
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t request_size =
          TEST_INFO->small_min_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->small_max_);
      std::string data(request_size, '1');
      TEST_INFO->test_fwrite(data.data(), request_size);
      REQUIRE(TEST_INFO->size_written_orig_ == request_size);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedReadStrideNegativeRSRangeSmall",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-small][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=stride_negative][file=1]") {
  TEST_INFO->Pretest();
  SECTION("read from existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      auto offset = (TEST_INFO->total_size_ - i * TEST_INFO->stride_size_) %
                    (TEST_INFO->total_size_ - TEST_INFO->small_max_);
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t request_size =
          TEST_INFO->small_min_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->small_max_);
      std::string data(request_size, '1');
      TEST_INFO->test_fread(data.data(), request_size);
      REQUIRE(TEST_INFO->size_read_orig_ == request_size);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedUpdateStrideNegativeRSRangeSmall",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              ""
              "[operation=batched_write]"
              "[request_size=range-small][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=stride_negative][file=1]") {
  TEST_INFO->Pretest();
  SECTION("write to existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      auto offset = TEST_INFO->total_size_ - ((i * TEST_INFO->stride_size_) %
                                       (TEST_INFO->total_size_ - TEST_INFO->small_max_));
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t request_size =
          TEST_INFO->small_min_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->small_max_);
      std::string data(request_size, '1');
      TEST_INFO->test_fwrite(data.data(), request_size);
      REQUIRE(TEST_INFO->size_written_orig_ == request_size);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedReadStride2DRSRangeSmall",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-small][repetition=" +
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
                    (TEST_INFO->total_size_ - TEST_INFO->small_max_);
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t request_size =
          TEST_INFO->small_min_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->small_max_);
      std::string data(request_size, '1');
      TEST_INFO->test_fread(data.data(), request_size);
      REQUIRE(TEST_INFO->size_read_orig_ == request_size);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedUpdateStride2DRSRangeSmall",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-small][repetition=" +
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
    size_t prev_cell_col = 0, prev_cell_row = 0;
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      size_t current_cell_col = (prev_cell_col + cell_stride) % cols;
      size_t current_cell_row = prev_cell_col + cell_stride > cols
                                    ? prev_cell_row + 1
                                    : prev_cell_row;
      prev_cell_row = current_cell_row;
      auto offset = (current_cell_col * cell_stride + prev_cell_row * cols) %
                    (TEST_INFO->total_size_ - TEST_INFO->small_max_);
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t request_size =
          TEST_INFO->small_min_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->small_max_);
      std::string data(request_size, '1');
      TEST_INFO->test_fwrite(data.data(), request_size);
      REQUIRE(TEST_INFO->size_written_orig_ == request_size);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}
/**
 * Medium RS
 **/

TEST_CASE("BatchedWriteRSRangeMedium",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-medium][repetition=" +
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
          TEST_INFO->medium_min_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->medium_max_);
      std::string data(request_size, '1');
      TEST_INFO->test_fwrite(data.data(), request_size);
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
    size_t total_written = 0;
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      size_t request_size =
          TEST_INFO->medium_min_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->medium_max_);
      std::string data(request_size, '1');
      TEST_INFO->test_fwrite(data.data(), request_size);
      REQUIRE(TEST_INFO->size_written_orig_ == request_size);
      total_written += TEST_INFO->size_written_orig_;
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedReadSequentialRSRangeMedium",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-medium][repetition=" +
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
      size_t request_size =
          (TEST_INFO->medium_min_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->medium_max_)) %
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
          TEST_INFO->medium_min_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->medium_max_);
      std::string data(request_size, '1');
      TEST_INFO->test_fread(data.data(), request_size);
      REQUIRE(TEST_INFO->size_read_orig_ == request_size);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedReadRandomRSRangeMedium",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-medium]"
              "[repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "][pattern=random][file=1]") {
  TEST_INFO->Pretest();

  SECTION("read from existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      auto offset =
          rand_r(&TEST_INFO->offset_seed_) % (TEST_INFO->total_size_ - TEST_INFO->medium_max_);
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t request_size =
          TEST_INFO->medium_min_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->medium_max_);
      std::string data(request_size, '1');
      TEST_INFO->test_fread(data.data(), request_size);
      REQUIRE(TEST_INFO->size_read_orig_ == request_size);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedUpdateRandomRSRangeMedium",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-medium][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=random][file=1]") {
  TEST_INFO->Pretest();

  SECTION("read from existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      auto offset =
          rand_r(&TEST_INFO->offset_seed_) % (TEST_INFO->total_size_ - TEST_INFO->medium_max_);
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t request_size =
          TEST_INFO->medium_min_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->medium_max_);
      std::string data(request_size, '1');
      TEST_INFO->test_fwrite(data.data(), request_size);
      REQUIRE(TEST_INFO->size_written_orig_ == request_size);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedReadStrideFixedRSRangeMedium",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-medium][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=stride_fixed][file=1]") {
  TEST_INFO->Pretest();
  SECTION("read from existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      auto offset =
          (i * TEST_INFO->stride_size_) % (TEST_INFO->total_size_ - TEST_INFO->medium_max_);
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t request_size =
          TEST_INFO->medium_min_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->medium_max_);
      std::string data(request_size, '1');
      TEST_INFO->test_fread(data.data(), request_size);
      REQUIRE(TEST_INFO->size_read_orig_ == request_size);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedUpdateStrideFixedRSRangeMedium",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-medium][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=stride_fixed][file=1]") {
  TEST_INFO->Pretest();
  SECTION("write to existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      auto offset =
          (i * TEST_INFO->stride_size_) % (TEST_INFO->total_size_ - TEST_INFO->medium_max_);
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t request_size =
          TEST_INFO->medium_min_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->medium_max_);
      std::string data(request_size, '1');
      TEST_INFO->test_fwrite(data.data(), request_size);
      REQUIRE(TEST_INFO->size_written_orig_ == request_size);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedReadStrideDynamicRSRangeMedium",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-medium][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=stride_dynamic][file=1]") {
  TEST_INFO->Pretest();
  SECTION("read from existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      auto offset = TEST_INFO->GetRandomOffset(i, TEST_INFO->offset_seed_, TEST_INFO->stride_size_,
                                    TEST_INFO->total_size_ - TEST_INFO->medium_max_);
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t request_size =
          TEST_INFO->medium_min_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->medium_max_);
      std::string data(request_size, '1');
      TEST_INFO->test_fread(data.data(), request_size);
      REQUIRE(TEST_INFO->size_read_orig_ == request_size);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedUpdateStrideDynamicRSRangeMedium",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-medium][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=stride_dynamic][file=1]") {
  TEST_INFO->Pretest();
  SECTION("read from existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      auto offset = TEST_INFO->GetRandomOffset(i, TEST_INFO->offset_seed_, TEST_INFO->stride_size_,
                                    TEST_INFO->total_size_ - TEST_INFO->medium_max_);
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t request_size =
          TEST_INFO->medium_min_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->medium_max_);
      std::string data(request_size, '1');
      TEST_INFO->test_fwrite(data.data(), request_size);
      REQUIRE(TEST_INFO->size_written_orig_ == request_size);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedReadStrideNegativeRSRangeMedium",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-medium][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=stride_negative][file=1]") {
  TEST_INFO->Pretest();
  SECTION("read from existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      auto offset = (TEST_INFO->total_size_ - i * TEST_INFO->stride_size_) %
                    (TEST_INFO->total_size_ - TEST_INFO->medium_max_);
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t request_size =
          TEST_INFO->medium_min_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->medium_max_);
      std::string data(request_size, '1');
      TEST_INFO->test_fread(data.data(), request_size);
      REQUIRE(TEST_INFO->size_read_orig_ == request_size);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedUpdateStrideNegativeRSRangeMedium",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-medium][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=stride_negative][file=1]") {
  TEST_INFO->Pretest();
  SECTION("write to existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      auto offset = TEST_INFO->total_size_ - ((i * TEST_INFO->stride_size_) %
                                       (TEST_INFO->total_size_ - TEST_INFO->medium_max_));
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t request_size =
          TEST_INFO->medium_min_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->medium_max_);
      std::string data(request_size, '1');
      TEST_INFO->test_fwrite(data.data(), request_size);
      REQUIRE(TEST_INFO->size_written_orig_ == request_size);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedReadStride2DRSRangeMedium",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-medium][repetition=" +
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
                    (TEST_INFO->total_size_ - TEST_INFO->medium_max_);
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t request_size =
          TEST_INFO->medium_min_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->medium_max_);
      std::string data(request_size, '1');
      TEST_INFO->test_fread(data.data(), request_size);
      REQUIRE(TEST_INFO->size_read_orig_ == request_size);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedUpdateStride2DRSRangeMedium",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-medium][repetition=" +
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
    size_t prev_cell_col = 0, prev_cell_row = 0;
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      size_t current_cell_col = (prev_cell_col + cell_stride) % cols;
      size_t current_cell_row = prev_cell_col + cell_stride > cols
                                    ? prev_cell_row + 1
                                    : prev_cell_row;
      prev_cell_row = current_cell_row;
      auto offset = (current_cell_col * cell_stride + prev_cell_row * cols) %
                    (TEST_INFO->total_size_ - TEST_INFO->medium_max_);
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t request_size =
          TEST_INFO->medium_min_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->medium_max_);
      std::string data(request_size, '1');
      TEST_INFO->test_fwrite(data.data(), request_size);
      REQUIRE(TEST_INFO->size_written_orig_ == request_size);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}
/**
 * Large RS
 **/

TEST_CASE("BatchedWriteRSRangeLarge",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-large][repetition=" +
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
          TEST_INFO->large_min_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->large_max_);
      std::string data(request_size, '1');
      TEST_INFO->test_fwrite(data.data(), request_size);
      REQUIRE(TEST_INFO->size_written_orig_ == request_size);
      if (biggest_written < request_size) biggest_written = request_size;
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }

  SECTION("write to new file") {
    TEST_INFO->test_fopen(TEST_INFO->new_file_, "w+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    size_t total_written = 0;
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      size_t request_size =
          TEST_INFO->large_min_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->large_max_);
      std::string data(request_size, '1');
      TEST_INFO->test_fwrite(data.data(), request_size);
      REQUIRE(TEST_INFO->size_written_orig_ == request_size);
      total_written += TEST_INFO->size_written_orig_;
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
    REQUIRE(stdfs::file_size(TEST_INFO->new_file_.hermes_) == total_written);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedReadSequentialRSRangeLarge",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-large][repetition=" +
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
      size_t request_size =
          (TEST_INFO->large_min_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->large_max_)) %
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
          TEST_INFO->large_min_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->large_max_);
      std::string data(request_size, '1');
      TEST_INFO->test_fread(data.data(), request_size);
      REQUIRE(TEST_INFO->size_read_orig_ == request_size);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedReadRandomRSRangeLarge",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-large]"
              "[repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "][pattern=random][file=1]") {
  TEST_INFO->Pretest();
  SECTION("read from existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      auto offset =
          rand_r(&TEST_INFO->offset_seed_) % (TEST_INFO->total_size_ - TEST_INFO->large_max_);
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t request_size =
          (TEST_INFO->large_min_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->large_max_)) %
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

TEST_CASE("BatchedUpdateRandomRSRangeLarge",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-large][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=random][file=1]") {
  TEST_INFO->Pretest();

  SECTION("write into existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      auto offset =
          rand_r(&TEST_INFO->offset_seed_) % (TEST_INFO->total_size_ - TEST_INFO->large_max_);
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t request_size =
          TEST_INFO->large_min_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->large_max_);
      std::vector<char> data = TEST_INFO->GenRandom(request_size);
      TEST_INFO->test_fwrite(data.data(), request_size);
      REQUIRE(TEST_INFO->size_written_orig_ == request_size);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedReadStrideFixedRSRangeLarge",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-large][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=stride_fixed][file=1]") {
  TEST_INFO->Pretest();
  SECTION("read from existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      auto offset = (i * TEST_INFO->stride_size_) % (TEST_INFO->total_size_ - TEST_INFO->large_max_);
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t request_size =
          TEST_INFO->large_min_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->large_max_);
      std::string data(request_size, '1');
      TEST_INFO->test_fread(data.data(), request_size);
      REQUIRE(TEST_INFO->size_read_orig_ == request_size);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedUpdateStrideFixedRSRangeLarge",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-large][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=stride_fixed][file=1]") {
  TEST_INFO->Pretest();
  SECTION("write to existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      auto offset = (i * TEST_INFO->stride_size_) % (TEST_INFO->total_size_ - TEST_INFO->large_max_);
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t request_size =
          TEST_INFO->large_min_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->large_max_);
      std::string data(request_size, '1');
      TEST_INFO->test_fwrite(data.data(), request_size);
      REQUIRE(TEST_INFO->size_written_orig_ == request_size);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedReadStrideDynamicRSRangeLarge",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-large][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=stride_dynamic][file=1]") {
  TEST_INFO->Pretest();
  SECTION("read from existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      auto offset = TEST_INFO->GetRandomOffset(i, TEST_INFO->offset_seed_, TEST_INFO->stride_size_,
                                    TEST_INFO->total_size_ - TEST_INFO->large_max_);
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t request_size =
          TEST_INFO->large_min_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->large_max_);
      std::string data(request_size, '1');
      TEST_INFO->test_fread(data.data(), request_size);
      REQUIRE(TEST_INFO->size_read_orig_ == request_size);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedUpdateStrideDynamicRSRangeLarge",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-large][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=stride_dynamic][file=1]") {
  TEST_INFO->Pretest();
  SECTION("read from existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      auto offset = TEST_INFO->GetRandomOffset(i, TEST_INFO->offset_seed_, TEST_INFO->stride_size_,
                                    TEST_INFO->total_size_ - TEST_INFO->large_max_);
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t request_size =
          TEST_INFO->large_min_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->large_max_);
      std::string data(request_size, '1');
      TEST_INFO->test_fwrite(data.data(), request_size);
      REQUIRE(TEST_INFO->size_written_orig_ == request_size);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedReadStrideNegativeRSRangeLarge",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-large][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=stride_negative][file=1]") {
  TEST_INFO->Pretest();
  SECTION("read from existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      auto offset = (TEST_INFO->total_size_ - i * TEST_INFO->stride_size_) %
                    (TEST_INFO->total_size_ - TEST_INFO->large_max_);
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t request_size =
          (TEST_INFO->large_min_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->large_max_)) %
          (TEST_INFO->total_size_ - TEST_INFO->large_max_);
      std::string data(request_size, '1');
      TEST_INFO->test_fread(data.data(), request_size);
      REQUIRE(TEST_INFO->size_read_orig_ == request_size);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedUpdateStrideNegativeRSRangeLarge",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-large][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=stride_negative][file=1]") {
  TEST_INFO->Pretest();
  SECTION("write to existing file") {
    TEST_INFO->test_fopen(TEST_INFO->existing_file_, "r+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      auto offset = TEST_INFO->total_size_ - ((i * TEST_INFO->stride_size_) %
                                       (TEST_INFO->total_size_ - TEST_INFO->large_max_));
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t request_size =
          TEST_INFO->large_min_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->large_max_);
      std::string data(request_size, '1');
      TEST_INFO->test_fwrite(data.data(), request_size);
      REQUIRE(TEST_INFO->size_written_orig_ == request_size);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedReadStride2DRSRangeLarge",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-large][repetition=" +
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
                    (TEST_INFO->total_size_ - TEST_INFO->large_max_);
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t request_size =
          TEST_INFO->large_min_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->large_max_);
      std::string data(request_size, '1');
      TEST_INFO->test_fread(data.data(), request_size);
      REQUIRE(TEST_INFO->size_read_orig_ == request_size);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedUpdateStride2DRSRangeLarge",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              ""
              "[operation=batched_write]"
              "[request_size=range-large][repetition=" +
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
    size_t prev_cell_col = 0, prev_cell_row = 0;
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      size_t current_cell_col = (prev_cell_col + cell_stride) % cols;
      size_t current_cell_row = prev_cell_col + cell_stride > cols
                                    ? prev_cell_row + 1
                                    : prev_cell_row;
      prev_cell_row = current_cell_row;
      auto offset = (current_cell_col * cell_stride + prev_cell_row * cols) %
                    (TEST_INFO->total_size_ - TEST_INFO->large_max_);
      TEST_INFO->test_fseek(offset, SEEK_SET);
      REQUIRE(TEST_INFO->status_orig_ == 0);
      size_t request_size =
          TEST_INFO->large_min_ + (rand_r(&TEST_INFO->rs_seed_) % TEST_INFO->large_max_);
      std::string data(request_size, '1');
      TEST_INFO->test_fwrite(data.data(), request_size);
      REQUIRE(TEST_INFO->size_written_orig_ == request_size);
    }
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
  }
  TEST_INFO->Posttest();
}
