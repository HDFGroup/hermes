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

#include "posix_adapter_test.h"

TEST_CASE("BatchedWriteRSRangeSmall",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-small][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=sequential][file=1]") {
  TESTER->Pretest();
  SECTION("write to new file always at the start") {
    TESTER->test_open(TESTER->new_file_, O_WRONLY | O_CREAT, 0600);
    REQUIRE(TESTER->fh_orig_ != -1);
    size_t biggest_size_written = 0;
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      TESTER->test_seek(0, SEEK_SET);
      REQUIRE(TESTER->status_orig_ == 0);
      size_t request_size =
          TESTER->small_min_ + (rand_r(&TESTER->rs_seed_) % TESTER->small_max_);
      std::string data(request_size, '1');
      TESTER->test_write(data.c_str(), request_size);
      REQUIRE(TESTER->size_written_orig_ == request_size);
      if (biggest_size_written < request_size)
        biggest_size_written = request_size;
    }
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == 0);
    REQUIRE(stdfs::file_size(TESTER->new_file_.hermes_.c_str()) ==
        biggest_size_written);
  }

  SECTION("write to new file") {
    TESTER->test_open(TESTER->new_file_, O_WRONLY | O_CREAT, 0600);
    REQUIRE(TESTER->fh_orig_ != -1);
    size_t total_size_written = 0;
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t request_size =
          TESTER->small_min_ + (rand_r(&TESTER->rs_seed_) % TESTER->small_max_);
      std::string data(request_size, '1');
      TESTER->test_write(data.c_str(), request_size);
      REQUIRE(TESTER->size_written_orig_ == request_size);
      total_size_written += TESTER->size_written_orig_;
    }
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == 0);
    REQUIRE(stdfs::file_size(TESTER->new_file_.hermes_.c_str()) ==
        total_size_written);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedReadSequentialRSRangeSmall",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-small][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=sequential][file=1]") {
  TESTER->Pretest();
  SECTION("read from existing file") {
    TESTER->test_open(TESTER->existing_file_, O_RDONLY);
    REQUIRE(TESTER->fh_orig_ != -1);
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t request_size =
          TESTER->small_min_ +
              (rand_r(&TESTER->rs_seed_) % TESTER->small_max_);
      std::string data(request_size, '1');
      TESTER->test_read(data.data(), request_size);
      REQUIRE(TESTER->size_read_orig_ == request_size);
    }
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == 0);
  }

  SECTION("read from existing file always at start") {
    TESTER->test_open(TESTER->existing_file_, O_RDONLY);
    REQUIRE(TESTER->fh_orig_ != -1);

    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      TESTER->test_seek(0, SEEK_SET);
      REQUIRE(TESTER->status_orig_ == 0);
      size_t request_size =
          TESTER->small_min_ +
              (rand_r(&TESTER->rs_seed_) % TESTER->small_max_);
      std::string data(request_size, '1');
      TESTER->test_read(data.data(), request_size);
      REQUIRE(TESTER->size_read_orig_ == request_size);
    }
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedReadRandomRSRangeSmall",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-small]"
              "[repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "][pattern=random][file=1]") {
  TESTER->Pretest();

  SECTION("read from existing file") {
    TESTER->test_open(TESTER->existing_file_, O_RDWR);
    REQUIRE(TESTER->fh_orig_ != -1);
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t offset =
          rand_r(&TESTER->offset_seed_) %
              (TESTER->total_size_ - TESTER->small_max_);
      TESTER->test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)TESTER->status_orig_) == offset);
      size_t request_size =
          TESTER->small_min_ +
              (rand_r(&TESTER->rs_seed_) % TESTER->small_max_);
      std::string data(request_size, '1');
      TESTER->test_read(data.data(), request_size);
      REQUIRE(TESTER->size_read_orig_ == request_size);
    }
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedUpdateRandomRSRangeSmall",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-small][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=random][file=1]") {
  TESTER->Pretest();

  SECTION("read from existing file") {
    TESTER->test_open(TESTER->existing_file_, O_RDWR);
    REQUIRE(TESTER->fh_orig_ != -1);
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t offset =
          rand_r(&TESTER->offset_seed_) %
              (TESTER->total_size_ - TESTER->small_max_);
      TESTER->test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)TESTER->status_orig_) == offset);
      size_t request_size =
          TESTER->small_min_ +
              (rand_r(&TESTER->rs_seed_) % TESTER->small_max_);
      std::string data(request_size, '1');
      TESTER->test_write(data.c_str(), request_size);
      REQUIRE(TESTER->size_written_orig_ == request_size);
    }
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedReadStrideFixedRSRangeSmall",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-small][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=stride_fixed][file=1]") {
  TESTER->Pretest();
  SECTION("read from existing file") {
    TESTER->test_open(TESTER->existing_file_, O_RDWR);
    REQUIRE(TESTER->fh_orig_ != -1);
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t offset =
          (i * TESTER->stride_size_) %
              (TESTER->total_size_ - TESTER->small_max_);
      TESTER->test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)TESTER->status_orig_) == offset);
      size_t request_size =
          TESTER->small_min_ +
              (rand_r(&TESTER->rs_seed_) % TESTER->small_max_);
      std::string data(request_size, '1');
      TESTER->test_read(data.data(), request_size);
      REQUIRE(TESTER->size_read_orig_ == request_size);
    }
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedUpdateStrideFixedRSRangeSmall",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-small][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=stride_fixed][file=1]") {
  TESTER->Pretest();
  SECTION("write to existing file") {
    TESTER->test_open(TESTER->existing_file_, O_RDWR);
    REQUIRE(TESTER->fh_orig_ != -1);
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t offset =
          (i * TESTER->stride_size_) %
              (TESTER->total_size_ - TESTER->small_max_);
      TESTER->test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)TESTER->status_orig_) == offset);
      size_t request_size =
          TESTER->small_min_ +
              (rand_r(&TESTER->rs_seed_) % TESTER->small_max_);
      std::string data(request_size, '1');
      TESTER->test_write(data.c_str(), request_size);
      REQUIRE(TESTER->size_written_orig_ == request_size);
    }
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedReadStrideDynamicRSRangeSmall",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-small][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=stride_dynamic][file=1]") {
  TESTER->Pretest();
  SECTION("read from existing file") {
    TESTER->test_open(TESTER->existing_file_, O_RDWR);
    REQUIRE(TESTER->fh_orig_ != -1);
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t offset = TESTER->GetRandomOffset(
          i, TESTER->offset_seed_, TESTER->stride_size_,
          TESTER->total_size_ - TESTER->small_max_);
      TESTER->test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)TESTER->status_orig_) == offset);
      size_t request_size =
          TESTER->small_min_ + (rand_r(&TESTER->rs_seed_) % TESTER->small_max_);
      std::string data(request_size, '1');
      TESTER->test_read(data.data(), request_size);
      REQUIRE(TESTER->size_read_orig_ == request_size);
    }
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedUpdateStrideDynamicRSRangeSmall",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-small][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=stride_dynamic][file=1]") {
  TESTER->Pretest();
  SECTION("read from existing file") {
    TESTER->test_open(TESTER->existing_file_, O_RDWR);
    REQUIRE(TESTER->fh_orig_ != -1);
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t offset = TESTER->GetRandomOffset(
          i, TESTER->offset_seed_, TESTER->stride_size_,
          TESTER->total_size_ - TESTER->small_max_);
      TESTER->test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)TESTER->status_orig_) == offset);
      size_t request_size =
          TESTER->small_min_ + (rand_r(&TESTER->rs_seed_) % TESTER->small_max_);
      std::string data(request_size, '1');
      TESTER->test_write(data.c_str(), request_size);
      REQUIRE(TESTER->size_written_orig_ == request_size);
    }
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedReadStrideNegativeRSRangeSmall",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-small][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=stride_negative][file=1]") {
  TESTER->Pretest();
  SECTION("read from existing file") {
    TESTER->test_open(TESTER->existing_file_, O_RDWR);
    REQUIRE(TESTER->fh_orig_ != -1);
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t offset = (TESTER->total_size_ - i * TESTER->stride_size_) %
          (TESTER->total_size_ - TESTER->small_max_);
      TESTER->test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)TESTER->status_orig_) == offset);
      size_t request_size =
          TESTER->small_min_ + (rand_r(&TESTER->rs_seed_) % TESTER->small_max_);
      std::string data(request_size, '1');
      TESTER->test_read(data.data(), request_size);
      REQUIRE(TESTER->size_read_orig_ == request_size);
    }
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedUpdateStrideNegativeRSRangeSmall",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-small][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=stride_negative][file=1]") {
  TESTER->Pretest();
  SECTION("write to existing file") {
    TESTER->test_open(TESTER->existing_file_, O_RDWR);
    REQUIRE(TESTER->fh_orig_ != -1);
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t offset = TESTER->total_size_ - ((i * TESTER->stride_size_) %
          (TESTER->total_size_ - TESTER->small_max_));
      TESTER->test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)TESTER->status_orig_) == offset);
      size_t request_size =
          TESTER->small_min_ + (rand_r(&TESTER->rs_seed_) % TESTER->small_max_);
      std::string data(request_size, '1');
      TESTER->test_write(data.c_str(), request_size);
      REQUIRE(TESTER->size_written_orig_ == request_size);
    }
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedReadStride2DRSRangeSmall",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-small][repetition=" +
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
    TESTER->test_open(TESTER->existing_file_, O_RDWR);
    REQUIRE(TESTER->fh_orig_ != -1);
    size_t prev_cell_col = 0, prev_cell_row = 0;
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t current_cell_col = (prev_cell_col + cell_stride) % cols;
      size_t current_cell_row = prev_cell_col + cell_stride > cols
                                ? prev_cell_row + 1
                                : prev_cell_row;
      prev_cell_row = current_cell_row;
      size_t offset = (current_cell_col * cell_stride + prev_cell_row * cols) %
          (TESTER->total_size_ - TESTER->small_max_);
      TESTER->test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)TESTER->status_orig_) == offset);
      size_t request_size =
          TESTER->small_min_ +
              (rand_r(&TESTER->rs_seed_) % TESTER->small_max_);
      std::string data(request_size, '1');
      TESTER->test_read(data.data(), request_size);
      REQUIRE(TESTER->size_read_orig_ == request_size);
    }
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedUpdateStride2DRSRangeSmall",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-small][repetition=" +
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
    TESTER->test_open(TESTER->existing_file_, O_RDWR);
    REQUIRE(TESTER->fh_orig_ != -1);
    size_t prev_cell_col = 0, prev_cell_row = 0;
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t current_cell_col = (prev_cell_col + cell_stride) % cols;
      size_t current_cell_row = prev_cell_col + cell_stride > cols
                                ? prev_cell_row + 1
                                : prev_cell_row;
      prev_cell_row = current_cell_row;
      size_t offset = (current_cell_col * cell_stride + prev_cell_row * cols) %
          (TESTER->total_size_ - TESTER->small_max_);
      TESTER->test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)TESTER->status_orig_) == offset);
      size_t request_size =
          TESTER->small_min_ +
              (rand_r(&TESTER->rs_seed_) % TESTER->small_max_);
      std::string data(request_size, '1');
      TESTER->test_write(data.c_str(), request_size);
      REQUIRE(TESTER->size_written_orig_ == request_size);
    }
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}
/**
 * Medium RS
 **/

TEST_CASE("BatchedWriteRSRangeMedium",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-medium][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=sequential][file=1]") {
  TESTER->Pretest();
  SECTION("write to new file always at the start") {
    TESTER->test_open(TESTER->new_file_, O_WRONLY | O_CREAT, 0600);
    REQUIRE(TESTER->fh_orig_ != -1);
    size_t biggest_size_written = 0;
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      TESTER->test_seek(0, SEEK_SET);
      REQUIRE(TESTER->status_orig_ == 0);
      size_t request_size =
          TESTER->medium_min_ +
              (rand_r(&TESTER->rs_seed_) % TESTER->medium_max_);
      std::string data(request_size, '1');
      TESTER->test_write(data.c_str(), request_size);
      REQUIRE(TESTER->size_written_orig_ == request_size);
      if (biggest_size_written < request_size)
        biggest_size_written = request_size;
    }
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == 0);
    REQUIRE(stdfs::file_size(TESTER->new_file_.hermes_.c_str()) ==
        biggest_size_written);
  }

  SECTION("write to new file") {
    TESTER->test_open(TESTER->new_file_, O_WRONLY | O_CREAT, 0600);
    REQUIRE(TESTER->fh_orig_ != -1);
    size_t total_size_written = 0;
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t request_size =
          TESTER->medium_min_ +
              (rand_r(&TESTER->rs_seed_) % TESTER->medium_max_);
      std::string data(request_size, '1');
      TESTER->test_write(data.c_str(), request_size);
      REQUIRE(TESTER->size_written_orig_ == request_size);
      total_size_written += TESTER->size_written_orig_;
    }
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == 0);
    REQUIRE(stdfs::file_size(TESTER->new_file_.hermes_.c_str()) ==
        total_size_written);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedReadSequentialRSRangeMedium",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-medium][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=sequential][file=1]") {
  TESTER->Pretest();
  SECTION("read from existing file") {
    TESTER->test_open(TESTER->existing_file_, O_RDONLY);
    REQUIRE(TESTER->fh_orig_ != -1);
    std::string data(TESTER->request_size_, '1');
    size_t current_offset = 0;
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t request_size =
          (TESTER->medium_min_ +
              (rand_r(&TESTER->rs_seed_) % TESTER->medium_max_)) %
              (TESTER->total_size_ - current_offset);
      std::string data(request_size, '1');
      TESTER->test_read(data.data(), request_size);
      REQUIRE(TESTER->size_read_orig_ == request_size);
      current_offset += TESTER->size_read_orig_;
    }
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == 0);
  }

  SECTION("read from existing file always at start") {
    TESTER->test_open(TESTER->existing_file_, O_RDONLY);
    REQUIRE(TESTER->fh_orig_ != -1);

    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      TESTER->test_seek(0, SEEK_SET);
      REQUIRE(TESTER->status_orig_ == 0);
      size_t request_size =
          TESTER->medium_min_ +
              (rand_r(&TESTER->rs_seed_) % TESTER->medium_max_);
      std::string data(request_size, '1');
      TESTER->test_read(data.data(), request_size);
      REQUIRE(TESTER->size_read_orig_ == request_size);
    }
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedReadRandomRSRangeMedium",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-medium]"
              "[repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "][pattern=random][file=1]") {
  TESTER->Pretest();

  SECTION("read from existing file") {
    TESTER->test_open(TESTER->existing_file_, O_RDWR);
    REQUIRE(TESTER->fh_orig_ != -1);
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t offset =
          rand_r(&TESTER->offset_seed_) %
              (TESTER->total_size_ - TESTER->medium_max_);
      TESTER->test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)TESTER->status_orig_) == offset);
      size_t request_size =
          TESTER->medium_min_ +
              (rand_r(&TESTER->rs_seed_) % TESTER->medium_max_);
      std::string data(request_size, '1');
      TESTER->test_read(data.data(), request_size);
      REQUIRE(TESTER->size_read_orig_ == request_size);
    }
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedUpdateRandomRSRangeMedium",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-medium][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=random][file=1]") {
  TESTER->Pretest();

  SECTION("read from existing file") {
    TESTER->test_open(TESTER->existing_file_, O_RDWR);
    REQUIRE(TESTER->fh_orig_ != -1);
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t offset =
          rand_r(&TESTER->offset_seed_) %
              (TESTER->total_size_ - TESTER->medium_max_);
      TESTER->test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)TESTER->status_orig_) == offset);
      size_t request_size =
          TESTER->medium_min_ +
              (rand_r(&TESTER->rs_seed_) % TESTER->medium_max_);
      std::string data(request_size, '1');
      TESTER->test_write(data.c_str(), request_size);
      REQUIRE(TESTER->size_written_orig_ == request_size);
    }
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedReadStrideFixedRSRangeMedium",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-medium][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=stride_fixed][file=1]") {
  TESTER->Pretest();
  SECTION("read from existing file") {
    TESTER->test_open(TESTER->existing_file_, O_RDWR);
    REQUIRE(TESTER->fh_orig_ != -1);
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t offset =
          (i * TESTER->stride_size_) %
              (TESTER->total_size_ - TESTER->medium_max_);
      TESTER->test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)TESTER->status_orig_) == offset);
      size_t request_size =
          TESTER->medium_min_ +
              (rand_r(&TESTER->rs_seed_) % TESTER->medium_max_);
      std::string data(request_size, '1');
      TESTER->test_read(data.data(), request_size);
      REQUIRE(TESTER->size_read_orig_ == request_size);
    }
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedUpdateStrideFixedRSRangeMedium",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-medium][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=stride_fixed][file=1]") {
  TESTER->Pretest();
  SECTION("write to existing file") {
    TESTER->test_open(TESTER->existing_file_, O_RDWR);
    REQUIRE(TESTER->fh_orig_ != -1);
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t offset =
          (i * TESTER->stride_size_) %
              (TESTER->total_size_ - TESTER->medium_max_);
      TESTER->test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)TESTER->status_orig_) == offset);
      size_t request_size =
          TESTER->medium_min_ +
              (rand_r(&TESTER->rs_seed_) % TESTER->medium_max_);
      std::string data(request_size, '1');
      TESTER->test_write(data.c_str(), request_size);
      REQUIRE(TESTER->size_written_orig_ == request_size);
    }
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedReadStrideDynamicRSRangeMedium",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-medium][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=stride_dynamic][file=1]") {
  TESTER->Pretest();
  SECTION("read from existing file") {
    TESTER->test_open(TESTER->existing_file_, O_RDWR);
    REQUIRE(TESTER->fh_orig_ != -1);
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t offset = TESTER->GetRandomOffset(
          i, TESTER->offset_seed_, TESTER->stride_size_,
          TESTER->total_size_ - TESTER->medium_max_);
      TESTER->test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)TESTER->status_orig_) == offset);
      size_t request_size =
          TESTER->medium_min_ +
              (rand_r(&TESTER->rs_seed_) % TESTER->medium_max_);
      std::string data(request_size, '1');
      TESTER->test_read(data.data(), request_size);
      REQUIRE(TESTER->size_read_orig_ == request_size);
    }
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedUpdateStrideDynamicRSRangeMedium",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-medium][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=stride_dynamic][file=1]") {
  TESTER->Pretest();
  SECTION("read from existing file") {
    TESTER->test_open(TESTER->existing_file_, O_RDWR);
    REQUIRE(TESTER->fh_orig_ != -1);
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t offset = TESTER->GetRandomOffset(
          i, TESTER->offset_seed_, TESTER->stride_size_,
          TESTER->total_size_ - TESTER->medium_max_);
      TESTER->test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)TESTER->status_orig_) == offset);
      size_t request_size =
          TESTER->medium_min_ +
              (rand_r(&TESTER->rs_seed_) % TESTER->medium_max_);
      std::string data(request_size, '1');
      TESTER->test_write(data.c_str(), request_size);
      REQUIRE(TESTER->size_written_orig_ == request_size);
    }
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedReadStrideNegativeRSRangeMedium",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-medium][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=stride_negative][file=1]") {
  TESTER->Pretest();
  SECTION("read from existing file") {
    TESTER->test_open(TESTER->existing_file_, O_RDWR);
    REQUIRE(TESTER->fh_orig_ != -1);
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t offset = (TESTER->total_size_ - i * TESTER->stride_size_) %
          (TESTER->total_size_ - TESTER->medium_max_);
      TESTER->test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)TESTER->status_orig_) == offset);
      size_t request_size =
          TESTER->medium_min_ +
              (rand_r(&TESTER->rs_seed_) % TESTER->medium_max_);
      std::string data(request_size, '1');
      TESTER->test_read(data.data(), request_size);
      REQUIRE(TESTER->size_read_orig_ == request_size);
    }
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedUpdateStrideNegativeRSRangeMedium",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-medium][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=stride_negative][file=1]") {
  TESTER->Pretest();
  SECTION("write to existing file") {
    TESTER->test_open(TESTER->existing_file_, O_RDWR);
    REQUIRE(TESTER->fh_orig_ != -1);
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t offset = TESTER->total_size_ - ((i * TESTER->stride_size_) %
          (TESTER->total_size_ - TESTER->medium_max_));
      TESTER->test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)TESTER->status_orig_) == offset);
      size_t request_size =
          TESTER->medium_min_ +
              (rand_r(&TESTER->rs_seed_) % TESTER->medium_max_);
      std::string data(request_size, '1');
      TESTER->test_write(data.c_str(), request_size);
      REQUIRE(TESTER->size_written_orig_ == request_size);
    }
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedReadStride2DRSRangeMedium",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-medium][repetition=" +
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
    TESTER->test_open(TESTER->existing_file_, O_RDWR);
    REQUIRE(TESTER->fh_orig_ != -1);
    size_t prev_cell_col = 0, prev_cell_row = 0;
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t current_cell_col = (prev_cell_col + cell_stride) % cols;
      size_t current_cell_row = prev_cell_col + cell_stride > cols
                                ? prev_cell_row + 1
                                : prev_cell_row;
      prev_cell_row = current_cell_row;
      size_t offset = (current_cell_col * cell_stride + prev_cell_row * cols) %
          (TESTER->total_size_ - TESTER->medium_max_);
      TESTER->test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)TESTER->status_orig_) == offset);
      size_t request_size =
          TESTER->medium_min_ +
              (rand_r(&TESTER->rs_seed_) % TESTER->medium_max_);
      std::string data(request_size, '1');
      TESTER->test_read(data.data(), request_size);
      REQUIRE(TESTER->size_read_orig_ == request_size);
    }
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedUpdateStride2DRSRangeMedium",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-medium][repetition=" +
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
    TESTER->test_open(TESTER->existing_file_, O_RDWR);
    REQUIRE(TESTER->fh_orig_ != -1);
    size_t prev_cell_col = 0, prev_cell_row = 0;
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t current_cell_col = (prev_cell_col + cell_stride) % cols;
      size_t current_cell_row = prev_cell_col + cell_stride > cols
                                ? prev_cell_row + 1
                                : prev_cell_row;
      prev_cell_row = current_cell_row;
      size_t offset = (current_cell_col * cell_stride + prev_cell_row * cols) %
          (TESTER->total_size_ - TESTER->medium_max_);
      TESTER->test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)TESTER->status_orig_) == offset);
      size_t request_size =
          TESTER->medium_min_ +
              (rand_r(&TESTER->rs_seed_) % TESTER->medium_max_);
      std::string data(request_size, '1');
      TESTER->test_write(data.c_str(), request_size);
      REQUIRE(TESTER->size_written_orig_ == request_size);
    }
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}
/**
 * Large RS
 **/

TEST_CASE("BatchedWriteRSRangeLarge",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-large][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=sequential][file=1]") {
  TESTER->Pretest();
  SECTION("write to new file always at the start") {
    TESTER->test_open(TESTER->new_file_, O_WRONLY | O_CREAT, 0600);
    REQUIRE(TESTER->fh_orig_ != -1);
    size_t biggest_size_written = 0;
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      TESTER->test_seek(0, SEEK_SET);
      REQUIRE(TESTER->status_orig_ == 0);
      size_t request_size =
          TESTER->large_min_ +
              (rand_r(&TESTER->rs_seed_) % TESTER->large_max_);
      std::string data(request_size, '1');
      TESTER->test_write(data.c_str(), request_size);
      REQUIRE(TESTER->size_written_orig_ == request_size);
      if (biggest_size_written < request_size)
        biggest_size_written = request_size;
    }
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == 0);
    REQUIRE(stdfs::file_size(TESTER->new_file_.hermes_.c_str()) ==
        biggest_size_written);
  }

  SECTION("write to new file") {
    TESTER->test_open(TESTER->new_file_, O_WRONLY | O_CREAT, 0600);
    REQUIRE(TESTER->fh_orig_ != -1);
    size_t total_size_written = 0;
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t request_size =
          TESTER->large_min_ +
              (rand_r(&TESTER->rs_seed_) % TESTER->large_max_);
      std::string data(request_size, '1');
      TESTER->test_write(data.c_str(), request_size);
      REQUIRE(TESTER->size_written_orig_ == request_size);
      total_size_written += TESTER->size_written_orig_;
    }
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == 0);
    REQUIRE(stdfs::file_size(TESTER->new_file_.hermes_.c_str()) ==
        total_size_written);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedReadSequentialRSRangeLarge",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-large][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=sequential][file=1]") {
  TESTER->Pretest();
  SECTION("read from existing file") {
    TESTER->test_open(TESTER->existing_file_, O_RDONLY);
    REQUIRE(TESTER->fh_orig_ != -1);
    std::string data(TESTER->request_size_, '1');
    size_t current_offset = 0;
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t request_size =
          (TESTER->large_min_ +
              (rand_r(&TESTER->rs_seed_) % TESTER->large_max_)) %
              (TESTER->total_size_ - current_offset);
      std::string data(request_size, '1');
      TESTER->test_read(data.data(), request_size);
      REQUIRE(TESTER->size_read_orig_ == request_size);
      current_offset += TESTER->size_read_orig_;
    }
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == 0);
  }

  SECTION("read from existing file always at start") {
    TESTER->test_open(TESTER->existing_file_, O_RDONLY);
    REQUIRE(TESTER->fh_orig_ != -1);

    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      TESTER->test_seek(0, SEEK_SET);
      REQUIRE(TESTER->status_orig_ == 0);
      size_t request_size =
          TESTER->large_min_ +
              (rand_r(&TESTER->rs_seed_) % TESTER->large_max_);
      std::string data(request_size, '1');
      TESTER->test_read(data.data(), request_size);
      REQUIRE(TESTER->size_read_orig_ == request_size);
    }
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedReadRandomRSRangeLarge",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-large]"
              "[repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "][pattern=random][file=1]") {
  TESTER->Pretest();

  SECTION("read from existing file") {
    TESTER->test_open(TESTER->existing_file_, O_RDWR);
    REQUIRE(TESTER->fh_orig_ != -1);
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t offset =
          rand_r(&TESTER->offset_seed_) %
              (TESTER->total_size_ - TESTER->large_max_);
      TESTER->test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)TESTER->status_orig_) == offset);
      size_t request_size =
          (TESTER->large_min_ +
              (rand_r(&TESTER->rs_seed_) % TESTER->large_max_)) %
              (TESTER->total_size_ - TESTER->status_orig_);
      std::string data(request_size, '1');
      TESTER->test_read(data.data(), request_size);
      REQUIRE(TESTER->size_read_orig_ == request_size);
    }
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedUpdateRandomRSRangeLarge",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-large][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=random][file=1]") {
  TESTER->Pretest();

  SECTION("read from existing file") {
    TESTER->test_open(TESTER->existing_file_, O_RDWR);
    REQUIRE(TESTER->fh_orig_ != -1);
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t offset =
          rand_r(&TESTER->offset_seed_) %
              (TESTER->total_size_ - TESTER->large_max_);
      TESTER->test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)TESTER->status_orig_) == offset);
      size_t request_size =
          TESTER->large_min_ + (rand_r(&TESTER->rs_seed_) % TESTER->large_max_);
      std::string data(request_size, '1');
      TESTER->test_write(data.c_str(), request_size);
      REQUIRE(TESTER->size_written_orig_ == request_size);
    }
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedReadStrideFixedRSRangeLarge",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-large][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=stride_fixed][file=1]") {
  TESTER->Pretest();
  SECTION("read from existing file") {
    TESTER->test_open(TESTER->existing_file_, O_RDWR);
    REQUIRE(TESTER->fh_orig_ != -1);
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t offset =
          (i * TESTER->stride_size_) %
              (TESTER->total_size_ - TESTER->large_max_);
      TESTER->test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)TESTER->status_orig_) == offset);
      size_t request_size =
          TESTER->large_min_ +
              (rand_r(&TESTER->rs_seed_) % TESTER->large_max_);
      std::string data(request_size, '1');
      TESTER->test_read(data.data(), request_size);
      REQUIRE(TESTER->size_read_orig_ == request_size);
    }
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedUpdateStrideFixedRSRangeLarge",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-large][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=stride_fixed][file=1]") {
  TESTER->Pretest();
  SECTION("write to existing file") {
    TESTER->test_open(TESTER->existing_file_, O_RDWR);
    REQUIRE(TESTER->fh_orig_ != -1);
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t offset =
          (i * TESTER->stride_size_) %
              (TESTER->total_size_ - TESTER->large_max_);
      TESTER->test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)TESTER->status_orig_) == offset);
      size_t request_size =
          TESTER->large_min_ + (rand_r(&TESTER->rs_seed_) % TESTER->large_max_);
      std::string data(request_size, '1');
      TESTER->test_write(data.c_str(), request_size);
      REQUIRE(TESTER->size_written_orig_ == request_size);
    }
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedReadStrideDynamicRSRangeLarge",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-large][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=stride_dynamic][file=1]") {
  TESTER->Pretest();
  SECTION("read from existing file") {
    TESTER->test_open(TESTER->existing_file_, O_RDWR);
    REQUIRE(TESTER->fh_orig_ != -1);
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t offset = TESTER->GetRandomOffset(
          i, TESTER->offset_seed_, TESTER->stride_size_,
          TESTER->total_size_ - TESTER->large_max_);
      TESTER->test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)TESTER->status_orig_) == offset);
      size_t request_size =
          TESTER->large_min_ + (rand_r(&TESTER->rs_seed_) % TESTER->large_max_);
      std::string data(request_size, '1');
      TESTER->test_read(data.data(), request_size);
      REQUIRE(TESTER->size_read_orig_ == request_size);
    }
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedUpdateStrideDynamicRSRangeLarge",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-large][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=stride_dynamic][file=1]") {
  TESTER->Pretest();
  SECTION("read from existing file") {
    TESTER->test_open(TESTER->existing_file_, O_RDWR);
    REQUIRE(TESTER->fh_orig_ != -1);
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t offset = TESTER->GetRandomOffset(
          i, TESTER->offset_seed_, TESTER->stride_size_,
          TESTER->total_size_ - TESTER->large_max_);
      TESTER->test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)TESTER->status_orig_) == offset);
      size_t request_size =
          TESTER->large_min_ + (rand_r(&TESTER->rs_seed_) % TESTER->large_max_);
      std::string data(request_size, '1');
      TESTER->test_write(data.c_str(), request_size);
      REQUIRE(TESTER->size_written_orig_ == request_size);
    }
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedReadStrideNegativeRSRangeLarge",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-large][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=stride_negative][file=1]") {
  TESTER->Pretest();
  SECTION("read from existing file") {
    TESTER->test_open(TESTER->existing_file_, O_RDWR);
    REQUIRE(TESTER->fh_orig_ != -1);
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t offset = (TESTER->total_size_ - i * TESTER->stride_size_) %
          (TESTER->total_size_ - TESTER->large_max_);
      TESTER->test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)TESTER->status_orig_) == offset);
      size_t request_size =
          (TESTER->large_min_ +
              (rand_r(&TESTER->rs_seed_) % TESTER->large_max_)) %
              (TESTER->total_size_ - TESTER->large_max_);
      std::string data(request_size, '1');
      TESTER->test_read(data.data(), request_size);
      REQUIRE(TESTER->size_read_orig_ == request_size);
    }
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedUpdateStrideNegativeRSRangeLarge",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-large][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=stride_negative][file=1]") {
  TESTER->Pretest();
  SECTION("write to existing file") {
    TESTER->test_open(TESTER->existing_file_, O_RDWR);
    REQUIRE(TESTER->fh_orig_ != -1);
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t offset = TESTER->total_size_ - ((i * TESTER->stride_size_) %
          (TESTER->total_size_ - TESTER->large_max_));
      TESTER->test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)TESTER->status_orig_) == offset);
      size_t request_size =
          TESTER->large_min_ + (rand_r(&TESTER->rs_seed_) % TESTER->large_max_);
      std::string data(request_size, '1');
      TESTER->test_write(data.c_str(), request_size);
      REQUIRE(TESTER->size_written_orig_ == request_size);
    }
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedReadStride2DRSRangeLarge",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=range-large][repetition=" +
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
    TESTER->test_open(TESTER->existing_file_, O_RDWR);
    REQUIRE(TESTER->fh_orig_ != -1);
    size_t prev_cell_col = 0, prev_cell_row = 0;
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t current_cell_col = (prev_cell_col + cell_stride) % cols;
      size_t current_cell_row = prev_cell_col + cell_stride > cols
                                ? prev_cell_row + 1
                                : prev_cell_row;
      prev_cell_row = current_cell_row;
      size_t offset = (current_cell_col * cell_stride + prev_cell_row * cols) %
          (TESTER->total_size_ - TESTER->large_max_);
      TESTER->test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)TESTER->status_orig_) == offset);
      size_t request_size =
          TESTER->large_min_ + (rand_r(&TESTER->rs_seed_) % TESTER->large_max_);
      std::string data(request_size, '1');
      TESTER->test_read(data.data(), request_size);
      REQUIRE(TESTER->size_read_orig_ == request_size);
    }
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedUpdateStride2DRSRangeLarge",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=range-large][repetition=" +
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
    TESTER->test_open(TESTER->existing_file_, O_RDWR);
    REQUIRE(TESTER->fh_orig_ != -1);
    size_t prev_cell_col = 0, prev_cell_row = 0;
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t current_cell_col = (prev_cell_col + cell_stride) % cols;
      size_t current_cell_row = prev_cell_col + cell_stride > cols
                                ? prev_cell_row + 1
                                : prev_cell_row;
      prev_cell_row = current_cell_row;
      size_t offset = (current_cell_col * cell_stride + prev_cell_row * cols) %
          (TESTER->total_size_ - TESTER->large_max_);
      TESTER->test_seek(offset, SEEK_SET);
      REQUIRE(((size_t)TESTER->status_orig_) == offset);
      size_t request_size =
          TESTER->large_min_ + (rand_r(&TESTER->rs_seed_) % TESTER->large_max_);
      std::string data(request_size, '1');
      TESTER->test_write(data.c_str(), request_size);
      REQUIRE(TESTER->size_written_orig_ == request_size);
    }
    TESTER->test_close();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}
