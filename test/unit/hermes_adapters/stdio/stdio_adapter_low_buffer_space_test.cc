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

TEST_CASE("LowBufferSpace",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=sequential][file=1]") {
  TESTER->Pretest();
  SECTION("write to new file one big write") {
    TESTER->test_fopen(TESTER->new_file_, "w+");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    auto write_size = TESTER->request_size_ * (TESTER->num_iterations_ + 1);
    TESTER->write_data_ = TESTER->GenRandom(write_size);
    TESTER->test_fwrite(TESTER->write_data_.data(), write_size);
    REQUIRE(TESTER->size_written_orig_ == write_size);
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
    REQUIRE(stdfs::file_size(TESTER->new_file_.hermes_) == write_size);
  }
  SECTION("write to new file multiple write") {
    TESTER->test_fopen(TESTER->new_file_, "w+");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    for (size_t i = 0; i <= TESTER->num_iterations_; ++i) {
      TESTER->test_fwrite(TESTER->write_data_.data(), TESTER->request_size_);
      REQUIRE(TESTER->size_written_orig_ == TESTER->request_size_);
    }
    REQUIRE(TESTER->size_written_orig_ == TESTER->request_size_);
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
    REQUIRE(stdfs::file_size(TESTER->new_file_.hermes_) ==
        TESTER->request_size_ * (TESTER->num_iterations_ + 1));
  }
  TESTER->Posttest();
}
