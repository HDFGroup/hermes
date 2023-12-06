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
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=sequential][file=1]") {
  TEST_INFO->Pretest();
  SECTION("write to new file one big write") {
    TEST_INFO->test_fopen(TEST_INFO->new_file_, "w+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    auto write_size = TEST_INFO->request_size_ * (TEST_INFO->num_iterations_ + 1);
    TEST_INFO->write_data_ = TEST_INFO->GenRandom(write_size);
    TEST_INFO->test_fwrite(TEST_INFO->write_data_.data(), write_size);
    REQUIRE(TEST_INFO->size_written_orig_ == write_size);
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
    REQUIRE(stdfs::file_size(TEST_INFO->new_file_.hermes_) == write_size);
  }
  SECTION("write to new file multiple write") {
    TEST_INFO->test_fopen(TEST_INFO->new_file_, "w+");
    REQUIRE(TEST_INFO->fh_orig_ != nullptr);
    for (size_t i = 0; i <= TEST_INFO->num_iterations_; ++i) {
      TEST_INFO->test_fwrite(TEST_INFO->write_data_.data(), TEST_INFO->request_size_);
      REQUIRE(TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    }
    REQUIRE(TEST_INFO->size_written_orig_ == TEST_INFO->request_size_);
    TEST_INFO->test_fclose();
    REQUIRE(TEST_INFO->status_orig_ == 0);
    REQUIRE(stdfs::file_size(TEST_INFO->new_file_.hermes_) ==
            TEST_INFO->request_size_ * (TEST_INFO->num_iterations_ + 1));
  }
  TEST_INFO->Posttest();
}
