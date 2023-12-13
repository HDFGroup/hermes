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

TEST_CASE("BatchedWriteSequentialPersistent",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[hermes_mode=persistent]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=sequential][file=1]") {
  HERMES_CLIENT_CONF.SetBaseAdapterMode(hermes::adapter::AdapterMode::kDefault);
  REQUIRE(HERMES_CLIENT_CONF.GetBaseAdapterMode() ==
          hermes::adapter::AdapterMode::kDefault);
  TESTER->Pretest();
  SECTION("write to new file always at end") {
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

TEST_CASE("BatchedWriteSequentialBypass",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[hermes_mode=bypass]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=sequential][file=1]") {
  HERMES_CLIENT_CONF.SetBaseAdapterMode(hermes::adapter::AdapterMode::kBypass);
  REQUIRE(HERMES_CLIENT_CONF.GetBaseAdapterMode() ==
          hermes::adapter::AdapterMode::kBypass);
  TESTER->Pretest();
  SECTION("write to new file always at end") {
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

TEST_CASE("BatchedWriteSequentialScratch",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[hermes_mode=scratch]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=sequential][file=1]") {
  HERMES_CLIENT_CONF.SetBaseAdapterMode(hermes::adapter::AdapterMode::kScratch);
  REQUIRE(HERMES_CLIENT_CONF.GetBaseAdapterMode() ==
          hermes::adapter::AdapterMode::kScratch);
  TESTER->Pretest();
  SECTION("write to new file always at end") {
    TESTER->test_fopen(TESTER->new_file_, "w+");
    REQUIRE(TESTER->fh_orig_ != nullptr);

    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      TESTER->test_fwrite(TESTER->write_data_.data(), TESTER->request_size_);
      REQUIRE(TESTER->size_written_orig_ == TESTER->request_size_);
    }
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
    REQUIRE(stdfs::exists(TESTER->new_file_.hermes_) == 0);
  }
  TESTER->Posttest(false);
}
