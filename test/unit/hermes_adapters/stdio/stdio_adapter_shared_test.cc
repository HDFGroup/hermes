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

TEST_CASE("SharedSTDIORead", "[process=" + std::to_string(TESTER->comm_size_) +
    "]"
    "[operation=batched_read]"
    "[request_size=type-fixed][repetition=" +
    std::to_string(TESTER->num_iterations_) +
    "]"
    "[mode=shared]"
    "[pattern=sequential][file=1]") {
  TESTER->Pretest();

  SECTION("read from existing file") {
    TESTER->test_fopen(TESTER->shared_existing_file_, "r+");
    REQUIRE(TESTER->fh_orig_ != nullptr);
    std::string data(TESTER->request_size_, '1');
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      TESTER->test_fread(data.data(), TESTER->request_size_);
      REQUIRE(TESTER->size_read_orig_ == TESTER->request_size_);
    }
    TESTER->test_fclose();
    REQUIRE(TESTER->status_orig_ == 0);
  }
  TESTER->Posttest();
}
