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

#include <hermes_shm/util/formatter.h>
#include "basic_test.h"

namespace hshm = hermes_shm;

TEST_CASE("TestFormatter") {
  int rank = 0;
  int i = 0;

  PAGE_DIVIDE("Test with equivalent parameters") {
    std::string name = hshm::Formatter::format("bucket{}_{}",
                                               rank, i);
    REQUIRE(name == "bucket0_0");
  }

  PAGE_DIVIDE("Test with equivalent parameters at start") {
    std::string name = hshm::Formatter::format("{}bucket{}",
                                               rank, i);
    REQUIRE(name == "0bucket0");
  }

  PAGE_DIVIDE("Test with too many parameters") {
    std::string name = hshm::Formatter::format("bucket",
                                               rank, i);
    REQUIRE(name == "bucket");
  }

  PAGE_DIVIDE("Test with fewer parameters") {
    std::string name = hshm::Formatter::format("bucket{}_{}",
                                               rank);
    REQUIRE(name == "bucket{}_{}");
  }
}
