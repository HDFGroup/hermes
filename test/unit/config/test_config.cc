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

#include "basic_test.h"
#include "hrun/api/hrun_client.h"
#include "hrun_admin/hrun_admin.h"
#include "hermes/hermes.h"
#include "hermes/bucket.h"
#include "data_stager/factory/binary_stager.h"
#include <mpi.h>

TEST_CASE("TestHermesPaths") {
  PAGE_DIVIDE("Directory path") {
    hermes::config::UserPathInfo info("/home/hello", true, true);
    REQUIRE(info.Match("/home") == false);
    REQUIRE(info.Match("/home/hello") == true);
    REQUIRE(info.Match("/home/hello/hi.txt") == true);
  }

  PAGE_DIVIDE("Simple path") {
    hermes::config::UserPathInfo info("/home/hello.txt", true, false);
    REQUIRE(info.Match("/home/hello") == false);
    REQUIRE(info.Match("/home/hello.txt") == true);
  }

  PAGE_DIVIDE("Wildcard path") {
    hermes::config::UserPathInfo info("/home/hello/*.json", true, false);
    REQUIRE(info.Match("/home/hello") == false);
    REQUIRE(info.Match("/home/hello/hi.json") == true);
    REQUIRE(info.Match("/home/hello/.json") == true);
  }
}

