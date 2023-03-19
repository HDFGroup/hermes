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

#include "hermes.h"
#include "basic_test.h"

void MainPretest() {
}

void MainPosttest() {
}

TEST_CASE("AresConfig") {
  auto vec = hermes::RpcContext::ParseHostfile("hostfile.txt");
  REQUIRE(vec.size() == 2);
  REQUIRE(vec[0] == "ares-comp-01");
  REQUIRE(vec[1] == "ares-comp-02");
}
