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

TEST_CASE("Hostfile") {
  auto vec = hermes::RpcContext::ParseHostfile("hostfile.txt");
  REQUIRE(vec.size() == 8);
  REQUIRE(vec[0] == "ares-comp-01");
  REQUIRE(vec[1] == "ares-comp-02");
  REQUIRE(vec[2] == "ares-comp-03");
  REQUIRE(vec[3] == "ares-comp-04");
  REQUIRE(vec[4] == "ares-comp-05");
  REQUIRE(vec[5] == "ares-comp-08");
  REQUIRE(vec[6] == "ares-comp-09");
  REQUIRE(vec[7] == "ares-comp-10");
}

TEST_CASE("HostName") {
  SECTION("Simple host name") {
    std::vector<std::string> host_names;
    hshm::ConfigParse::ParseHostNameString("localhost", host_names);
    REQUIRE(host_names.size() == 1);
    REQUIRE(host_names[0] == "localhost");
  }

  SECTION("Host name with range at the end") {
    std::vector<std::string> host_names;
    hshm::ConfigParse::ParseHostNameString("ares-comp-[0-4]",
                                                    host_names);
    REQUIRE(host_names.size() == 5);
    REQUIRE(host_names[0] == "ares-comp-0");
    REQUIRE(host_names[1] == "ares-comp-1");
    REQUIRE(host_names[2] == "ares-comp-2");
    REQUIRE(host_names[3] == "ares-comp-3");
    REQUIRE(host_names[4] == "ares-comp-4");
  }

  SECTION("Host name with fixed number range") {
    std::vector<std::string> host_names;
    hshm::ConfigParse::ParseHostNameString("ares-comp-[08-12]",
                                                    host_names);
    REQUIRE(host_names.size() == 5);
    REQUIRE(host_names[0] == "ares-comp-08");
    REQUIRE(host_names[1] == "ares-comp-09");
    REQUIRE(host_names[2] == "ares-comp-10");
    REQUIRE(host_names[3] == "ares-comp-11");
    REQUIRE(host_names[4] == "ares-comp-12");
  }

  SECTION("Host name with fixed number range and suffix") {
    std::vector<std::string> host_names;
    hshm::ConfigParse::ParseHostNameString("ares-comp-[08-12]-hello",
                                        host_names);
    REQUIRE(host_names.size() == 5);
    REQUIRE(host_names[0] == "ares-comp-08-hello");
    REQUIRE(host_names[1] == "ares-comp-09-hello");
    REQUIRE(host_names[2] == "ares-comp-10-hello");
    REQUIRE(host_names[3] == "ares-comp-11-hello");
    REQUIRE(host_names[4] == "ares-comp-12-hello");
  }

  SECTION("Host name with multiple ranges") {
    std::vector<std::string> host_names;
    hshm::ConfigParse::ParseHostNameString(
        "ares-comp-[08-10,13-14,25]-hello", host_names);
    REQUIRE(host_names.size() == 6);
    REQUIRE(host_names[0] == "ares-comp-08-hello");
    REQUIRE(host_names[1] == "ares-comp-09-hello");
    REQUIRE(host_names[2] == "ares-comp-10-hello");
    REQUIRE(host_names[3] == "ares-comp-13-hello");
    REQUIRE(host_names[4] == "ares-comp-14-hello");
    REQUIRE(host_names[5] == "ares-comp-25-hello");
  }
}
