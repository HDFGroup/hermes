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

using hermes::adapter::vfd::test::VfdApi;
using hermes::adapter::vfd::test::MuteHdf5Errors;

TEST_CASE("Open", "[process=" + std::to_string(info.comm_size) +
                      "]"
                      "[operation=single_open]"
                      "[repetition=1][file=1]") {
  pretest();
  VfdApi api;
  SECTION("open non-existent file") {
    MuteHdf5Errors mute;
    hid_t result = api.Open(info.new_file, H5F_ACC_RDONLY);
    REQUIRE(result == H5I_INVALID_HID);
    result = api.Open(info.new_file.c_str(), H5F_ACC_RDWR);
    REQUIRE(result == H5I_INVALID_HID);
  }

  // SECTION("truncate existing file and write-only") {
  //   test::test_fopen(info.existing_file.c_str(), "w");
  //   REQUIRE(test::fh_orig != nullptr);
  //   test::test_fclose();
  //   REQUIRE(test::status_orig == 0);
  // }

  // SECTION("truncate existing file and read/write") {
  //   test::test_fopen(info.existing_file.c_str(), "w+");
  //   REQUIRE(test::fh_orig != nullptr);
  //   test::test_fclose();
  //   REQUIRE(test::status_orig == 0);
  // }

  // SECTION("open existing file") {
  //   test::test_fopen(info.existing_file.c_str(), "r+");
  //   REQUIRE(test::fh_orig != nullptr);
  //   test::test_fclose();
  //   REQUIRE(test::status_orig == 0);
  //   test::test_fopen(info.existing_file.c_str(), "r");
  //   REQUIRE(test::fh_orig != nullptr);
  //   test::test_fclose();
  //   REQUIRE(test::status_orig == 0);
  // }

  // SECTION("append write existing file") {
  //   test::test_fopen(info.existing_file.c_str(), "a");
  //   REQUIRE(test::fh_orig != nullptr);
  //   test::test_fclose();
  //   REQUIRE(test::status_orig == 0);
  // }

  // SECTION("append write and read existing file") {
  //   test::test_fopen(info.existing_file.c_str(), "a+");
  //   REQUIRE(test::fh_orig != nullptr);
  //   test::test_fclose();
  //   REQUIRE(test::status_orig == 0);
  // }
  posttest();
}
