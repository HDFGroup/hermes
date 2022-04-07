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
using hermes::adapter::vfd::test::AssertFunc;

TEST_CASE("Open", "[process=" + std::to_string(info.comm_size) +
                      "]"
                      "[operation=single_open]"
                      "[repetition=1][file=1]") {
  pretest();
  SECTION("open non-existent file") {
    MuteHdf5Errors mute;
    test::test_open(info.new_file, H5F_ACC_RDONLY);
    REQUIRE(test::hermes_hid == H5I_INVALID_HID);
    test::test_open(info.new_file, H5F_ACC_RDWR);
    REQUIRE(test::hermes_hid == H5I_INVALID_HID);
  }

  SECTION("truncate existing file") {
    bool create = true;
    test::test_open(info.existing_file, H5F_ACC_TRUNC, create);
    REQUIRE(test::hermes_hid != H5I_INVALID_HID);
    test::test_close();
    REQUIRE(test::hermes_herr >= 0);
  }

  // SECTION("open existing file") {
  //   {
  //     hid_t result = api.Open(info.existing_file, H5F_ACC_RDWR);
  //     REQUIRE(result != H5I_INVALID_HID);
  //     herr_t err = api.Close(result);
  //     REQUIRE(err >= 0);
  //   }

  //   {
  //     hid_t result = api.Open(info.existing_file, H5F_ACC_RDONLY);
  //     REQUIRE(result != H5I_INVALID_HID);
  //     herr_t err = api.Close(result);
  //     REQUIRE(err >= 0);
  //   }
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
