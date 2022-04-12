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

using hermes::adapter::vfd::test::MuteHdf5Errors;

TEST_CASE("H5FOpen", "[process=" + std::to_string(info.comm_size) +
                      "]"
                      "[operation=single_open]"
                      "[repetition=1][file=1]") {
  Pretest();
  SECTION("open non-existent file") {
    MuteHdf5Errors mute;
    test::TestOpen(info.new_file, H5F_ACC_RDONLY);
    REQUIRE(test::hermes_hid == H5I_INVALID_HID);
    test::TestOpen(info.new_file, H5F_ACC_RDWR);
    REQUIRE(test::hermes_hid == H5I_INVALID_HID);
  }

  SECTION("truncate existing file") {
    bool create = true;
    test::TestOpen(info.existing_file, H5F_ACC_TRUNC, create);
    REQUIRE(test::hermes_hid != H5I_INVALID_HID);
    test::TestClose();
    REQUIRE(test::hermes_herr >= 0);
  }

  SECTION("open existing file") {
    {
      test::TestOpen(info.existing_file, H5F_ACC_RDWR);
      REQUIRE(test::hermes_hid != H5I_INVALID_HID);
      test::TestClose();
      REQUIRE(test::hermes_herr >= 0);
    }

    {
      test::TestOpen(info.existing_file, H5F_ACC_RDONLY);
      REQUIRE(test::hermes_hid != H5I_INVALID_HID);
      test::TestClose();
      REQUIRE(test::hermes_herr >= 0);
    }
  }
  Posttest();
}
