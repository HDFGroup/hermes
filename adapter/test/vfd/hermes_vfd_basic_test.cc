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
    test::TestOpen(info.existing_file, H5F_ACC_RDWR);
    REQUIRE(test::hermes_hid != H5I_INVALID_HID);
    test::TestClose();
    REQUIRE(test::hermes_herr >= 0);

    test::TestOpen(info.existing_file, H5F_ACC_RDONLY);
    REQUIRE(test::hermes_hid != H5I_INVALID_HID);
    test::TestClose();
    REQUIRE(test::hermes_herr >= 0);
  }

  SECTION("create existing file exclusively") {
    MuteHdf5Errors mute;
    bool create = true;
    test::TestOpen(info.existing_file, H5F_ACC_EXCL, create);
    REQUIRE(test::hermes_hid == H5I_INVALID_HID);
  }

  Posttest();
}

TEST_CASE("SingleWrite", "[process=" + std::to_string(info.comm_size) +
                             "]"
                             "[operation=single_write]"
                             "[request_size=type-fixed][repetition=1]"
                             "[file=1]") {
  Pretest();

  // SECTION("write to existing file") {
  //   test::TestOpen(info.existing_file, H5F_ACC_RDWR);
  //   REQUIRE(test::hermes_hid != H5I_INVALID_HID);
  //   test::test_seek(0, SEEK_SET);
  //   REQUIRE(test::status_orig == 0);
  //   test::test_write(info.write_data.data(), args.request_size);
  //   REQUIRE(test::size_written_orig == args.request_size);
  //   test::TestClose();
  //   REQUIRE(test::status_orig == 0);
  // }

  // SECTION("write to new file") {
  //   test::TestOpen(info.new_file, O_WRONLY | O_CREAT | O_EXCL, 0600);
  //   REQUIRE(test::fh_orig != -1);
  //   test::test_write(info.write_data.data(), args.request_size);
  //   REQUIRE(test::size_written_orig == args.request_size);
  //   test::TestClose();
  //   REQUIRE(test::status_orig == 0);
  //   REQUIRE(fs::file_size(info.new_file) == test::size_written_orig);
  // }

  // SECTION("write to existing file with truncate") {
  //   test::TestOpen(info.existing_file, O_WRONLY | O_TRUNC);
  //   REQUIRE(test::fh_orig != -1);
  //   test::test_write(info.write_data.data(), args.request_size);
  //   REQUIRE(test::size_written_orig == args.request_size);
  //   test::TestClose();
  //   REQUIRE(test::status_orig == 0);
  //   REQUIRE(fs::file_size(info.existing_file) == test::size_written_orig);
  // }

  // SECTION("write to existing file at the end") {
  //   test::TestOpen(info.existing_file, O_RDWR);
  //   REQUIRE(test::fh_orig != -1);
  //   test::test_seek(0, SEEK_END);
  //   REQUIRE(((size_t)test::status_orig) ==
  //           args.request_size * info.num_iterations);
  //   test::test_write(info.write_data.data(), args.request_size);
  //   REQUIRE(test::size_written_orig == args.request_size);
  //   test::TestClose();
  //   REQUIRE(test::status_orig == 0);
  //   REQUIRE(fs::file_size(info.existing_file) ==
  //           test::size_written_orig + args.request_size * info.num_iterations);
  // }

  // SECTION("append to existing file") {
  //   auto existing_size = fs::file_size(info.existing_file);
  //   test::TestOpen(info.existing_file, O_RDWR | O_APPEND);
  //   REQUIRE(test::fh_orig != -1);
  //   test::test_write(info.write_data.data(), args.request_size);
  //   REQUIRE(test::size_written_orig == args.request_size);
  //   test::TestClose();
  //   REQUIRE(test::status_orig == 0);
  //   REQUIRE(fs::file_size(info.existing_file) ==
  //           existing_size + test::size_written_orig);
  // }

  // SECTION("append to new file") {
  //   test::TestOpen(info.new_file, O_WRONLY | O_CREAT | O_EXCL, 0600);
  //   REQUIRE(test::fh_orig != -1);
  //   test::test_write(info.write_data.data(), args.request_size);
  //   REQUIRE(test::size_written_orig == args.request_size);
  //   test::TestClose();
  //   REQUIRE(test::status_orig == 0);
  //   REQUIRE(fs::file_size(info.new_file) == test::size_written_orig);
  // }

  Posttest();
}
