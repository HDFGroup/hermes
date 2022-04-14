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

  SECTION("write to existing file") {
    test::TestOpen(info.existing_file, H5F_ACC_RDWR);
    REQUIRE(test::hermes_hid != H5I_INVALID_HID);

    // overwrite the first args.request_size bytes with info.write_data
    hid_t dset_id = H5Dopen2(test::hermes_hid, "0", H5P_DEFAULT);
    REQUIRE(dset_id != H5I_INVALID_HID);
    hsize_t num_elements = args.request_size / sizeof(float);
    hid_t memspace_id = H5Screate_simple(1, &num_elements, NULL);
    REQUIRE(memspace_id != H5I_INVALID_HID);
    hid_t dspace_id = H5Dget_space(dset_id);
    REQUIRE(dspace_id != H5I_INVALID_HID);
    std::vector<hsize_t> offset = {0};
    std::vector<hsize_t> stride = {1};
    std::vector<hsize_t> count = {args.request_size / sizeof(float)};
    // std::vector<hsize_t> block = {1};
    herr_t status =
      H5Sselect_hyperslab(dspace_id, H5S_SELECT_SET, offset.data(),
                          stride.data(), count.data(), NULL);
    REQUIRE(status >= 0);
    status = H5Dwrite(dset_id, H5T_NATIVE_FLOAT, memspace_id, dspace_id,
                      H5P_DEFAULT, info.write_data.data());
    REQUIRE(status >= 0);

    // TODO Close all the things
    status = H5Sclose(memspace_id);
    REQUIRE(status >= 0);
    status = H5Sclose(dspace_id);
    REQUIRE(status >= 0);
    status = H5Dclose(dset_id);
    REQUIRE(status >= 0);

    {
      // TODO do the same thing on sec2_hid
      hid_t dset_id = H5Dopen2(test::sec2_hid, "0", H5P_DEFAULT);
      REQUIRE(dset_id != H5I_INVALID_HID);
      hsize_t num_elements = args.request_size / sizeof(float);
      hid_t memspace_id = H5Screate_simple(1, &num_elements, NULL);
      REQUIRE(memspace_id != H5I_INVALID_HID);
      hid_t dspace_id = H5Dget_space(dset_id);
      REQUIRE(dspace_id != H5I_INVALID_HID);
      std::vector<hsize_t> offset = {0};
      std::vector<hsize_t> stride = {1};
      std::vector<hsize_t> count = {args.request_size / sizeof(float)};
      // std::vector<hsize_t> block = {1};
      herr_t status =
        H5Sselect_hyperslab(dspace_id, H5S_SELECT_SET, offset.data(),
                            stride.data(), count.data(), NULL);
      REQUIRE(status >= 0);
      status = H5Dwrite(dset_id, H5T_NATIVE_FLOAT, memspace_id, dspace_id,
                        H5P_DEFAULT, info.write_data.data());
      REQUIRE(status >= 0);

      // TODO Close all the things
      status = H5Sclose(memspace_id);
      REQUIRE(status >= 0);
      status = H5Sclose(dspace_id);
      REQUIRE(status >= 0);
      status = H5Dclose(dset_id);
      REQUIRE(status >= 0);
    }

    test::TestClose();
    REQUIRE(test::hermes_herr >= 0);
  }

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
  //         test::size_written_orig + args.request_size * info.num_iterations);
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
