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

/** Returns a number in the range [1, upper_bound] */
static inline size_t Random1ToUpperBound(size_t upper_bound) {
  size_t result = ((size_t)GenNextRandom() % upper_bound) + 1;

  return result;
}

/** Returns a string in the range ["0", "upper_bound") */
static inline std::string RandomDatasetName(size_t upper_bound) {
  size_t dset_index = Random1ToUpperBound(upper_bound) - 1;
  std::string result = std::to_string(dset_index);

  return result;
}

TEST_CASE("H5FOpen", "[process=" + std::to_string(info.comm_size) +
                      "]"
                      "[operation=single_open]"
                      "[repetition=1][file=1]") {
  Pretest();
  SECTION("open non-existent file") {
    MuteHdf5Errors mute;
    HILOG(kInfo, "Why? Why? Why?")
    test::TestOpen(info.new_file, H5F_ACC_RDONLY);
    REQUIRE(test::hermes_hid == H5I_INVALID_HID);
    test::TestOpen(info.new_file, H5F_ACC_RDWR);
    REQUIRE(test::hermes_hid == H5I_INVALID_HID);
  }

  SECTION("truncate existing file") {
    test::TestOpen(info.existing_file, H5F_ACC_TRUNC, true);
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
    test::TestOpen(info.existing_file, H5F_ACC_EXCL, true);
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

  SECTION("overwrite dataset in existing file") {
    test::TestOpen(info.existing_file, H5F_ACC_RDWR);
    REQUIRE(test::hermes_hid != H5I_INVALID_HID);
    test::TestWritePartial1d("0", info.write_data.data(), 0,
                             info.nelems_per_dataset);
    test::TestClose();
    REQUIRE(test::hermes_herr >= 0);
  }

  SECTION("write to new file") {
    test::TestOpen(info.new_file, H5F_ACC_EXCL, true);
    REQUIRE(test::hermes_hid != H5I_INVALID_HID);
    test::TestWriteDataset("0", info.write_data);
    test::TestClose();
    REQUIRE(test::hermes_herr >= 0);
  }

  SECTION("write to existing file with truncate") {
    test::TestOpen(info.existing_file, H5F_ACC_TRUNC, true);
    REQUIRE(test::hermes_hid != H5I_INVALID_HID);
    test::TestWriteDataset("0", info.write_data);
    test::TestClose();
    REQUIRE(test::hermes_herr >= 0);
  }

  SECTION("add dataset to existing file") {
     test::TestOpen(info.existing_file, H5F_ACC_RDWR);
    REQUIRE(test::hermes_hid != H5I_INVALID_HID);
    test::TestWriteDataset(std::to_string(info.num_iterations),
                           info.write_data);
    test::TestClose();
    REQUIRE(test::hermes_herr >= 0);
  }

  Posttest();
}

TEST_CASE("SingleRead", "[process=" + std::to_string(info.comm_size) +
                            "]"
                            "[operation=single_read]"
                            "[request_size=type-fixed][repetition=1]"
                            "[file=1]") {
  Pretest();
  SECTION("read from non-existing file") {
    MuteHdf5Errors mute;
    test::TestOpen(info.new_file, H5F_ACC_RDONLY);
    REQUIRE(test::hermes_hid == H5I_INVALID_HID);
  }

  SECTION("read first dataset from existing file") {
    test::TestOpen(info.existing_file, H5F_ACC_RDONLY);
    REQUIRE(test::hermes_hid != H5I_INVALID_HID);
    test::TestRead("0", info.read_data, 0, info.nelems_per_dataset);
    test::TestClose();
    REQUIRE(test::hermes_herr >= 0);
  }

  SECTION("read last dataset of existing file") {
    test::TestOpen(info.existing_file, H5F_ACC_RDONLY);
    REQUIRE(test::hermes_hid != H5I_INVALID_HID);
    test::TestRead(std::to_string(info.num_iterations - 1), info.read_data, 0,
                   info.nelems_per_dataset);
    test::TestClose();
    REQUIRE(test::hermes_herr >= 0);
  }
  Posttest();
}

TEST_CASE("BatchedWriteSequential",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_write]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=sequential][file=1]") {
  Pretest();
  SECTION("write to new file") {
    test::TestOpen(info.new_file, H5F_ACC_EXCL, true);
    REQUIRE(test::hermes_hid != H5I_INVALID_HID);

    for (size_t i = 0; i < info.num_iterations; ++i) {
      test::TestWriteDataset(std::to_string(i), info.write_data);
    }
    test::TestClose();
    REQUIRE(test::hermes_herr >= 0);
  }

  SECTION("overwrite first dataset") {
    test::TestOpen(info.new_file, H5F_ACC_EXCL, true);
    REQUIRE(test::hermes_hid != H5I_INVALID_HID);

    test::TestWriteDataset("0", info.write_data);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      test::TestWritePartial1d("0", info.write_data.data(), 0,
                               info.nelems_per_dataset);
    }
    test::TestClose();
    REQUIRE(test::hermes_herr >= 0);
  }
  Posttest();
}

TEST_CASE("BatchedReadSequential",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_read]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=sequential][file=1]") {
  Pretest();
  SECTION("read from existing file") {
    test::TestOpen(info.existing_file, H5F_ACC_RDWR);
    REQUIRE(test::hermes_hid != H5I_INVALID_HID);

    std::vector<f32> buf(info.nelems_per_dataset, 0.0f);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      test::TestRead(std::to_string(i), buf, 0, info.nelems_per_dataset);
    }
    test::TestClose();
    REQUIRE(test::hermes_herr >= 0);
  }

  SECTION("read from existing file always at start") {
    test::TestOpen(info.existing_file, H5F_ACC_RDWR);
    REQUIRE(test::hermes_hid != H5I_INVALID_HID);

    for (size_t i = 0; i < info.num_iterations; ++i) {
      test::TestRead("0", info.read_data, 0, info.nelems_per_dataset);
    }
    test::TestClose();
    REQUIRE(test::hermes_herr >= 0);
  }
  Posttest();
}

TEST_CASE("BatchedReadRandom", "[process=" + std::to_string(info.comm_size) +
                                   "]"
                                   "[operation=batched_read]"
                                   "[request_size=type-fixed]"
                                   "[repetition=" +
                                   std::to_string(info.num_iterations) +
                                   "][pattern=random][file=1]") {
  Pretest();
  SECTION("read from existing file") {
    test::TestOpen(info.existing_file, H5F_ACC_RDWR);
    REQUIRE(test::hermes_hid != H5I_INVALID_HID);

    std::vector<f32> buf(info.nelems_per_dataset, 0.0f);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      u32 dataset = GenNextRandom() % info.num_iterations;
      test::TestRead(std::to_string(dataset), buf, 0, info.nelems_per_dataset);
    }
    test::TestClose();
    REQUIRE(test::hermes_herr >= 0);
  }
  Posttest();
}

TEST_CASE("BatchedUpdateRandom", "[process=" + std::to_string(info.comm_size) +
                                     "]"
                                     "[operation=batched_write]"
                                     "[request_size=type-fixed][repetition=" +
                                     std::to_string(info.num_iterations) +
                                     "]"
                                     "[pattern=random][file=1]") {
  Pretest();
  SECTION("update entire dataset in existing file") {
    test::TestOpen(info.existing_file, H5F_ACC_RDWR);
    REQUIRE(test::hermes_hid != H5I_INVALID_HID);

    for (size_t i = 0; i < info.num_iterations; ++i) {
      u32 dataset = GenNextRandom() % info.num_iterations;
      test::TestWritePartial1d(std::to_string(dataset), info.write_data.data(),
                               0, info.nelems_per_dataset);
    }
    test::TestClose();
    REQUIRE(test::hermes_herr >= 0);
  }

  SECTION("update partial dataset in existing file") {
    test::TestOpen(info.existing_file, H5F_ACC_RDWR);
    REQUIRE(test::hermes_hid != H5I_INVALID_HID);

    for (size_t i = 0; i < info.num_iterations; ++i) {
      u32 dataset = GenNextRandom() % info.num_iterations;
      // NOTE(chogan): Subtract 1 from size so we're always writing at least 1
      // element
      hsize_t offset = GenNextRandom() % (info.write_data.size() - 1);
      hsize_t elements_to_write = info.write_data.size() - offset;
      test::TestWritePartial1d(std::to_string(dataset), info.write_data.data(),
                               offset, elements_to_write);
    }

    test::TestClose();
    REQUIRE(test::hermes_herr >= 0);
  }
  Posttest();
}

TEST_CASE("BatchedWriteRSVariable",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_write]"
              "[request_size=type-variable][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=sequential][file=1]") {
  Pretest();

  SECTION("write to new file always at the start") {
    MuteHdf5Errors mute;
    test::TestOpen(info.new_file, H5F_ACC_EXCL, true);
    REQUIRE(test::hermes_hid != H5I_INVALID_HID);

    for (size_t i = 0; i < info.num_iterations; ++i) {
      size_t request_size = Random1ToUpperBound(info.nelems_per_dataset);
      std::vector<f32> data(request_size, 2.0f);
      test::TestWritePartial1d(std::to_string(i), data.data(), 0, request_size);
    }
    test::TestClose();
    REQUIRE(test::hermes_herr >= 0);
  }

  Posttest();
}

TEST_CASE("BatchedReadSequentialRSVariable",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_read]"
              "[request_size=type-variable][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=sequential][file=1]") {
  Pretest();

  SECTION("read from existing file") {
    test::TestOpen(info.existing_file, H5F_ACC_RDONLY);
    REQUIRE(test::hermes_hid != H5I_INVALID_HID);

    for (size_t i = 0; i < info.num_iterations; ++i) {
      size_t request_size = Random1ToUpperBound(info.nelems_per_dataset);
      size_t starting_element =  info.nelems_per_dataset - request_size;
      std::vector<f32> data(request_size, 1.5f);
      test::TestRead(std::to_string(i), data, starting_element, request_size);
    }
    test::TestClose();
    REQUIRE(test::hermes_herr >= 0);
  }

  SECTION("read from existing file always at start") {
    test::TestOpen(info.existing_file, H5F_ACC_RDONLY);
    REQUIRE(test::hermes_hid != H5I_INVALID_HID);

    for (size_t i = 0; i < info.num_iterations; ++i) {
      size_t request_size = Random1ToUpperBound(info.nelems_per_dataset);
      std::vector<f32> data(request_size, 3.0f);
      test::TestRead(std::to_string(i), data, 0, request_size);
    }
    test::TestClose();
    REQUIRE(test::hermes_herr >= 0);
  }

  Posttest();
}

TEST_CASE("BatchedReadRandomRSVariable",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_read]"
              "[request_size=type-variable]"
              "[repetition=" +
              std::to_string(info.num_iterations) +
              "][pattern=random][file=1]") {
  Pretest();

  SECTION("read from existing file") {
    test::TestOpen(info.existing_file, H5F_ACC_RDWR);
    REQUIRE(test::hermes_hid != H5I_INVALID_HID);

    std::vector<f32> data(info.nelems_per_dataset, 5.0f);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      std::string dset_name = RandomDatasetName(info.num_iterations);
      size_t starting_element = Random1ToUpperBound(info.nelems_per_dataset);
      size_t request_elements =
        Random1ToUpperBound(info.nelems_per_dataset - starting_element);
      std::vector<f32> data(request_elements, 3.8f);
      test::TestRead(dset_name, data, starting_element, request_elements);
    }
    test::TestClose();
    REQUIRE(test::hermes_herr >= 0);
  }

  Posttest();
}

TEST_CASE("BatchedUpdateRandomRSVariable",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_write]"
              "[request_size=type-variable][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=random][file=1]") {
  Pretest();

  SECTION("write to existing file") {
    test::TestOpen(info.existing_file, H5F_ACC_RDWR);
    REQUIRE(test::hermes_hid != H5I_INVALID_HID);

    std::vector<f32> data(info.nelems_per_dataset, 8.0f);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      std::string dset_name = RandomDatasetName(info.num_iterations);
      size_t request_size = Random1ToUpperBound(info.nelems_per_dataset);
      test::TestWritePartial1d(dset_name, data.data(), 0, request_size);
    }
    test::TestClose();
    REQUIRE(test::hermes_herr >= 0);
  }

  Posttest();
}

TEST_CASE("BatchedWriteTemporalFixed",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_write]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=sequential][file=1][temporal=fixed]") {
  Pretest();

  SECTION("write to existing file") {
    test::TestOpen(info.existing_file, H5F_ACC_RDWR);
    REQUIRE(test::hermes_hid != H5I_INVALID_HID);

    for (size_t i = 0; i < info.num_iterations; ++i) {
      usleep(info.temporal_interval_ms * 1000);
      test::TestWritePartial1d(std::to_string(i), info.write_data.data(), 0,
                               info.nelems_per_dataset);
    }
    test::TestClose();
    REQUIRE(test::hermes_herr >= 0);
  }

  SECTION("write to new file always at start") {
    test::TestOpen(info.new_file, H5F_ACC_EXCL, true);
    REQUIRE(test::hermes_hid != H5I_INVALID_HID);

    for (size_t i = 0; i < info.num_iterations; ++i) {
      usleep(info.temporal_interval_ms * 1000);
      test::TestWriteDataset(std::to_string(i), info.write_data);
    }
    test::TestClose();
  }

  Posttest();
}

TEST_CASE("BatchedWriteTemporalVariable",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_write]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=sequential][file=1][temporal=variable]") {
  Pretest();

  SECTION("write to existing file") {
    test::TestOpen(info.existing_file, H5F_ACC_RDWR);
    REQUIRE(test::hermes_hid != H5I_INVALID_HID);

    for (size_t i = 0; i < info.num_iterations; ++i) {
      size_t sleep_interval_ms =
        GenNextRandom() % (info.temporal_interval_ms + 2);
      usleep(sleep_interval_ms * 1000);
      test::TestWritePartial1d(std::to_string(i), info.write_data.data(), 0,
                               info.nelems_per_dataset);
    }
    test::TestClose();
    REQUIRE(test::hermes_herr >= 0);
  }

  SECTION("write to new file always at start") {
    test::TestOpen(info.new_file, H5F_ACC_EXCL, true);
    REQUIRE(test::hermes_hid != H5I_INVALID_HID);

    for (size_t i = 0; i < info.num_iterations; ++i) {
      size_t sleep_interval_ms =
        GenNextRandom() % (info.temporal_interval_ms + 2);
      usleep(sleep_interval_ms * 1000);
      test::TestWriteDataset(std::to_string(i), info.write_data);
    }
    test::TestClose();
    REQUIRE(test::hermes_herr >= 0);
  }

  Posttest();
}

TEST_CASE("BatchedMixedSequential",
          "[process=" + std::to_string(info.comm_size) +
              "]"
              "[operation=batched_mixed]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(info.num_iterations) +
              "]"
              "[pattern=sequential][file=1]") {
  Pretest();

  SECTION("read after write on new file") {
    test::TestOpen(info.new_file, H5F_ACC_EXCL, true);
    REQUIRE(test::hermes_hid != H5I_INVALID_HID);

    for (size_t i = 0; i < info.num_iterations; ++i) {
      std::string dset_name = std::to_string(i);
      test::TestWriteDataset(dset_name, info.write_data);
      test::TestRead(dset_name, info.read_data, 0, info.nelems_per_dataset);
    }
    test::TestClose();
    REQUIRE(test::hermes_herr >= 0);
  }

  SECTION("alternate write and read existing file") {
    test::TestOpen(info.existing_file, H5F_ACC_RDWR);
    REQUIRE(test::hermes_hid != H5I_INVALID_HID);

    for (size_t i = 0; i < info.num_iterations; ++i) {
      std::string dset_name = std::to_string(i);

      if (i % 2 == 0) {
        test::TestWritePartial1d(dset_name, info.write_data.data(), 0,
                                 info.nelems_per_dataset);
      } else {
        test::TestRead(dset_name, info.read_data, 0, info.nelems_per_dataset);
      }
    }
    test::TestClose();
    REQUIRE(test::hermes_herr >= 0);
  }

  SECTION("update after read existing file") {
    test::TestOpen(info.existing_file, H5F_ACC_RDWR);
    REQUIRE(test::hermes_hid != H5I_INVALID_HID);

    for (size_t i = 0; i < info.num_iterations; ++i) {
      std::string dset_name = std::to_string(i);
      test::TestRead(dset_name, info.read_data, 0, info.nelems_per_dataset);
      test::TestWritePartial1d(dset_name, info.write_data.data(), 0,
                               info.nelems_per_dataset);
    }

    test::TestClose();
    REQUIRE(test::hermes_herr >= 0);
  }

  SECTION("read all after write all on new file in single open") {
    test::TestOpen(info.new_file, H5F_ACC_EXCL, true);
    REQUIRE(test::hermes_hid != H5I_INVALID_HID);

    for (size_t i = 0; i < info.num_iterations; ++i) {
      std::string dset_name = std::to_string(i);
      test::TestWriteDataset(dset_name, info.write_data);
    }

    for (size_t i = 0; i < info.num_iterations; ++i) {
      std::string dset_name = std::to_string(i);
      test::TestRead(dset_name, info.read_data, 0, info.nelems_per_dataset);
    }

    test::TestClose();
    REQUIRE(test::hermes_herr >= 0);
  }

  SECTION("read all after write all on new file in different open") {
    test::TestOpen(info.new_file, H5F_ACC_EXCL, true);
    REQUIRE(test::hermes_hid != H5I_INVALID_HID);

    for (size_t i = 0; i < info.num_iterations; ++i) {
      test::TestWriteDataset(std::to_string(i), info.write_data);
    }

    test::TestClose();
    REQUIRE(test::hermes_herr >= 0);

    test::TestOpen(info.new_file, H5F_ACC_RDWR);
    REQUIRE(test::hermes_hid != H5I_INVALID_HID);

    for (size_t i = 0; i < info.num_iterations; ++i) {
      test::TestRead(std::to_string(i), info.read_data, 0,
                     info.nelems_per_dataset);
    }

    test::TestClose();
    REQUIRE(test::hermes_herr >= 0);
  }
  Posttest();
}

TEST_CASE("SingleMixed", "[process=" + std::to_string(info.comm_size) +
                             "][operation=single_mixed]"
                             "[request_size=type-fixed][repetition=1]"
                             "[file=1]") {
  Pretest();

  SECTION("read after write from new file") {
    test::TestOpen(info.new_file, H5F_ACC_EXCL, true);
    REQUIRE(test::hermes_hid != H5I_INVALID_HID);
    std::string dset_name("0");
    test::TestWriteDataset(dset_name, info.write_data);
    test::TestRead(dset_name, info.read_data, 0, info.nelems_per_dataset);
    test::TestClose();
    REQUIRE(test::hermes_herr >= 0);
  }

  SECTION("update after read from existing file") {
    test::TestOpen(info.existing_file, H5F_ACC_RDWR);
    REQUIRE(test::hermes_hid != H5I_INVALID_HID);
    std::string dset_name("0");
    test::TestRead(dset_name, info.read_data, 0, info.nelems_per_dataset);
    test::TestWritePartial1d(dset_name, info.write_data.data(), 0,
                             info.nelems_per_dataset);
    test::TestClose();
    REQUIRE(test::hermes_herr >= 0);
  }

  SECTION("read after write from new file different opens") {
    test::TestOpen(info.new_file, H5F_ACC_EXCL, true);
    REQUIRE(test::hermes_hid != H5I_INVALID_HID);
    std::string dset_name("0");
    test::TestWriteDataset(dset_name, info.write_data);
    test::TestClose();
    REQUIRE(test::hermes_herr >= 0);

    test::TestOpen(info.new_file, H5F_ACC_RDWR);
    test::TestRead(dset_name, info.read_data, 0, info.nelems_per_dataset);
    test::TestClose();
    REQUIRE(test::hermes_herr >= 0);
  }

  Posttest();
}

TEST_CASE("CompactDatasets") {
  Pretest();

  SECTION("create many and read randomly") {
    test::TestOpen(info.new_file, H5F_ACC_EXCL, true);
    REQUIRE(test::hermes_hid != H5I_INVALID_HID);

    size_t num_elements = KILOBYTES(32) / sizeof(f32);

    for (size_t i = 0; i < info.num_iterations; ++i) {
      std::vector<f32> data(num_elements);
      for (size_t i = 0; i < data.size(); ++i) {
        data[i] = GenRandom0to1();
      }
      test::TestMakeCompactDataset(std::to_string(i), data);
    }

    std::vector<f32> read_buf(num_elements, 0.0f);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      std::string dset_name = RandomDatasetName(info.num_iterations);
      test::TestRead(dset_name, read_buf, 0, num_elements);
    }

    test::TestClose();
    REQUIRE(test::hermes_herr >= 0);
  }

  Posttest();
}

TEST_CASE("PartialUpdateToLastPage") {
  Pretest();

  SECTION("beginning of last page") {
    test::TestOpen(info.existing_file, H5F_ACC_RDWR);
    REQUIRE(test::hermes_hid != H5I_INVALID_HID);
    test::TestWritePartial1d(std::to_string(info.num_iterations - 1),
                             info.write_data.data(), 0,
                             info.nelems_per_dataset / 2);
    test::TestClose();
    REQUIRE(test::hermes_herr >= 0);
  }

  SECTION("in middle of last page") {
    test::TestOpen(info.existing_file, H5F_ACC_RDWR);
    REQUIRE(test::hermes_hid != H5I_INVALID_HID);
    test::TestWritePartial1d(std::to_string(info.num_iterations - 1),
                             info.write_data.data(),
                             info.nelems_per_dataset / 4,
                             info.nelems_per_dataset / 2);
    test::TestClose();
    REQUIRE(test::hermes_herr >= 0);
  }

  SECTION("at end of last page") {
    test::TestOpen(info.existing_file, H5F_ACC_RDWR);
    REQUIRE(test::hermes_hid != H5I_INVALID_HID);
    test::TestWritePartial1d(std::to_string(info.num_iterations - 1),
                             info.write_data.data(),
                             info.nelems_per_dataset / 2,
                             info.nelems_per_dataset / 2);
    test::TestClose();
    REQUIRE(test::hermes_herr >= 0);
  }

  Posttest();
}

TEST_CASE("ScratchMode", "[scratch]") {
  Pretest();

  SECTION("created files shouldn't persist") {
    test::TestOpen(info.new_file, H5F_ACC_EXCL, true);
    REQUIRE(test::hermes_hid != H5I_INVALID_HID);

    for (size_t i = 0; i < info.num_iterations; ++i) {
      test::TestWriteDataset(std::to_string(i), info.write_data);
    }

    for (size_t i = 0; i < info.num_iterations; ++i) {
      std::string dset_name = RandomDatasetName(info.num_iterations);
      test::TestRead(dset_name, info.read_data, 0, info.nelems_per_dataset);
    }

    test::TestClose();
    REQUIRE(test::hermes_herr >= 0);

    if (HERMES->client_config_.GetBaseAdapterMode()
        == hermes::adapter::AdapterMode::kScratch) {
      REQUIRE(!stdfs::exists(info.new_file));
    }
  }

  Posttest();
}
