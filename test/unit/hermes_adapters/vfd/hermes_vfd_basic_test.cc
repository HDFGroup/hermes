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

#include "hermes_vfd_test.h"
using hermes::adapter::fs::test::MuteHdf5Errors;

/** Returns a number in the range [1, upper_bound] */
static inline size_t Random1ToUpperBound(size_t upper_bound) {
  size_t result = ((size_t)TEST_INFO->GenNextRandom() % upper_bound) + 1;

  return result;
}

/** Returns a string in the range ["0", "upper_bound") */
static inline std::string RandomDatasetName(size_t upper_bound) {
  size_t dset_index = Random1ToUpperBound(upper_bound) - 1;
  std::string result = std::to_string(dset_index);

  return result;
}

TEST_CASE("H5FOpen", "[process=" + std::to_string(TEST_INFO->comm_size_) +
                      "]"
                      "[operation=single_open]"
                      "[repetition=1][file=1]") {
  TEST_INFO->Pretest();
  SECTION("open non-existent file") {
    MuteHdf5Errors mute;
    TEST_INFO->TestOpen(TEST_INFO->new_file_, H5F_ACC_RDONLY);
    REQUIRE(TEST_INFO->hermes_hid_ == H5I_INVALID_HID);
    TEST_INFO->TestOpen(TEST_INFO->new_file_, H5F_ACC_RDWR);
    REQUIRE(TEST_INFO->hermes_hid_ == H5I_INVALID_HID);
  }

  SECTION("truncate existing file") {
    TEST_INFO->TestOpen(TEST_INFO->existing_file_, H5F_ACC_TRUNC, true);
    REQUIRE(TEST_INFO->hermes_hid_ != H5I_INVALID_HID);
    TEST_INFO->TestClose();
    REQUIRE(TEST_INFO->hermes_herr_ >= 0);
  }

  SECTION("open existing file") {
    TEST_INFO->TestOpen(TEST_INFO->existing_file_, H5F_ACC_RDWR);
    REQUIRE(TEST_INFO->hermes_hid_ != H5I_INVALID_HID);
    TEST_INFO->TestClose();
    REQUIRE(TEST_INFO->hermes_herr_ >= 0);

    TEST_INFO->TestOpen(TEST_INFO->existing_file_, H5F_ACC_RDONLY);
    REQUIRE(TEST_INFO->hermes_hid_ != H5I_INVALID_HID);
    TEST_INFO->TestClose();
    REQUIRE(TEST_INFO->hermes_herr_ >= 0);
  }

  SECTION("create existing file exclusively") {
    MuteHdf5Errors mute;
    TEST_INFO->TestOpen(TEST_INFO->existing_file_, H5F_ACC_EXCL, true);
    REQUIRE(TEST_INFO->hermes_hid_ == H5I_INVALID_HID);
  }

  TEST_INFO->Posttest();
}

TEST_CASE("SingleWrite", "[process=" + std::to_string(TEST_INFO->comm_size_) +
                             "]"
                             "[operation=single_write]"
                             "[request_size=type-fixed][repetition=1]"
                             "[file=1]") {
  TEST_INFO->Pretest();

  SECTION("overwrite dataset in existing file") {
    TEST_INFO->TestOpen(TEST_INFO->existing_file_, H5F_ACC_RDWR);
    REQUIRE(TEST_INFO->hermes_hid_ != H5I_INVALID_HID);
    TEST_INFO->TestWritePartial1d("0", TEST_INFO->write_data_.data(), 0,
                                  TEST_INFO->request_size_);
    TEST_INFO->TestClose();
    REQUIRE(TEST_INFO->hermes_herr_ >= 0);
  }

  SECTION("write to new file") {
    TEST_INFO->TestOpen(TEST_INFO->new_file_, H5F_ACC_EXCL, true);
    REQUIRE(TEST_INFO->hermes_hid_ != H5I_INVALID_HID);
    TEST_INFO->TestWriteDataset("0", TEST_INFO->write_data_);
    TEST_INFO->TestClose();
    REQUIRE(TEST_INFO->hermes_herr_ >= 0);
  }

  SECTION("write to existing file with truncate") {
    TEST_INFO->TestOpen(TEST_INFO->existing_file_, H5F_ACC_TRUNC, true);
    REQUIRE(TEST_INFO->hermes_hid_ != H5I_INVALID_HID);
    TEST_INFO->TestWriteDataset("0", TEST_INFO->write_data_);
    TEST_INFO->TestClose();
    REQUIRE(TEST_INFO->hermes_herr_ >= 0);
  }

  SECTION("add dataset to existing file") {
     TEST_INFO->TestOpen(TEST_INFO->existing_file_, H5F_ACC_RDWR);
    REQUIRE(TEST_INFO->hermes_hid_ != H5I_INVALID_HID);
    TEST_INFO->TestWriteDataset(std::to_string(TEST_INFO->num_iterations_), TEST_INFO->write_data_);
    TEST_INFO->TestClose();
    REQUIRE(TEST_INFO->hermes_herr_ >= 0);
  }

  TEST_INFO->Posttest();
}

TEST_CASE("SingleRead", "[process=" + std::to_string(TEST_INFO->comm_size_) +
                            "]"
                            "[operation=single_read]"
                            "[request_size=type-fixed][repetition=1]"
                            "[file=1]") {
  TEST_INFO->Pretest();
  SECTION("read from non-existing file") {
    MuteHdf5Errors mute;
    TEST_INFO->TestOpen(TEST_INFO->new_file_, H5F_ACC_RDONLY);
    REQUIRE(TEST_INFO->hermes_hid_ == H5I_INVALID_HID);
  }

  SECTION("read first dataset from existing file") {
    TEST_INFO->TestOpen(TEST_INFO->existing_file_, H5F_ACC_RDONLY);
    REQUIRE(TEST_INFO->hermes_hid_ != H5I_INVALID_HID);
    TEST_INFO->TestRead("0", TEST_INFO->read_data_, 0, TEST_INFO->request_size_);
    TEST_INFO->TestClose();
    REQUIRE(TEST_INFO->hermes_herr_ >= 0);
  }

  SECTION("read last dataset of existing file") {
    TEST_INFO->TestOpen(TEST_INFO->existing_file_, H5F_ACC_RDONLY);
    REQUIRE(TEST_INFO->hermes_hid_ != H5I_INVALID_HID);
    TEST_INFO->TestRead(std::to_string(TEST_INFO->num_iterations_ - 1), TEST_INFO->read_data_, 0,
                   TEST_INFO->request_size_);
    TEST_INFO->TestClose();
    REQUIRE(TEST_INFO->hermes_herr_ >= 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedWriteSequential",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=sequential][file=1]") {
  TEST_INFO->Pretest();
  SECTION("write to new file") {
    TEST_INFO->TestOpen(TEST_INFO->new_file_, H5F_ACC_EXCL, true);
    REQUIRE(TEST_INFO->hermes_hid_ != H5I_INVALID_HID);

    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      TEST_INFO->TestWriteDataset(std::to_string(i), TEST_INFO->write_data_);
    }
    TEST_INFO->TestClose();
    REQUIRE(TEST_INFO->hermes_herr_ >= 0);
  }

  SECTION("overwrite first dataset") {
    TEST_INFO->TestOpen(TEST_INFO->new_file_, H5F_ACC_EXCL, true);
    REQUIRE(TEST_INFO->hermes_hid_ != H5I_INVALID_HID);

    TEST_INFO->TestWriteDataset("0", TEST_INFO->write_data_);
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      TEST_INFO->TestWritePartial1d("0", TEST_INFO->write_data_.data(), 0,
                               TEST_INFO->request_size_);
    }
    TEST_INFO->TestClose();
    REQUIRE(TEST_INFO->hermes_herr_ >= 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedReadSequential",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=sequential][file=1]") {
  TEST_INFO->Pretest();
  SECTION("read from existing file") {
    TEST_INFO->TestOpen(TEST_INFO->existing_file_, H5F_ACC_RDWR);
    REQUIRE(TEST_INFO->hermes_hid_ != H5I_INVALID_HID);

    std::vector<f32> buf(TEST_INFO->request_size_, 0.0f);
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      TEST_INFO->TestRead(std::to_string(i), buf, 0, TEST_INFO->request_size_);
    }
    TEST_INFO->TestClose();
    REQUIRE(TEST_INFO->hermes_herr_ >= 0);
  }

  SECTION("read from existing file always at start") {
    TEST_INFO->TestOpen(TEST_INFO->existing_file_, H5F_ACC_RDWR);
    REQUIRE(TEST_INFO->hermes_hid_ != H5I_INVALID_HID);

    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      TEST_INFO->TestRead("0", TEST_INFO->read_data_, 0, TEST_INFO->request_size_);
    }
    TEST_INFO->TestClose();
    REQUIRE(TEST_INFO->hermes_herr_ >= 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedReadRandom", "[process=" + std::to_string(TEST_INFO->comm_size_) +
                                   "]"
                                   "[operation=batched_read]"
                                   "[request_size=type-fixed]"
                                   "[repetition=" +
                                   std::to_string(TEST_INFO->num_iterations_) +
                                   "][pattern=random][file=1]") {
  TEST_INFO->Pretest();
  SECTION("read from existing file") {
    TEST_INFO->TestOpen(TEST_INFO->existing_file_, H5F_ACC_RDWR);
    REQUIRE(TEST_INFO->hermes_hid_ != H5I_INVALID_HID);

    std::vector<f32> buf(TEST_INFO->request_size_, 0.0f);
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      u32 dataset = TEST_INFO->GenNextRandom() % TEST_INFO->num_iterations_;
      TEST_INFO->TestRead(std::to_string(dataset), buf, 0, TEST_INFO->request_size_);
    }
    TEST_INFO->TestClose();
    REQUIRE(TEST_INFO->hermes_herr_ >= 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedUpdateRandom", "[process=" + std::to_string(TEST_INFO->comm_size_) +
                                     "]"
                                     "[operation=batched_write]"
                                     "[request_size=type-fixed][repetition=" +
                                     std::to_string(TEST_INFO->num_iterations_) +
                                     "]"
                                     "[pattern=random][file=1]") {
  TEST_INFO->Pretest();
  SECTION("update entire dataset in existing file") {
    TEST_INFO->TestOpen(TEST_INFO->existing_file_, H5F_ACC_RDWR);
    REQUIRE(TEST_INFO->hermes_hid_ != H5I_INVALID_HID);

    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      u32 dataset = TEST_INFO->GenNextRandom() % TEST_INFO->num_iterations_;
      TEST_INFO->TestWritePartial1d(std::to_string(dataset), TEST_INFO->write_data_.data(),
                               0, TEST_INFO->request_size_);
    }
    TEST_INFO->TestClose();
    REQUIRE(TEST_INFO->hermes_herr_ >= 0);
  }

  SECTION("update partial dataset in existing file") {
    TEST_INFO->TestOpen(TEST_INFO->existing_file_, H5F_ACC_RDWR);
    REQUIRE(TEST_INFO->hermes_hid_ != H5I_INVALID_HID);

    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      u32 dataset = TEST_INFO->GenNextRandom() % TEST_INFO->num_iterations_;
      // NOTE(chogan): Subtract 1 from size so we're always writing at least 1
      // element
      hsize_t offset = TEST_INFO->GenNextRandom() % (TEST_INFO->write_data_.size() - 1);
      hsize_t elements_to_write = TEST_INFO->write_data_.size() - offset;
      TEST_INFO->TestWritePartial1d(std::to_string(dataset), TEST_INFO->write_data_.data(),
                               offset, elements_to_write);
    }

    TEST_INFO->TestClose();
    REQUIRE(TEST_INFO->hermes_herr_ >= 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("BatchedWriteRSVariable",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=type-variable][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=sequential][file=1]") {
  TEST_INFO->Pretest();

  SECTION("write to new file always at the start") {
    MuteHdf5Errors mute;
    TEST_INFO->TestOpen(TEST_INFO->new_file_, H5F_ACC_EXCL, true);
    REQUIRE(TEST_INFO->hermes_hid_ != H5I_INVALID_HID);

    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      size_t request_size = Random1ToUpperBound(TEST_INFO->request_size_);
      std::vector<f32> data(request_size, 2.0f);
      TEST_INFO->TestWritePartial1d(std::to_string(i), data.data(), 0, request_size);
    }
    TEST_INFO->TestClose();
    REQUIRE(TEST_INFO->hermes_herr_ >= 0);
  }

  TEST_INFO->Posttest();
}

TEST_CASE("BatchedReadSequentialRSVariable",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=type-variable][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=sequential][file=1]") {
  TEST_INFO->Pretest();

  SECTION("read from existing file") {
    TEST_INFO->TestOpen(TEST_INFO->existing_file_, H5F_ACC_RDONLY);
    REQUIRE(TEST_INFO->hermes_hid_ != H5I_INVALID_HID);

    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      size_t request_size = Random1ToUpperBound(TEST_INFO->request_size_);
      size_t starting_element =  TEST_INFO->request_size_ - request_size;
      std::vector<f32> data(request_size, 1.5f);
      TEST_INFO->TestRead(std::to_string(i), data, starting_element, request_size);
    }
    TEST_INFO->TestClose();
    REQUIRE(TEST_INFO->hermes_herr_ >= 0);
  }

  SECTION("read from existing file always at start") {
    TEST_INFO->TestOpen(TEST_INFO->existing_file_, H5F_ACC_RDONLY);
    REQUIRE(TEST_INFO->hermes_hid_ != H5I_INVALID_HID);

    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      size_t request_size = Random1ToUpperBound(TEST_INFO->request_size_);
      std::vector<f32> data(request_size, 3.0f);
      TEST_INFO->TestRead(std::to_string(i), data, 0, request_size);
    }
    TEST_INFO->TestClose();
    REQUIRE(TEST_INFO->hermes_herr_ >= 0);
  }

  TEST_INFO->Posttest();
}

TEST_CASE("BatchedReadRandomRSVariable",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=type-variable]"
              "[repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "][pattern=random][file=1]") {
  TEST_INFO->Pretest();

  SECTION("read from existing file") {
    TEST_INFO->TestOpen(TEST_INFO->existing_file_, H5F_ACC_RDWR);
    REQUIRE(TEST_INFO->hermes_hid_ != H5I_INVALID_HID);

    std::vector<f32> data(TEST_INFO->request_size_, 5.0f);
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      std::string dset_name = RandomDatasetName(TEST_INFO->num_iterations_);
      size_t starting_element = Random1ToUpperBound(TEST_INFO->request_size_);
      size_t request_elements =
        Random1ToUpperBound(TEST_INFO->request_size_ - starting_element);
      std::vector<f32> data(request_elements, 3.8f);
      TEST_INFO->TestRead(dset_name, data, starting_element, request_elements);
    }
    TEST_INFO->TestClose();
    REQUIRE(TEST_INFO->hermes_herr_ >= 0);
  }

  TEST_INFO->Posttest();
}

TEST_CASE("BatchedUpdateRandomRSVariable",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=type-variable][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=random][file=1]") {
  TEST_INFO->Pretest();

  SECTION("write to existing file") {
    TEST_INFO->TestOpen(TEST_INFO->existing_file_, H5F_ACC_RDWR);
    REQUIRE(TEST_INFO->hermes_hid_ != H5I_INVALID_HID);

    std::vector<f32> data(TEST_INFO->request_size_, 8.0f);
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      std::string dset_name = RandomDatasetName(TEST_INFO->num_iterations_);
      size_t request_size = Random1ToUpperBound(TEST_INFO->request_size_);
      TEST_INFO->TestWritePartial1d(dset_name, data.data(), 0, request_size);
    }
    TEST_INFO->TestClose();
    REQUIRE(TEST_INFO->hermes_herr_ >= 0);
  }

  TEST_INFO->Posttest();
}

TEST_CASE("BatchedWriteTemporalFixed",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=sequential][file=1][temporal=fixed]") {
  TEST_INFO->Pretest();

  SECTION("write to existing file") {
    TEST_INFO->TestOpen(TEST_INFO->existing_file_, H5F_ACC_RDWR);
    REQUIRE(TEST_INFO->hermes_hid_ != H5I_INVALID_HID);

    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      usleep(TEST_INFO->temporal_interval_ms_ * 1000);
      TEST_INFO->TestWritePartial1d(std::to_string(i), TEST_INFO->write_data_.data(), 0,
                               TEST_INFO->request_size_);
    }
    TEST_INFO->TestClose();
    REQUIRE(TEST_INFO->hermes_herr_ >= 0);
  }

  SECTION("write to new file always at start") {
    TEST_INFO->TestOpen(TEST_INFO->new_file_, H5F_ACC_EXCL, true);
    REQUIRE(TEST_INFO->hermes_hid_ != H5I_INVALID_HID);

    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      usleep(TEST_INFO->temporal_interval_ms_ * 1000);
      TEST_INFO->TestWriteDataset(std::to_string(i), TEST_INFO->write_data_);
    }
    TEST_INFO->TestClose();
  }

  TEST_INFO->Posttest();
}

TEST_CASE("BatchedWriteTemporalVariable",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=sequential][file=1][temporal=variable]") {
  TEST_INFO->Pretest();

  SECTION("write to existing file") {
    TEST_INFO->TestOpen(TEST_INFO->existing_file_, H5F_ACC_RDWR);
    REQUIRE(TEST_INFO->hermes_hid_ != H5I_INVALID_HID);

    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      size_t sleep_interval_ms =
        TEST_INFO->GenNextRandom() % (TEST_INFO->temporal_interval_ms_ + 2);
      usleep(sleep_interval_ms * 1000);
      TEST_INFO->TestWritePartial1d(std::to_string(i), TEST_INFO->write_data_.data(), 0,
                               TEST_INFO->request_size_);
    }
    TEST_INFO->TestClose();
    REQUIRE(TEST_INFO->hermes_herr_ >= 0);
  }

  SECTION("write to new file always at start") {
    TEST_INFO->TestOpen(TEST_INFO->new_file_, H5F_ACC_EXCL, true);
    REQUIRE(TEST_INFO->hermes_hid_ != H5I_INVALID_HID);

    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      size_t sleep_interval_ms =
        TEST_INFO->GenNextRandom() % (TEST_INFO->temporal_interval_ms_ + 2);
      usleep(sleep_interval_ms * 1000);
      TEST_INFO->TestWriteDataset(std::to_string(i), TEST_INFO->write_data_);
    }
    TEST_INFO->TestClose();
    REQUIRE(TEST_INFO->hermes_herr_ >= 0);
  }

  TEST_INFO->Posttest();
}

TEST_CASE("BatchedMixedSequential",
          "[process=" + std::to_string(TEST_INFO->comm_size_) +
              "]"
              "[operation=batched_mixed]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(TEST_INFO->num_iterations_) +
              "]"
              "[pattern=sequential][file=1]") {
  TEST_INFO->Pretest();

  SECTION("read after write on new file") {
    TEST_INFO->TestOpen(TEST_INFO->new_file_, H5F_ACC_EXCL, true);
    REQUIRE(TEST_INFO->hermes_hid_ != H5I_INVALID_HID);

    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      std::string dset_name = std::to_string(i);
      TEST_INFO->TestWriteDataset(dset_name, TEST_INFO->write_data_);
      TEST_INFO->TestRead(dset_name, TEST_INFO->read_data_, 0, TEST_INFO->request_size_);
    }
    TEST_INFO->TestClose();
    REQUIRE(TEST_INFO->hermes_herr_ >= 0);
  }

  SECTION("alternate write and read existing file") {
    TEST_INFO->TestOpen(TEST_INFO->existing_file_, H5F_ACC_RDWR);
    REQUIRE(TEST_INFO->hermes_hid_ != H5I_INVALID_HID);

    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      std::string dset_name = std::to_string(i);

      if (i % 2 == 0) {
        TEST_INFO->TestWritePartial1d(dset_name, TEST_INFO->write_data_.data(), 0,
                                 TEST_INFO->request_size_);
      } else {
        TEST_INFO->TestRead(dset_name, TEST_INFO->read_data_, 0, TEST_INFO->request_size_);
      }
    }
    TEST_INFO->TestClose();
    REQUIRE(TEST_INFO->hermes_herr_ >= 0);
  }

  SECTION("update after read existing file") {
    TEST_INFO->TestOpen(TEST_INFO->existing_file_, H5F_ACC_RDWR);
    REQUIRE(TEST_INFO->hermes_hid_ != H5I_INVALID_HID);

    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      std::string dset_name = std::to_string(i);
      TEST_INFO->TestRead(dset_name, TEST_INFO->read_data_, 0, TEST_INFO->request_size_);
      TEST_INFO->TestWritePartial1d(dset_name, TEST_INFO->write_data_.data(), 0,
                               TEST_INFO->request_size_);
    }

    TEST_INFO->TestClose();
    REQUIRE(TEST_INFO->hermes_herr_ >= 0);
  }

  SECTION("read all after write all on new file in single open") {
    TEST_INFO->TestOpen(TEST_INFO->new_file_, H5F_ACC_EXCL, true);
    REQUIRE(TEST_INFO->hermes_hid_ != H5I_INVALID_HID);

    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      std::string dset_name = std::to_string(i);
      TEST_INFO->TestWriteDataset(dset_name, TEST_INFO->write_data_);
    }

    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      std::string dset_name = std::to_string(i);
      TEST_INFO->TestRead(dset_name, TEST_INFO->read_data_, 0, TEST_INFO->request_size_);
    }

    TEST_INFO->TestClose();
    REQUIRE(TEST_INFO->hermes_herr_ >= 0);
  }

  SECTION("read all after write all on new file in different open") {
    TEST_INFO->TestOpen(TEST_INFO->new_file_, H5F_ACC_EXCL, true);
    REQUIRE(TEST_INFO->hermes_hid_ != H5I_INVALID_HID);

    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      TEST_INFO->TestWriteDataset(std::to_string(i), TEST_INFO->write_data_);
    }

    TEST_INFO->TestClose();
    REQUIRE(TEST_INFO->hermes_herr_ >= 0);

    TEST_INFO->TestOpen(TEST_INFO->new_file_, H5F_ACC_RDWR);
    REQUIRE(TEST_INFO->hermes_hid_ != H5I_INVALID_HID);

    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      TEST_INFO->TestRead(std::to_string(i), TEST_INFO->read_data_, 0,
                     TEST_INFO->request_size_);
    }

    TEST_INFO->TestClose();
    REQUIRE(TEST_INFO->hermes_herr_ >= 0);
  }
  TEST_INFO->Posttest();
}

TEST_CASE("SingleMixed", "[process=" + std::to_string(TEST_INFO->comm_size_) +
                             "][operation=single_mixed]"
                             "[request_size=type-fixed][repetition=1]"
                             "[file=1]") {
  TEST_INFO->Pretest();

  SECTION("read after write from new file") {
    TEST_INFO->TestOpen(TEST_INFO->new_file_, H5F_ACC_EXCL, true);
    REQUIRE(TEST_INFO->hermes_hid_ != H5I_INVALID_HID);
    std::string dset_name("0");
    TEST_INFO->TestWriteDataset(dset_name, TEST_INFO->write_data_);
    TEST_INFO->TestRead(dset_name, TEST_INFO->read_data_, 0, TEST_INFO->request_size_);
    TEST_INFO->TestClose();
    REQUIRE(TEST_INFO->hermes_herr_ >= 0);
  }

  SECTION("update after read from existing file") {
    TEST_INFO->TestOpen(TEST_INFO->existing_file_, H5F_ACC_RDWR);
    REQUIRE(TEST_INFO->hermes_hid_ != H5I_INVALID_HID);
    std::string dset_name("0");
    TEST_INFO->TestRead(dset_name, TEST_INFO->read_data_, 0, TEST_INFO->request_size_);
    TEST_INFO->TestWritePartial1d(dset_name, TEST_INFO->write_data_.data(), 0,
                             TEST_INFO->request_size_);
    TEST_INFO->TestClose();
    REQUIRE(TEST_INFO->hermes_herr_ >= 0);
  }

  SECTION("read after write from new file different opens") {
    TEST_INFO->TestOpen(TEST_INFO->new_file_, H5F_ACC_EXCL, true);
    REQUIRE(TEST_INFO->hermes_hid_ != H5I_INVALID_HID);
    std::string dset_name("0");
    TEST_INFO->TestWriteDataset(dset_name, TEST_INFO->write_data_);
    TEST_INFO->TestClose();
    REQUIRE(TEST_INFO->hermes_herr_ >= 0);

    TEST_INFO->TestOpen(TEST_INFO->new_file_, H5F_ACC_RDWR);
    TEST_INFO->TestRead(dset_name, TEST_INFO->read_data_, 0, TEST_INFO->request_size_);
    TEST_INFO->TestClose();
    REQUIRE(TEST_INFO->hermes_herr_ >= 0);
  }

  TEST_INFO->Posttest();
}

TEST_CASE("CompactDatasets") {
  TEST_INFO->Pretest();

  SECTION("create many and read randomly") {
    TEST_INFO->TestOpen(TEST_INFO->new_file_, H5F_ACC_EXCL, true);
    REQUIRE(TEST_INFO->hermes_hid_ != H5I_INVALID_HID);

    size_t num_elements = KILOBYTES(32) / sizeof(f32);

    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      std::vector<f32> data(num_elements);
      for (size_t i = 0; i < data.size(); ++i) {
        data[i] = TEST_INFO->GenRandom0to1();
      }
      TEST_INFO->TestMakeCompactDataset(std::to_string(i), data);
    }

    std::vector<f32> read_buf(num_elements, 0.0f);
    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      std::string dset_name = RandomDatasetName(TEST_INFO->num_iterations_);
      TEST_INFO->TestRead(dset_name, read_buf, 0, num_elements);
    }

    TEST_INFO->TestClose();
    REQUIRE(TEST_INFO->hermes_herr_ >= 0);
  }

  TEST_INFO->Posttest();
}

TEST_CASE("PartialUpdateToLastPage") {
  TEST_INFO->Pretest();

  SECTION("beginning of last page") {
    TEST_INFO->TestOpen(TEST_INFO->existing_file_, H5F_ACC_RDWR);
    REQUIRE(TEST_INFO->hermes_hid_ != H5I_INVALID_HID);
    TEST_INFO->TestWritePartial1d(std::to_string(TEST_INFO->num_iterations_ - 1),
                             TEST_INFO->write_data_.data(), 0,
                             TEST_INFO->request_size_ / 2);
    TEST_INFO->TestClose();
    REQUIRE(TEST_INFO->hermes_herr_ >= 0);
  }

  SECTION("in middle of last page") {
    TEST_INFO->TestOpen(TEST_INFO->existing_file_, H5F_ACC_RDWR);
    REQUIRE(TEST_INFO->hermes_hid_ != H5I_INVALID_HID);
    TEST_INFO->TestWritePartial1d(std::to_string(TEST_INFO->num_iterations_ - 1),
                             TEST_INFO->write_data_.data(),
                             TEST_INFO->request_size_ / 4,
                             TEST_INFO->request_size_ / 2);
    TEST_INFO->TestClose();
    REQUIRE(TEST_INFO->hermes_herr_ >= 0);
  }

  SECTION("at end of last page") {
    TEST_INFO->TestOpen(TEST_INFO->existing_file_, H5F_ACC_RDWR);
    REQUIRE(TEST_INFO->hermes_hid_ != H5I_INVALID_HID);
    TEST_INFO->TestWritePartial1d(std::to_string(TEST_INFO->num_iterations_ - 1),
                             TEST_INFO->write_data_.data(),
                             TEST_INFO->request_size_ / 2,
                             TEST_INFO->request_size_ / 2);
    TEST_INFO->TestClose();
    REQUIRE(TEST_INFO->hermes_herr_ >= 0);
  }

  TEST_INFO->Posttest();
}

TEST_CASE("ScratchMode", "[mode=scratch]") {
  TEST_INFO->Pretest();

  SECTION("created files shouldn't persist") {
    TEST_INFO->TestOpen(TEST_INFO->new_file_, H5F_ACC_EXCL, true);
    REQUIRE(TEST_INFO->hermes_hid_ != H5I_INVALID_HID);

    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      TEST_INFO->TestWriteDataset(std::to_string(i), TEST_INFO->write_data_);
    }

    for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
      std::string dset_name = RandomDatasetName(TEST_INFO->num_iterations_);
      TEST_INFO->TestRead(dset_name, TEST_INFO->read_data_, 0, TEST_INFO->request_size_);
    }

    TEST_INFO->TestClose();
    REQUIRE(TEST_INFO->hermes_herr_ >= 0);

    if (HERMES_CLIENT_CONF.GetBaseAdapterMode()
        == hermes::adapter::AdapterMode::kScratch) {
      REQUIRE(!stdfs::exists(TEST_INFO->new_file_.hermes_));
    }
  }

  TEST_INFO->Posttest();
}
