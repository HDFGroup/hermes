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
using hermes::adapter::test::MuteHdf5Errors;

/** Returns a number in the range [1, upper_bound] */
static inline size_t Random1ToUpperBound(size_t upper_bound) {
  size_t result = ((size_t)TESTER->GenNextRandom() % upper_bound) + 1;

  return result;
}

/** Returns a string in the range ["0", "upper_bound") */
static inline std::string RandomDatasetName(size_t upper_bound) {
  size_t dset_index = Random1ToUpperBound(upper_bound) - 1;
  std::string result = std::to_string(dset_index);

  return result;
}

TEST_CASE("H5FOpen", "[process=" + std::to_string(TESTER->comm_size_) +
    "]"
    "[operation=single_open]"
    "[repetition=1][file=1]") {
  TESTER->Pretest();
  SECTION("open non-existent file") {
    MuteHdf5Errors mute;
    TESTER->TestOpen(TESTER->new_file_, H5F_ACC_RDONLY);
    REQUIRE(TESTER->hermes_hid_ == H5I_INVALID_HID);
    TESTER->TestOpen(TESTER->new_file_, H5F_ACC_RDWR);
    REQUIRE(TESTER->hermes_hid_ == H5I_INVALID_HID);
  }

  SECTION("truncate existing file") {
    TESTER->TestOpen(TESTER->existing_file_, H5F_ACC_TRUNC, true);
    REQUIRE(TESTER->hermes_hid_ != H5I_INVALID_HID);
    TESTER->TestClose();
    REQUIRE(TESTER->hermes_herr_ >= 0);
  }

  SECTION("open existing file") {
    TESTER->TestOpen(TESTER->existing_file_, H5F_ACC_RDWR);
    REQUIRE(TESTER->hermes_hid_ != H5I_INVALID_HID);
    TESTER->TestClose();
    REQUIRE(TESTER->hermes_herr_ >= 0);

    TESTER->TestOpen(TESTER->existing_file_, H5F_ACC_RDONLY);
    REQUIRE(TESTER->hermes_hid_ != H5I_INVALID_HID);
    TESTER->TestClose();
    REQUIRE(TESTER->hermes_herr_ >= 0);
  }

  SECTION("create existing file exclusively") {
    MuteHdf5Errors mute;
    TESTER->TestOpen(TESTER->existing_file_, H5F_ACC_EXCL, true);
    REQUIRE(TESTER->hermes_hid_ == H5I_INVALID_HID);
  }

  TESTER->Posttest();
}

TEST_CASE("SingleWrite", "[process=" + std::to_string(TESTER->comm_size_) +
    "]"
    "[operation=single_write]"
    "[request_size=type-fixed][repetition=1]"
    "[file=1]") {
  TESTER->Pretest();

  SECTION("overwrite dataset in existing file") {
    TESTER->TestOpen(TESTER->existing_file_, H5F_ACC_RDWR);
    REQUIRE(TESTER->hermes_hid_ != H5I_INVALID_HID);
    TESTER->TestWritePartial1d("0", TESTER->write_data_.data(), 0,
                               TESTER->request_size_);
    TESTER->TestClose();
    REQUIRE(TESTER->hermes_herr_ >= 0);
  }

  SECTION("write to new file") {
    TESTER->TestOpen(TESTER->new_file_, H5F_ACC_EXCL, true);
    REQUIRE(TESTER->hermes_hid_ != H5I_INVALID_HID);
    TESTER->TestWriteDataset("0", TESTER->write_data_);
    TESTER->TestClose();
    REQUIRE(TESTER->hermes_herr_ >= 0);
  }

  SECTION("write to existing file with truncate") {
    TESTER->TestOpen(TESTER->existing_file_, H5F_ACC_TRUNC, true);
    REQUIRE(TESTER->hermes_hid_ != H5I_INVALID_HID);
    TESTER->TestWriteDataset("0", TESTER->write_data_);
    TESTER->TestClose();
    REQUIRE(TESTER->hermes_herr_ >= 0);
  }

  SECTION("add dataset to existing file") {
    TESTER->TestOpen(TESTER->existing_file_, H5F_ACC_RDWR);
    REQUIRE(TESTER->hermes_hid_ != H5I_INVALID_HID);
    TESTER->TestWriteDataset(std::to_string(TESTER->num_iterations_),
                             TESTER->write_data_);
    TESTER->TestClose();
    REQUIRE(TESTER->hermes_herr_ >= 0);
  }

  TESTER->Posttest();
}

TEST_CASE("SingleRead", "[process=" + std::to_string(TESTER->comm_size_) +
    "]"
    "[operation=single_read]"
    "[request_size=type-fixed][repetition=1]"
    "[file=1]") {
  TESTER->Pretest();
  SECTION("read from non-existing file") {
    MuteHdf5Errors mute;
    TESTER->TestOpen(TESTER->new_file_, H5F_ACC_RDONLY);
    REQUIRE(TESTER->hermes_hid_ == H5I_INVALID_HID);
  }

  SECTION("read first dataset from existing file") {
    TESTER->TestOpen(TESTER->existing_file_, H5F_ACC_RDONLY);
    REQUIRE(TESTER->hermes_hid_ != H5I_INVALID_HID);
    TESTER->TestRead("0", TESTER->read_data_, 0,
                     TESTER->request_size_);
    TESTER->TestClose();
    REQUIRE(TESTER->hermes_herr_ >= 0);
  }

  SECTION("read last dataset of existing file") {
    TESTER->TestOpen(TESTER->existing_file_, H5F_ACC_RDONLY);
    REQUIRE(TESTER->hermes_hid_ != H5I_INVALID_HID);
    TESTER->TestRead(std::to_string(TESTER->num_iterations_ - 1),
                     TESTER->read_data_, 0,
                     TESTER->request_size_);
    TESTER->TestClose();
    REQUIRE(TESTER->hermes_herr_ >= 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedWriteSequential",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=sequential][file=1]") {
  TESTER->Pretest();
  SECTION("write to new file") {
    TESTER->TestOpen(TESTER->new_file_, H5F_ACC_EXCL, true);
    REQUIRE(TESTER->hermes_hid_ != H5I_INVALID_HID);

    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      TESTER->TestWriteDataset(std::to_string(i), TESTER->write_data_);
    }
    TESTER->TestClose();
    REQUIRE(TESTER->hermes_herr_ >= 0);
  }

  SECTION("overwrite first dataset") {
    TESTER->TestOpen(TESTER->new_file_, H5F_ACC_EXCL, true);
    REQUIRE(TESTER->hermes_hid_ != H5I_INVALID_HID);

    TESTER->TestWriteDataset("0", TESTER->write_data_);
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      TESTER->TestWritePartial1d("0", TESTER->write_data_.data(), 0,
                                 TESTER->request_size_);
    }
    TESTER->TestClose();
    REQUIRE(TESTER->hermes_herr_ >= 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedReadSequential",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=sequential][file=1]") {
  TESTER->Pretest();
  SECTION("read from existing file") {
    TESTER->TestOpen(TESTER->existing_file_, H5F_ACC_RDWR);
    REQUIRE(TESTER->hermes_hid_ != H5I_INVALID_HID);

    std::vector<f32> buf(TESTER->request_size_, 0.0f);
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      TESTER->TestRead(std::to_string(i), buf, 0,
                       TESTER->request_size_);
    }
    TESTER->TestClose();
    REQUIRE(TESTER->hermes_herr_ >= 0);
  }

  SECTION("read from existing file always at start") {
    TESTER->TestOpen(TESTER->existing_file_, H5F_ACC_RDWR);
    REQUIRE(TESTER->hermes_hid_ != H5I_INVALID_HID);

    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      TESTER->TestRead("0", TESTER->read_data_, 0,
                       TESTER->request_size_);
    }
    TESTER->TestClose();
    REQUIRE(TESTER->hermes_herr_ >= 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedReadRandom", "[process=" +
    std::to_string(TESTER->comm_size_) +
    "]"
    "[operation=batched_read]"
    "[request_size=type-fixed]"
    "[repetition=" +
    std::to_string(TESTER->num_iterations_) +
    "][pattern=random][file=1]") {
  TESTER->Pretest();
  SECTION("read from existing file") {
    TESTER->TestOpen(TESTER->existing_file_, H5F_ACC_RDWR);
    REQUIRE(TESTER->hermes_hid_ != H5I_INVALID_HID);

    std::vector<f32> buf(TESTER->request_size_, 0.0f);
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      u32 dataset = TESTER->GenNextRandom() % TESTER->num_iterations_;
      TESTER->TestRead(std::to_string(dataset), buf, 0, TESTER->request_size_);
    }
    TESTER->TestClose();
    REQUIRE(TESTER->hermes_herr_ >= 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedUpdateRandom", "[process=" +
    std::to_string(TESTER->comm_size_) +
    "]"
    "[operation=batched_write]"
    "[request_size=type-fixed][repetition=" +
    std::to_string(TESTER->num_iterations_) +
    "]"
    "[pattern=random][file=1]") {
  TESTER->Pretest();
  SECTION("update entire dataset in existing file") {
    TESTER->TestOpen(TESTER->existing_file_, H5F_ACC_RDWR);
    REQUIRE(TESTER->hermes_hid_ != H5I_INVALID_HID);

    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      u32 dataset = TESTER->GenNextRandom() % TESTER->num_iterations_;
      TESTER->TestWritePartial1d(std::to_string(dataset),
                                 TESTER->write_data_.data(),
                                 0, TESTER->request_size_);
    }
    TESTER->TestClose();
    REQUIRE(TESTER->hermes_herr_ >= 0);
  }

  SECTION("update partial dataset in existing file") {
    TESTER->TestOpen(TESTER->existing_file_, H5F_ACC_RDWR);
    REQUIRE(TESTER->hermes_hid_ != H5I_INVALID_HID);

    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      u32 dataset = TESTER->GenNextRandom() % TESTER->num_iterations_;
      // NOTE(chogan): Subtract 1 from size so we're always writing at least 1
      // element
      hsize_t offset = TESTER->GenNextRandom() %
          (TESTER->write_data_.size() - 1);
      hsize_t elements_to_write = TESTER->write_data_.size() - offset;
      TESTER->TestWritePartial1d(std::to_string(dataset),
                                 TESTER->write_data_.data(),
                                 offset, elements_to_write);
    }

    TESTER->TestClose();
    REQUIRE(TESTER->hermes_herr_ >= 0);
  }
  TESTER->Posttest();
}

TEST_CASE("BatchedWriteRSVariable",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=type-variable][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=sequential][file=1]") {
  TESTER->Pretest();

  SECTION("write to new file always at the start") {
    MuteHdf5Errors mute;
    TESTER->TestOpen(TESTER->new_file_, H5F_ACC_EXCL, true);
    REQUIRE(TESTER->hermes_hid_ != H5I_INVALID_HID);

    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t request_size = Random1ToUpperBound(TESTER->request_size_);
      std::vector<f32> data(request_size, 2.0f);
      TESTER->TestWritePartial1d(std::to_string(i), data.data(),
                                 0, request_size);
    }
    TESTER->TestClose();
    REQUIRE(TESTER->hermes_herr_ >= 0);
  }

  TESTER->Posttest();
}

TEST_CASE("BatchedReadSequentialRSVariable",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=type-variable][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=sequential][file=1]") {
  TESTER->Pretest();

  SECTION("read from existing file") {
    TESTER->TestOpen(TESTER->existing_file_, H5F_ACC_RDONLY);
    REQUIRE(TESTER->hermes_hid_ != H5I_INVALID_HID);

    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t request_size = Random1ToUpperBound(TESTER->request_size_);
      size_t starting_element =  TESTER->request_size_ - request_size;
      std::vector<f32> data(request_size, 1.5f);
      TESTER->TestRead(std::to_string(i), data,
                       starting_element, request_size);
    }
    TESTER->TestClose();
    REQUIRE(TESTER->hermes_herr_ >= 0);
  }

  SECTION("read from existing file always at start") {
    TESTER->TestOpen(TESTER->existing_file_, H5F_ACC_RDONLY);
    REQUIRE(TESTER->hermes_hid_ != H5I_INVALID_HID);

    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t request_size = Random1ToUpperBound(TESTER->request_size_);
      std::vector<f32> data(request_size, 3.0f);
      TESTER->TestRead(std::to_string(i), data, 0, request_size);
    }
    TESTER->TestClose();
    REQUIRE(TESTER->hermes_herr_ >= 0);
  }

  TESTER->Posttest();
}

TEST_CASE("BatchedReadRandomRSVariable",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_read]"
              "[request_size=type-variable]"
              "[repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "][pattern=random][file=1]") {
  TESTER->Pretest();

  SECTION("read from existing file") {
    TESTER->TestOpen(TESTER->existing_file_, H5F_ACC_RDWR);
    REQUIRE(TESTER->hermes_hid_ != H5I_INVALID_HID);

    std::vector<f32> data(TESTER->request_size_, 5.0f);
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      std::string dset_name = RandomDatasetName(TESTER->num_iterations_);
      size_t starting_element = Random1ToUpperBound(TESTER->request_size_);
      size_t request_elements =
          Random1ToUpperBound(TESTER->request_size_ - starting_element);
      std::vector<f32> data(request_elements, 3.8f);
      TESTER->TestRead(dset_name, data, starting_element, request_elements);
    }
    TESTER->TestClose();
    REQUIRE(TESTER->hermes_herr_ >= 0);
  }

  TESTER->Posttest();
}

TEST_CASE("BatchedUpdateRandomRSVariable",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=type-variable][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=random][file=1]") {
  TESTER->Pretest();

  SECTION("write to existing file") {
    TESTER->TestOpen(TESTER->existing_file_, H5F_ACC_RDWR);
    REQUIRE(TESTER->hermes_hid_ != H5I_INVALID_HID);

    std::vector<f32> data(TESTER->request_size_, 8.0f);
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      std::string dset_name = RandomDatasetName(TESTER->num_iterations_);
      size_t request_size = Random1ToUpperBound(TESTER->request_size_);
      TESTER->TestWritePartial1d(dset_name, data.data(), 0, request_size);
    }
    TESTER->TestClose();
    REQUIRE(TESTER->hermes_herr_ >= 0);
  }

  TESTER->Posttest();
}

TEST_CASE("BatchedWriteTemporalFixed",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=sequential][file=1][temporal=fixed]") {
  TESTER->Pretest();

  SECTION("write to existing file") {
    TESTER->TestOpen(TESTER->existing_file_, H5F_ACC_RDWR);
    REQUIRE(TESTER->hermes_hid_ != H5I_INVALID_HID);

    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      usleep(TESTER->temporal_interval_ms_ * 1000);
      TESTER->TestWritePartial1d(std::to_string(i),
                                 TESTER->write_data_.data(), 0,
                                 TESTER->request_size_);
    }
    TESTER->TestClose();
    REQUIRE(TESTER->hermes_herr_ >= 0);
  }

  SECTION("write to new file always at start") {
    TESTER->TestOpen(TESTER->new_file_, H5F_ACC_EXCL, true);
    REQUIRE(TESTER->hermes_hid_ != H5I_INVALID_HID);

    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      usleep(TESTER->temporal_interval_ms_ * 1000);
      TESTER->TestWriteDataset(std::to_string(i), TESTER->write_data_);
    }
    TESTER->TestClose();
  }

  TESTER->Posttest();
}

TEST_CASE("BatchedWriteTemporalVariable",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_write]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=sequential][file=1][temporal=variable]") {
  TESTER->Pretest();

  SECTION("write to existing file") {
    TESTER->TestOpen(TESTER->existing_file_, H5F_ACC_RDWR);
    REQUIRE(TESTER->hermes_hid_ != H5I_INVALID_HID);

    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t sleep_interval_ms =
          TESTER->GenNextRandom() % (TESTER->temporal_interval_ms_ + 2);
      usleep(sleep_interval_ms * 1000);
      TESTER->TestWritePartial1d(std::to_string(i),
                                 TESTER->write_data_.data(), 0,
                                 TESTER->request_size_);
    }
    TESTER->TestClose();
    REQUIRE(TESTER->hermes_herr_ >= 0);
  }

  SECTION("write to new file always at start") {
    TESTER->TestOpen(TESTER->new_file_, H5F_ACC_EXCL, true);
    REQUIRE(TESTER->hermes_hid_ != H5I_INVALID_HID);

    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      size_t sleep_interval_ms =
          TESTER->GenNextRandom() % (TESTER->temporal_interval_ms_ + 2);
      usleep(sleep_interval_ms * 1000);
      TESTER->TestWriteDataset(std::to_string(i), TESTER->write_data_);
    }
    TESTER->TestClose();
    REQUIRE(TESTER->hermes_herr_ >= 0);
  }

  TESTER->Posttest();
}

TEST_CASE("BatchedMixedSequential",
          "[process=" + std::to_string(TESTER->comm_size_) +
              "]"
              "[operation=batched_mixed]"
              "[request_size=type-fixed][repetition=" +
              std::to_string(TESTER->num_iterations_) +
              "]"
              "[pattern=sequential][file=1]") {
  TESTER->Pretest();

  SECTION("read after write on new file") {
    TESTER->TestOpen(TESTER->new_file_, H5F_ACC_EXCL, true);
    REQUIRE(TESTER->hermes_hid_ != H5I_INVALID_HID);

    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      std::string dset_name = std::to_string(i);
      TESTER->TestWriteDataset(dset_name, TESTER->write_data_);
      TESTER->TestRead(dset_name, TESTER->read_data_,
                       0, TESTER->request_size_);
    }
    TESTER->TestClose();
    REQUIRE(TESTER->hermes_herr_ >= 0);
  }

  SECTION("alternate write and read existing file") {
    TESTER->TestOpen(TESTER->existing_file_, H5F_ACC_RDWR);
    REQUIRE(TESTER->hermes_hid_ != H5I_INVALID_HID);

    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      std::string dset_name = std::to_string(i);

      if (i % 2 == 0) {
        TESTER->TestWritePartial1d(dset_name, TESTER->write_data_.data(),
                                   0, TESTER->request_size_);
      } else {
        TESTER->TestRead(dset_name, TESTER->read_data_,
                         0, TESTER->request_size_);
      }
    }
    TESTER->TestClose();
    REQUIRE(TESTER->hermes_herr_ >= 0);
  }

  SECTION("update after read existing file") {
    TESTER->TestOpen(TESTER->existing_file_, H5F_ACC_RDWR);
    REQUIRE(TESTER->hermes_hid_ != H5I_INVALID_HID);

    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      std::string dset_name = std::to_string(i);
      TESTER->TestRead(dset_name, TESTER->read_data_,
                       0, TESTER->request_size_);
      TESTER->TestWritePartial1d(dset_name, TESTER->write_data_.data(), 0,
                                 TESTER->request_size_);
    }

    TESTER->TestClose();
    REQUIRE(TESTER->hermes_herr_ >= 0);
  }

  SECTION("read all after write all on new file in single open") {
    TESTER->TestOpen(TESTER->new_file_, H5F_ACC_EXCL, true);
    REQUIRE(TESTER->hermes_hid_ != H5I_INVALID_HID);

    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      std::string dset_name = std::to_string(i);
      TESTER->TestWriteDataset(dset_name, TESTER->write_data_);
    }

    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      std::string dset_name = std::to_string(i);
      TESTER->TestRead(dset_name, TESTER->read_data_,
                       0, TESTER->request_size_);
    }

    TESTER->TestClose();
    REQUIRE(TESTER->hermes_herr_ >= 0);
  }

  SECTION("read all after write all on new file in different open") {
    TESTER->TestOpen(TESTER->new_file_, H5F_ACC_EXCL, true);
    REQUIRE(TESTER->hermes_hid_ != H5I_INVALID_HID);

    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      TESTER->TestWriteDataset(std::to_string(i), TESTER->write_data_);
    }

    TESTER->TestClose();
    REQUIRE(TESTER->hermes_herr_ >= 0);

    TESTER->TestOpen(TESTER->new_file_, H5F_ACC_RDWR);
    REQUIRE(TESTER->hermes_hid_ != H5I_INVALID_HID);

    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      TESTER->TestRead(std::to_string(i), TESTER->read_data_, 0,
                       TESTER->request_size_);
    }

    TESTER->TestClose();
    REQUIRE(TESTER->hermes_herr_ >= 0);
  }
  TESTER->Posttest();
}

TEST_CASE("SingleMixed", "[process=" + std::to_string(TESTER->comm_size_) +
    "][operation=single_mixed]"
    "[request_size=type-fixed][repetition=1]"
    "[file=1]") {
  TESTER->Pretest();

  SECTION("read after write from new file") {
    TESTER->TestOpen(TESTER->new_file_, H5F_ACC_EXCL, true);
    REQUIRE(TESTER->hermes_hid_ != H5I_INVALID_HID);
    std::string dset_name("0");
    TESTER->TestWriteDataset(dset_name, TESTER->write_data_);
    TESTER->TestRead(dset_name, TESTER->read_data_,
                     0, TESTER->request_size_);
    TESTER->TestClose();
    REQUIRE(TESTER->hermes_herr_ >= 0);
  }

  SECTION("update after read from existing file") {
    TESTER->TestOpen(TESTER->existing_file_, H5F_ACC_RDWR);
    REQUIRE(TESTER->hermes_hid_ != H5I_INVALID_HID);
    std::string dset_name("0");
    TESTER->TestRead(dset_name, TESTER->read_data_,
                     0, TESTER->request_size_);
    TESTER->TestWritePartial1d(dset_name, TESTER->write_data_.data(), 0,
                               TESTER->request_size_);
    TESTER->TestClose();
    REQUIRE(TESTER->hermes_herr_ >= 0);
  }

  SECTION("read after write from new file different opens") {
    TESTER->TestOpen(TESTER->new_file_, H5F_ACC_EXCL, true);
    REQUIRE(TESTER->hermes_hid_ != H5I_INVALID_HID);
    std::string dset_name("0");
    TESTER->TestWriteDataset(dset_name, TESTER->write_data_);
    TESTER->TestClose();
    REQUIRE(TESTER->hermes_herr_ >= 0);

    TESTER->TestOpen(TESTER->new_file_, H5F_ACC_RDWR);
    TESTER->TestRead(dset_name, TESTER->read_data_,
                     0, TESTER->request_size_);
    TESTER->TestClose();
    REQUIRE(TESTER->hermes_herr_ >= 0);
  }

  TESTER->Posttest();
}

TEST_CASE("CompactDatasets") {
  TESTER->Pretest();

  SECTION("create many and read randomly") {
    TESTER->TestOpen(TESTER->new_file_, H5F_ACC_EXCL, true);
    REQUIRE(TESTER->hermes_hid_ != H5I_INVALID_HID);

    size_t num_elements = KILOBYTES(32) / sizeof(f32);

    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      std::vector<f32> data(num_elements);
      for (size_t i = 0; i < data.size(); ++i) {
        data[i] = TESTER->GenRandom0to1();
      }
      TESTER->TestMakeCompactDataset(std::to_string(i), data);
    }

    std::vector<f32> read_buf(num_elements, 0.0f);
    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      std::string dset_name = RandomDatasetName(TESTER->num_iterations_);
      TESTER->TestRead(dset_name, read_buf, 0, num_elements);
    }

    TESTER->TestClose();
    REQUIRE(TESTER->hermes_herr_ >= 0);
  }

  TESTER->Posttest();
}

TEST_CASE("PartialUpdateToLastPage") {
  TESTER->Pretest();

  SECTION("beginning of last page") {
    TESTER->TestOpen(TESTER->existing_file_, H5F_ACC_RDWR);
    REQUIRE(TESTER->hermes_hid_ != H5I_INVALID_HID);
    TESTER->TestWritePartial1d(
        std::to_string(TESTER->num_iterations_ - 1),
        TESTER->write_data_.data(), 0,
        TESTER->request_size_ / 2);
    TESTER->TestClose();
    REQUIRE(TESTER->hermes_herr_ >= 0);
  }

  SECTION("in middle of last page") {
    TESTER->TestOpen(TESTER->existing_file_, H5F_ACC_RDWR);
    REQUIRE(TESTER->hermes_hid_ != H5I_INVALID_HID);
    TESTER->TestWritePartial1d(
        std::to_string(TESTER->num_iterations_ - 1),
        TESTER->write_data_.data(),
        TESTER->request_size_ / 4,
        TESTER->request_size_ / 2);
    TESTER->TestClose();
    REQUIRE(TESTER->hermes_herr_ >= 0);
  }

  SECTION("at end of last page") {
    TESTER->TestOpen(TESTER->existing_file_, H5F_ACC_RDWR);
    REQUIRE(TESTER->hermes_hid_ != H5I_INVALID_HID);
    TESTER->TestWritePartial1d(
        std::to_string(TESTER->num_iterations_ - 1),
        TESTER->write_data_.data(),
        TESTER->request_size_ / 2,
        TESTER->request_size_ / 2);
    TESTER->TestClose();
    REQUIRE(TESTER->hermes_herr_ >= 0);
  }

  TESTER->Posttest();
}

TEST_CASE("ScratchMode", "[mode=scratch]") {
  TESTER->Pretest();

  SECTION("created files shouldn't persist") {
    TESTER->TestOpen(TESTER->new_file_, H5F_ACC_EXCL, true);
    REQUIRE(TESTER->hermes_hid_ != H5I_INVALID_HID);

    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      TESTER->TestWriteDataset(std::to_string(i), TESTER->write_data_);
    }

    for (size_t i = 0; i < TESTER->num_iterations_; ++i) {
      std::string dset_name = RandomDatasetName(TESTER->num_iterations_);
      TESTER->TestRead(dset_name, TESTER->read_data_,
                       0, TESTER->request_size_);
    }

    TESTER->TestClose();
    REQUIRE(TESTER->hermes_herr_ >= 0);

    if (HERMES_CLIENT_CONF.GetBaseAdapterMode()
        == hermes::adapter::AdapterMode::kScratch) {
      REQUIRE(!stdfs::exists(TESTER->new_file_.hermes_));
    }
  }

  TESTER->Posttest();
}
