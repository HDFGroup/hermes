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
    test::TestWritePartial1d("0", info.write_data.data(), 0, args.request_size);
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
    test::TestRead("0", info.read_data, 0, args.request_size);
    test::TestClose();
    REQUIRE(test::hermes_herr >= 0);
  }

  SECTION("read last dataset of existing file") {
    test::TestOpen(info.existing_file, H5F_ACC_RDONLY);
    REQUIRE(test::hermes_hid != H5I_INVALID_HID);
    test::TestRead(std::to_string(info.num_iterations - 1), info.read_data, 0,
                   args.request_size);
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
                               args.request_size);
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

    std::vector<f32> buf(args.request_size / sizeof(f32), 0.0f);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      test::TestRead(std::to_string(i), buf, 0, args.request_size);
    }
    test::TestClose();
    REQUIRE(test::hermes_herr >= 0);
  }

  SECTION("read from existing file always at start") {
    test::TestOpen(info.existing_file, H5F_ACC_RDWR);
    REQUIRE(test::hermes_hid != H5I_INVALID_HID);

    for (size_t i = 0; i < info.num_iterations; ++i) {
      test::TestRead("0", info.read_data, 0, args.request_size);
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

    std::vector<f32> buf(args.request_size / sizeof(f32), 0.0f);
    for (size_t i = 0; i < info.num_iterations; ++i) {
      u32 dataset = GenNextRandom() % info.num_iterations;
      test::TestRead(std::to_string(dataset), buf, 0, args.request_size);
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
                               0, args.request_size);
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
                               offset, elements_to_write * sizeof(f32));
    }

    test::TestClose();
    REQUIRE(test::hermes_herr >= 0);
  }
  Posttest();
}

// TEST_CASE("BatchedReadStrideFixed",
//           "[process=" + std::to_string(info.comm_size) +
//               "]"
//               "[operation=batched_read]"
//               "[request_size=type-fixed][repetition=" +
//               std::to_string(info.num_iterations) +
//               "]"
//               "[pattern=stride_fixed][file=1]") {
//   Pretest();

  // TODO(chogan):
  // SECTION("read from existing file") {
  //   test::TestOpen(info.existing_file, H5F_ACC_RDONLY);
  //   REQUIRE(test::hermes_hid != H5I_INVALID_HID);

  //   std::vector<f32> buf(args.request_size / sizeof(f32), 0.0f);
  //   for (size_t i = 0; i < info.num_iterations; ++i) {
  //     size_t offset = (i * info.stride_size) % info.total_size;
  //     test::TestRead(data.data(), args.request_size);
  //   }
  //   test::TestClose();
  //   REQUIRE(test::hermes_herr >= 0);
  // }
//   Posttest();
// }

// TEST_CASE("BatchedUpdateStrideFixed",
//           "[process=" + std::to_string(info.comm_size) +
//               "]"
//               "[operation=batched_write]"
//               "[request_size=type-fixed][repetition=" +
//               std::to_string(info.num_iterations) +
//               "]"
//               "[pattern=stride_fixed][file=1]") {
//   Pretest();

//   SECTION("read from existing file") {
//     test::TestOpen(info.existing_file, H5F_ACC_RDWR);
//     REQUIRE(test::hermes_hid != H5I_INVALID_HID);
//     std::string data(args.request_size, '1');
//     for (size_t i = 0; i < info.num_iterations; ++i) {
//       size_t offset = (i * info.stride_size) % info.total_size;
//       test::test_seek(offset, SEEK_SET);
//       REQUIRE(((size_t)test::status_orig) == offset);
//       test::TestWrite(data.data(), args.request_size);
//       REQUIRE(test::size_read_orig == args.request_size);
//     }
//     test::TestClose();
//     REQUIRE(test::hermes_herr >= 0);
//   }
//   Posttest();
// }

// TEST_CASE("BatchedReadStrideDynamic",
//           "[process=" + std::to_string(info.comm_size) +
//               "]"
//               "[operation=batched_read]"
//               "[request_size=type-fixed][repetition=" +
//               std::to_string(info.num_iterations) +
//               "]"
//               "[pattern=stride_dynamic][file=1]") {
//   Pretest();

//   SECTION("read from existing file") {
//     test::TestOpen(info.existing_file, H5F_ACC_RDWR);
//     REQUIRE(test::hermes_hid != H5I_INVALID_HID);
//     std::string data(args.request_size, '1');
//     for (size_t i = 0; i < info.num_iterations; ++i) {
//       size_t offset = GetRandomOffset(i, info.offset_seed, info.stride_size,
//                                       info.total_size);
//       test::test_seek(offset, SEEK_SET);
//       REQUIRE(((size_t)test::status_orig) == offset);
//       test::TestRead(data.data(), args.request_size);
//       REQUIRE(test::size_read_orig == args.request_size);
//     }
//     test::TestClose();
//     REQUIRE(test::hermes_herr >= 0);
//   }
//   Posttest();
// }

// TEST_CASE("BatchedUpdateStrideDynamic",
//           "[process=" + std::to_string(info.comm_size) +
//               "]"
//               "[operation=batched_write]"
//               "[request_size=type-fixed][repetition=" +
//               std::to_string(info.num_iterations) +
//               "]"
//               "[pattern=stride_dynamic][file=1]") {
//   Pretest();
//   SECTION("read from existing file") {
//     test::TestOpen(info.existing_file, H5F_ACC_RDWR);
//     REQUIRE(test::hermes_hid != H5I_INVALID_HID);
//     std::string data(args.request_size, '1');
//     for (size_t i = 0; i < info.num_iterations; ++i) {
//       size_t offset = GetRandomOffset(i, info.offset_seed, info.stride_size,
//                                       info.total_size);
//       test::test_seek(offset, SEEK_SET);
//       REQUIRE(((size_t)test::status_orig) == offset);
//       test::TestWrite(data.data(), args.request_size);
//       REQUIRE(test::size_read_orig == args.request_size);
//     }
//     test::TestClose();
//     REQUIRE(test::hermes_herr >= 0);
//   }
//   Posttest();
// }

// TEST_CASE("BatchedWriteRSVariable",
//           "[process=" + std::to_string(info.comm_size) +
//               "]"
//               "[operation=batched_write]"
//               "[request_size=type-variable][repetition=" +
//               std::to_string(info.num_iterations) +
//               "]"
//               "[pattern=sequential][file=1]") {
//   Pretest();

//   SECTION("write to new file always at the start") {
//     test::TestOpen(info.new_file, H5F_ACC_RDWR | O_CREAT | H5F_ACC_EXCL, 0600);
//     REQUIRE(test::hermes_hid != H5I_INVALID_HID);
//     size_t biggest_size_written = 0;
//     for (size_t i = 0; i < info.num_iterations; ++i) {
//       test::test_seek(0, SEEK_SET);
//       REQUIRE(test::hermes_herr >= 0);
//       size_t request_size =
//           args.request_size + (rand_r(&info.offset_seed) % args.request_size);
//       std::string data(request_size, '1');
//       test::TestWrite(data.c_str(), request_size);
//       REQUIRE(test::size_written_orig == request_size);
//       if (biggest_size_written < request_size)
//         biggest_size_written = request_size;
//     }
//     test::TestClose();
//     REQUIRE(test::hermes_herr >= 0);
//     REQUIRE(fs::file_size(info.new_file) == biggest_size_written);
//   }

//   SECTION("write to new file") {
//     test::TestOpen(info.new_file, H5F_ACC_RDWR | O_CREAT | H5F_ACC_EXCL, 0600);
//     REQUIRE(test::hermes_hid != H5I_INVALID_HID);
//     size_t total_size_written = 0;
//     for (size_t i = 0; i < info.num_iterations; ++i) {
//       size_t request_size =
//           args.request_size + (rand_r(&info.offset_seed) % args.request_size);
//       std::string data(request_size, '1');
//       test::TestWrite(data.c_str(), request_size);
//       REQUIRE(test::size_written_orig == request_size);
//       total_size_written += test::size_written_orig;
//     }
//     test::TestClose();
//     REQUIRE(test::hermes_herr >= 0);
//     REQUIRE(fs::file_size(info.new_file) == total_size_written);
//   }
//   Posttest();
// }

// TEST_CASE("BatchedReadSequentialRSVariable",
//           "[process=" + std::to_string(info.comm_size) +
//               "]"
//               "[operation=batched_read]"
//               "[request_size=type-variable][repetition=" +
//               std::to_string(info.num_iterations) +
//               "]"
//               "[pattern=sequential][file=1]") {
//   Pretest();
//   SECTION("read from existing file") {
//     test::TestOpen(info.existing_file, H5F_ACC_RDONLY);
//     REQUIRE(test::hermes_hid != H5I_INVALID_HID);
//     std::string data(args.request_size, '1');
//     size_t current_offset = 0;
//     for (size_t i = 0; i < info.num_iterations; ++i) {
//       size_t request_size = (args.request_size +
//                              (rand_r(&info.offset_seed) % args.request_size)) %
//                             (info.total_size - current_offset);
//       std::string data(request_size, '1');
//       test::TestRead(data.data(), request_size);
//       REQUIRE(test::size_read_orig == request_size);
//       current_offset += test::size_read_orig;
//     }
//     test::TestClose();
//     REQUIRE(test::hermes_herr >= 0);
//   }

//   SECTION("read from existing file always at start") {
//     test::TestOpen(info.existing_file, H5F_ACC_RDONLY);
//     REQUIRE(test::hermes_hid != H5I_INVALID_HID);

//     for (size_t i = 0; i < info.num_iterations; ++i) {
//       test::test_seek(0, SEEK_SET);
//       REQUIRE(test::hermes_herr >= 0);
//       size_t request_size =
//           args.request_size + (rand_r(&info.offset_seed) % args.request_size);
//       std::string data(request_size, '1');
//       test::TestRead(data.data(), request_size);
//       REQUIRE(test::size_read_orig == request_size);
//     }
//     test::TestClose();
//     REQUIRE(test::hermes_herr >= 0);
//   }
//   Posttest();
// }

// TEST_CASE("BatchedReadRandomRSVariable",
//           "[process=" + std::to_string(info.comm_size) +
//               "]"
//               "[operation=batched_read]"
//               "[request_size=type-variable]"
//               "[repetition=" +
//               std::to_string(info.num_iterations) +
//               "][pattern=random][file=1]") {
//   Pretest();

//   SECTION("read from existing file") {
//     test::TestOpen(info.existing_file, H5F_ACC_RDWR);
//     REQUIRE(test::hermes_hid != H5I_INVALID_HID);
//     std::string data(args.request_size, '1');
//     for (size_t i = 0; i < info.num_iterations; ++i) {
//       size_t offset =
//           rand_r(&info.offset_seed) % (info.total_size - args.request_size);
//       test::test_seek(offset, SEEK_SET);
//       REQUIRE(((size_t)test::status_orig) == offset);
//       size_t request_size =
//           (args.request_size + (rand_r(&info.rs_seed) % args.request_size)) %
//           (info.total_size - offset);
//       std::string data(request_size, '1');
//       test::TestRead(data.data(), request_size);
//       REQUIRE(test::size_read_orig == request_size);
//     }
//     test::TestClose();
//     REQUIRE(test::hermes_herr >= 0);
//   }
//   Posttest();
// }

// TEST_CASE("BatchedUpdateRandomRSVariable",
//           "[process=" + std::to_string(info.comm_size) +
//               "]"
//               "[operation=batched_write]"
//               "[request_size=type-variable][repetition=" +
//               std::to_string(info.num_iterations) +
//               "]"
//               "[pattern=random][file=1]") {
//   Pretest();

//   SECTION("read from existing file") {
//     test::TestOpen(info.existing_file, H5F_ACC_RDWR);
//     REQUIRE(test::hermes_hid != H5I_INVALID_HID);
//     std::string data(args.request_size, '1');
//     for (size_t i = 0; i < info.num_iterations; ++i) {
//       size_t offset =
//           rand_r(&info.offset_seed) % (info.total_size - args.request_size);
//       test::test_seek(offset, SEEK_SET);
//       REQUIRE(((size_t)test::status_orig) == offset);
//       size_t request_size =
//           args.request_size + (rand_r(&info.rs_seed) % args.request_size);
//       std::string data(request_size, '1');
//       test::TestWrite(data.c_str(), request_size);
//       REQUIRE(test::size_written_orig == request_size);
//     }
//     test::TestClose();
//     REQUIRE(test::hermes_herr >= 0);
//   }
//   Posttest();
// }

// TEST_CASE("BatchedReadStrideFixedRSVariable",
//           "[process=" + std::to_string(info.comm_size) +
//               "]"
//               "[operation=batched_read]"
//               "[request_size=type-variable][repetition=" +
//               std::to_string(info.num_iterations) +
//               "]"
//               "[pattern=stride_fixed][file=1]") {
//   Pretest();

//   SECTION("read from existing file") {
//     test::TestOpen(info.existing_file, H5F_ACC_RDWR);
//     REQUIRE(test::hermes_hid != H5I_INVALID_HID);
//     for (size_t i = 0; i < info.num_iterations; ++i) {
//       size_t offset = (i * info.stride_size) % info.total_size;
//       test::test_seek(offset, SEEK_SET);
//       REQUIRE(((size_t)test::status_orig) == offset);
//       size_t request_size =
//           (args.request_size + (rand_r(&info.rs_seed) % args.request_size)) %
//           (info.total_size - offset);
//       std::string data(request_size, '1');
//       test::TestRead(data.data(), request_size);
//       REQUIRE(test::size_read_orig == request_size);
//     }
//     test::TestClose();
//     REQUIRE(test::hermes_herr >= 0);
//   }
//   Posttest();
// }

// TEST_CASE("BatchedUpdateStrideFixedRSVariable",
//           "[process=" + std::to_string(info.comm_size) +
//               "]"
//               "[operation=batched_write]"
//               "[request_size=type-variable][repetition=" +
//               std::to_string(info.num_iterations) +
//               "]"
//               "[pattern=stride_fixed][file=1]") {
//   Pretest();

//   SECTION("write to existing file") {
//     test::TestOpen(info.existing_file, H5F_ACC_RDWR);
//     REQUIRE(test::hermes_hid != H5I_INVALID_HID);
//     std::string data(args.request_size, '1');
//     for (size_t i = 0; i < info.num_iterations; ++i) {
//       size_t offset = (i * info.stride_size) % info.total_size;
//       test::test_seek(offset, SEEK_SET);
//       REQUIRE(((size_t)test::status_orig) == offset);
//       size_t request_size =
//           args.request_size + (rand_r(&info.rs_seed) % args.request_size);
//       std::string data(request_size, '1');
//       test::TestWrite(data.c_str(), request_size);
//       REQUIRE(test::size_written_orig == request_size);
//     }
//     test::TestClose();
//     REQUIRE(test::hermes_herr >= 0);
//   }
//   Posttest();
// }

// TEST_CASE("BatchedReadStrideDynamicRSVariable",
//           "[process=" + std::to_string(info.comm_size) +
//               "]"
//               "[operation=batched_read]"
//               "[request_size=type-variable][repetition=" +
//               std::to_string(info.num_iterations) +
//               "]"
//               "[pattern=stride_dynamic][file=1]") {
//   Pretest();

//   SECTION("read from existing file") {
//     test::TestOpen(info.existing_file, H5F_ACC_RDWR);
//     REQUIRE(test::hermes_hid != H5I_INVALID_HID);
//     for (size_t i = 0; i < info.num_iterations; ++i) {
//       size_t offset = GetRandomOffset(i, info.offset_seed, info.stride_size,
//                                       info.total_size);
//       test::test_seek(offset, SEEK_SET);
//       REQUIRE(((size_t)test::status_orig) == offset);
//       size_t request_size =
//           args.request_size + (rand_r(&info.rs_seed) % args.request_size);
//       std::string data(request_size, '1');
//       test::TestRead(data.data(), request_size);
//       REQUIRE(test::size_read_orig == request_size);
//     }
//     test::TestClose();
//     REQUIRE(test::hermes_herr >= 0);
//   }
//   Posttest();
// }

// TEST_CASE("BatchedUpdateStrideDynamicRSVariable",
//           "[process=" + std::to_string(info.comm_size) +
//               "]"
//               "[operation=batched_write]"
//               "[request_size=type-variable][repetition=" +
//               std::to_string(info.num_iterations) +
//               "]"
//               "[pattern=stride_dynamic][file=1]") {
//   Pretest();
//   SECTION("read from existing file") {
//     test::TestOpen(info.existing_file, H5F_ACC_RDWR);
//     REQUIRE(test::hermes_hid != H5I_INVALID_HID);
//     for (size_t i = 0; i < info.num_iterations; ++i) {
//       size_t offset = GetRandomOffset(i, info.offset_seed, info.stride_size,
//                                       info.total_size);
//       test::test_seek(offset, SEEK_SET);
//       REQUIRE(((size_t)test::status_orig) == offset);
//       size_t request_size =
//           args.request_size + (rand_r(&info.rs_seed) % args.request_size);
//       std::string data(request_size, '1');
//       test::TestWrite(data.c_str(), request_size);
//       REQUIRE(test::size_written_orig == request_size);
//     }
//     test::TestClose();
//     REQUIRE(test::hermes_herr >= 0);
//   }
//   Posttest();
// }

// TEST_CASE("BatchedReadStrideNegative",
//           "[process=" + std::to_string(info.comm_size) +
//               "]"
//               "[operation=batched_read]"
//               "[request_size=type-fixed][repetition=" +
//               std::to_string(info.num_iterations) +
//               "]"
//               "[pattern=stride_negative][file=1]") {
//   Pretest();
//   SECTION("read from existing file") {
//     test::TestOpen(info.existing_file, H5F_ACC_RDWR);
//     REQUIRE(test::hermes_hid != H5I_INVALID_HID);
//     std::string data(args.request_size, '1');
//     size_t prev_offset = info.total_size + 1;
//     for (size_t i = 0; i < info.num_iterations; ++i) {
//       auto stride_offset = info.total_size - i * info.stride_size;
//       REQUIRE(prev_offset > stride_offset);
//       prev_offset = stride_offset;
//       size_t offset = (stride_offset) % (info.total_size - args.request_size);
//       test::test_seek(offset, SEEK_SET);
//       REQUIRE(((size_t)test::status_orig) == offset);
//       test::TestRead(data.data(), args.request_size);
//       REQUIRE(test::size_read_orig == args.request_size);
//     }
//     test::TestClose();
//     REQUIRE(test::hermes_herr >= 0);
//   }
//   Posttest();
// }

// TEST_CASE("BatchedUpdateStrideNegative",
//           "[process=" + std::to_string(info.comm_size) +
//               "]"
//               "[operation=batched_write]"
//               "[request_size=type-fixed][repetition=" +
//               std::to_string(info.num_iterations) +
//               "]"
//               "[pattern=stride_negative][file=1]") {
//   Pretest();
//   SECTION("read from existing file") {
//     test::TestOpen(info.existing_file, H5F_ACC_RDWR);
//     REQUIRE(test::hermes_hid != H5I_INVALID_HID);
//     std::string data(args.request_size, '1');
//     for (size_t i = 0; i < info.num_iterations; ++i) {
//       size_t offset =
//           info.total_size - ((i * info.stride_size) % info.total_size);
//       test::test_seek(offset, SEEK_SET);
//       REQUIRE(((size_t)test::status_orig) == offset);
//       test::TestWrite(data.data(), args.request_size);
//       REQUIRE(test::size_read_orig == args.request_size);
//     }
//     test::TestClose();
//     REQUIRE(test::hermes_herr >= 0);
//   }
//   Posttest();
// }

// TEST_CASE("BatchedReadStrideNegativeRSVariable",
//           "[process=" + std::to_string(info.comm_size) +
//               "]"
//               "[operation=batched_read]"
//               "[request_size=type-variable][repetition=" +
//               std::to_string(info.num_iterations) +
//               "]"
//               "[pattern=stride_negative][file=1]") {
//   Pretest();

//   SECTION("read from existing file") {
//     test::TestOpen(info.existing_file, H5F_ACC_RDWR);
//     REQUIRE(test::hermes_hid != H5I_INVALID_HID);
//     for (size_t i = 0; i < info.num_iterations; ++i) {
//       size_t offset = (info.total_size - i * info.stride_size) %
//                       (info.total_size - 2 * args.request_size);
//       test::test_seek(offset, SEEK_SET);
//       REQUIRE(((size_t)test::status_orig) == offset);
//       size_t request_size =
//           (args.request_size + (rand_r(&info.rs_seed) % args.request_size)) %
//           (info.total_size - offset);
//       std::string data(request_size, '1');
//       test::TestRead(data.data(), request_size);
//       REQUIRE(test::size_read_orig == request_size);
//     }
//     test::TestClose();
//     REQUIRE(test::hermes_herr >= 0);
//   }
//   Posttest();
// }

// TEST_CASE("BatchedUpdateStrideNegativeRSVariable",
//           "[process=" + std::to_string(info.comm_size) +
//               "]"
//               "[operation=batched_write]"
//               "[request_size=type-variable][repetition=" +
//               std::to_string(info.num_iterations) +
//               "]"
//               "[pattern=stride_negative][file=1]") {
//   Pretest();

//   SECTION("write to existing file") {
//     test::TestOpen(info.existing_file, H5F_ACC_RDWR);
//     REQUIRE(test::hermes_hid != H5I_INVALID_HID);
//     std::string data(args.request_size, '1');
//     for (size_t i = 0; i < info.num_iterations; ++i) {
//       size_t offset =
//           info.total_size - ((i * info.stride_size) % info.total_size);
//       test::test_seek(offset, SEEK_SET);
//       REQUIRE(((size_t)test::status_orig) == offset);
//       size_t request_size =
//           args.request_size + (rand_r(&info.rs_seed) % args.request_size);
//       std::string data(request_size, '1');
//       test::TestWrite(data.c_str(), request_size);
//       REQUIRE(test::size_written_orig == request_size);
//     }
//     test::TestClose();
//     REQUIRE(test::hermes_herr >= 0);
//   }
//   Posttest();
// }

// TEST_CASE("BatchedReadStride2D", "[process=" + std::to_string(info.comm_size) +
//                                      "]"
//                                      "[operation=batched_read]"
//                                      "[request_size=type-fixed][repetition=" +
//                                      std::to_string(info.num_iterations) +
//                                      "]"
//                                      "[pattern=stride_2d][file=1]") {
//   Pretest();
//   size_t rows = sqrt(info.total_size);
//   size_t cols = rows;
//   REQUIRE(rows * cols == info.total_size);
//   size_t cell_size = 128;
//   size_t cell_stride = rows * cols / cell_size / info.num_iterations;
//   SECTION("read from existing file") {
//     test::TestOpen(info.existing_file, H5F_ACC_RDWR);
//     REQUIRE(test::hermes_hid != H5I_INVALID_HID);
//     std::string data(args.request_size, '1');
//     size_t prev_cell_col = 0, prev_cell_row = 0;
//     for (size_t i = 0; i < info.num_iterations; ++i) {
//       size_t current_cell_col = (prev_cell_col + cell_stride) % cols;
//       size_t current_cell_row = prev_cell_col + cell_stride > cols
//                                     ? prev_cell_row + 1
//                                     : prev_cell_row;
//       prev_cell_row = current_cell_row;
//       size_t offset = (current_cell_col * cell_stride + prev_cell_row * cols) %
//                       (info.total_size - args.request_size);
//       test::test_seek(offset, SEEK_SET);
//       REQUIRE(((size_t)test::status_orig) == offset);
//       test::TestRead(data.data(), args.request_size);
//       REQUIRE(test::size_read_orig == args.request_size);
//     }
//     test::TestClose();
//     REQUIRE(test::hermes_herr >= 0);
//   }
//   Posttest();
// }

// TEST_CASE("BatchedUpdateStride2D",
//           "[process=" + std::to_string(info.comm_size) +
//               "]"
//               "[operation=batched_write]"
//               "[request_size=type-fixed][repetition=" +
//               std::to_string(info.num_iterations) +
//               "]"
//               "[pattern=stride_2d][file=1]") {
//   Pretest();

//   size_t rows = sqrt(info.total_size);
//   size_t cols = rows;
//   REQUIRE(rows * cols == info.total_size);
//   size_t cell_size = 128;
//   size_t cell_stride = rows * cols / cell_size / info.num_iterations;
//   SECTION("read from existing file") {
//     test::TestOpen(info.existing_file, H5F_ACC_RDWR);
//     REQUIRE(test::hermes_hid != H5I_INVALID_HID);
//     std::string data(args.request_size, '1');
//     size_t prev_cell_col = 0, prev_cell_row = 0;
//     for (size_t i = 0; i < info.num_iterations; ++i) {
//       size_t current_cell_col = (prev_cell_col + cell_stride) % cols;
//       size_t current_cell_row = prev_cell_col + cell_stride > cols
//                                     ? prev_cell_row + 1
//                                     : prev_cell_row;
//       prev_cell_row = current_cell_row;
//       size_t offset = (current_cell_col * cell_stride + prev_cell_row * cols) %
//                       (info.total_size - args.request_size);
//       test::test_seek(offset, SEEK_SET);
//       REQUIRE(((size_t)test::status_orig) == offset);
//       test::TestWrite(data.data(), args.request_size);
//       REQUIRE(test::size_read_orig == args.request_size);
//     }
//     test::TestClose();
//     REQUIRE(test::hermes_herr >= 0);
//   }
//   Posttest();
// }

// TEST_CASE("BatchedReadStride2DRSVariable",
//           "[process=" + std::to_string(info.comm_size) +
//               "]"
//               "[operation=batched_read]"
//               "[request_size=type-variable][repetition=" +
//               std::to_string(info.num_iterations) +
//               "]"
//               "[pattern=stride_2d][file=1]") {
//   Pretest();
//   size_t rows = sqrt(info.total_size);
//   size_t cols = rows;
//   REQUIRE(rows * cols == info.total_size);
//   size_t cell_size = 128;
//   size_t cell_stride = rows * cols / cell_size / info.num_iterations;
//   SECTION("read from existing file") {
//     test::TestOpen(info.existing_file, H5F_ACC_RDWR);
//     REQUIRE(test::hermes_hid != H5I_INVALID_HID);
//     size_t prev_cell_col = 0, prev_cell_row = 0;
//     for (size_t i = 0; i < info.num_iterations; ++i) {
//       size_t current_cell_col = (prev_cell_col + cell_stride) % cols;
//       size_t current_cell_row = prev_cell_col + cell_stride > cols
//                                     ? prev_cell_row + 1
//                                     : prev_cell_row;
//       prev_cell_row = current_cell_row;
//       size_t offset = (current_cell_col * cell_stride + prev_cell_row * cols) %
//                       (info.total_size - 2 * args.request_size);
//       test::test_seek(offset, SEEK_SET);
//       REQUIRE(((size_t)test::status_orig) == offset);
//       size_t request_size =
//           (args.request_size + (rand_r(&info.rs_seed) % args.request_size)) %
//           (info.total_size - offset);
//       std::string data(request_size, '1');
//       test::TestRead(data.data(), request_size);
//       REQUIRE(test::size_read_orig == request_size);
//     }
//     test::TestClose();
//     REQUIRE(test::hermes_herr >= 0);
//   }
//   Posttest();
// }

// TEST_CASE("BatchedUpdateStride2DRSVariable",
//           "[process=" + std::to_string(info.comm_size) +
//               "]"
//               "[operation=batched_write]"
//               "[request_size=type-variable][repetition=" +
//               std::to_string(info.num_iterations) +
//               "]"
//               "[pattern=stride_2d][file=1]") {
//   Pretest();
//   size_t rows = sqrt(info.total_size);
//   size_t cols = rows;
//   REQUIRE(rows * cols == info.total_size);
//   size_t cell_size = 128;
//   size_t cell_stride = rows * cols / cell_size / info.num_iterations;
//   SECTION("write to existing file") {
//     test::TestOpen(info.existing_file, H5F_ACC_RDWR);
//     REQUIRE(test::hermes_hid != H5I_INVALID_HID);
//     std::string data(args.request_size, '1');
//     size_t prev_cell_col = 0, prev_cell_row = 0;
//     for (size_t i = 0; i < info.num_iterations; ++i) {
//       size_t current_cell_col = (prev_cell_col + cell_stride) % cols;
//       size_t current_cell_row = prev_cell_col + cell_stride > cols
//                                     ? prev_cell_row + 1
//                                     : prev_cell_row;
//       prev_cell_row = current_cell_row;
//       size_t offset = (current_cell_col * cell_stride + prev_cell_row * cols) %
//                       (info.total_size - 2 * args.request_size);
//       test::test_seek(offset, SEEK_SET);
//       REQUIRE(((size_t)test::status_orig) == offset);
//       size_t request_size =
//           args.request_size + (rand_r(&info.rs_seed) % args.request_size);
//       std::string data(request_size, '1');
//       test::TestWrite(data.c_str(), request_size);
//       REQUIRE(test::size_written_orig == request_size);
//     }
//     test::TestClose();
//     REQUIRE(test::hermes_herr >= 0);
//   }
//   Posttest();
// }

// /**
//  * Temporal Fixed
//  */

// TEST_CASE("BatchedWriteTemporalFixed",
//           "[process=" + std::to_string(info.comm_size) +
//               "]"
//               "[operation=batched_write]"
//               "[request_size=type-fixed][repetition=" +
//               std::to_string(info.num_iterations) +
//               "]"
//               "[pattern=sequential][file=1][temporal=fixed]") {
//   Pretest();

//   SECTION("write to existing file") {
//     test::TestOpen(info.new_file, H5F_ACC_RDWR | O_CREAT | H5F_ACC_EXCL, 0600);
//     REQUIRE(test::hermes_hid != H5I_INVALID_HID);

//     for (size_t i = 0; i < info.num_iterations; ++i) {
//       usleep(info.temporal_interval_ms * 1000);
//       test::test_seek(0, SEEK_SET);
//       REQUIRE(test::hermes_herr >= 0);
//       test::TestWrite(info.write_data.data(), args.request_size);
//       REQUIRE(test::size_written_orig == args.request_size);
//     }
//     test::TestClose();
//     REQUIRE(test::hermes_herr >= 0);
//     REQUIRE(fs::file_size(info.new_file) == args.request_size);
//   }

//   SECTION("write to new file always at start") {
//     test::TestOpen(info.new_file, H5F_ACC_RDWR | O_CREAT | H5F_ACC_EXCL, 0600);
//     REQUIRE(test::hermes_hid != H5I_INVALID_HID);

//     for (size_t i = 0; i < info.num_iterations; ++i) {
//       usleep(info.temporal_interval_ms * 1000);
//       test::TestWrite(info.write_data.data(), args.request_size);
//       REQUIRE(test::size_written_orig == args.request_size);
//     }
//     test::TestClose();
//     REQUIRE(test::hermes_herr >= 0);
//     REQUIRE(fs::file_size(info.new_file) ==
//             info.num_iterations * args.request_size);
//   }
//   Posttest();
// }

// TEST_CASE("BatchedReadSequentialTemporalFixed",
//           "[process=" + std::to_string(info.comm_size) +
//               "]"
//               "[operation=batched_read]"
//               "[request_size=type-fixed][repetition=" +
//               std::to_string(info.num_iterations) +
//               "]"
//               "[pattern=sequential][file=1][temporal=fixed]") {
//   Pretest();

//   SECTION("read from existing file") {
//     test::TestOpen(info.existing_file, H5F_ACC_RDWR);
//     REQUIRE(test::hermes_hid != H5I_INVALID_HID);
//     std::string data(args.request_size, '1');
//     for (size_t i = 0; i < info.num_iterations; ++i) {
//       usleep(info.temporal_interval_ms * 1000);
//       test::TestRead(data.data(), args.request_size);
//       REQUIRE(test::size_read_orig == args.request_size);
//     }
//     test::TestClose();
//     REQUIRE(test::hermes_herr >= 0);
//   }

//   SECTION("read from existing file always at start") {
//     test::TestOpen(info.existing_file, H5F_ACC_RDWR);
//     REQUIRE(test::hermes_hid != H5I_INVALID_HID);

//     for (size_t i = 0; i < info.num_iterations; ++i) {
//       usleep(info.temporal_interval_ms * 1000);
//       test::test_seek(0, SEEK_SET);
//       REQUIRE(test::hermes_herr >= 0);
//       test::TestWrite(info.write_data.data(), args.request_size);
//       REQUIRE(test::size_written_orig == args.request_size);
//     }
//     test::TestClose();
//     REQUIRE(test::hermes_herr >= 0);
//   }
//   Posttest();
// }

// TEST_CASE("BatchedWriteTemporalVariable",
//           "[process=" + std::to_string(info.comm_size) +
//               "]"
//               "[operation=batched_write]"
//               "[request_size=type-fixed][repetition=" +
//               std::to_string(info.num_iterations) +
//               "]"
//               "[pattern=sequential][file=1][temporal=variable]") {
//   Pretest();

//   SECTION("write to existing file") {
//     test::TestOpen(info.new_file, H5F_ACC_RDWR | O_CREAT | H5F_ACC_EXCL, 0600);
//     REQUIRE(test::hermes_hid != H5I_INVALID_HID);

//     for (size_t i = 0; i < info.num_iterations; ++i) {
//       info.temporal_interval_ms =
//           rand_r(&info.temporal_interval_seed) % info.temporal_interval_ms + 1;
//       usleep(info.temporal_interval_ms * 1000);
//       test::test_seek(0, SEEK_SET);
//       REQUIRE(test::hermes_herr >= 0);
//       test::TestWrite(info.write_data.data(), args.request_size);
//       REQUIRE(test::size_written_orig == args.request_size);
//     }
//     test::TestClose();
//     REQUIRE(test::hermes_herr >= 0);
//     REQUIRE(fs::file_size(info.new_file) == args.request_size);
//   }

//   SECTION("write to new file always at start") {
//     test::TestOpen(info.new_file, H5F_ACC_RDWR | O_CREAT | H5F_ACC_EXCL, 0600);
//     REQUIRE(test::hermes_hid != H5I_INVALID_HID);

//     for (size_t i = 0; i < info.num_iterations; ++i) {
//       info.temporal_interval_ms =
//           rand_r(&info.temporal_interval_seed) % info.temporal_interval_ms + 1;
//       usleep(info.temporal_interval_ms * 1000);
//       test::TestWrite(info.write_data.data(), args.request_size);
//       REQUIRE(test::size_written_orig == args.request_size);
//     }
//     test::TestClose();
//     REQUIRE(test::hermes_herr >= 0);
//     REQUIRE(fs::file_size(info.new_file) ==
//             info.num_iterations * args.request_size);
//   }
//   Posttest();
// }

// TEST_CASE("BatchedReadSequentialTemporalVariable",
//           "[process=" + std::to_string(info.comm_size) +
//               "]"
//               "[operation=batched_read]"
//               "[request_size=type-fixed][repetition=" +
//               std::to_string(info.num_iterations) +
//               "]"
//               "[pattern=sequential][file=1][temporal=variable]") {
//   Pretest();

//   SECTION("read from existing file") {
//     test::TestOpen(info.existing_file, H5F_ACC_RDWR);
//     REQUIRE(test::hermes_hid != H5I_INVALID_HID);
//     std::string data(args.request_size, '1');
//     for (size_t i = 0; i < info.num_iterations; ++i) {
//       info.temporal_interval_ms =
//           rand_r(&info.temporal_interval_seed) % info.temporal_interval_ms + 1;
//       usleep(info.temporal_interval_ms * 1000);
//       test::TestRead(data.data(), args.request_size);
//       REQUIRE(test::size_read_orig == args.request_size);
//     }
//     test::TestClose();
//     REQUIRE(test::hermes_herr >= 0);
//   }

//   SECTION("read from existing file always at start") {
//     test::TestOpen(info.existing_file, H5F_ACC_RDWR);
//     REQUIRE(test::hermes_hid != H5I_INVALID_HID);

//     for (size_t i = 0; i < info.num_iterations; ++i) {
//       info.temporal_interval_ms =
//           rand_r(&info.temporal_interval_seed) % info.temporal_interval_ms + 1;
//       usleep(info.temporal_interval_ms * 1000);
//       test::test_seek(0, SEEK_SET);
//       REQUIRE(test::hermes_herr >= 0);
//       test::TestWrite(info.write_data.data(), args.request_size);
//       REQUIRE(test::size_written_orig == args.request_size);
//     }
//     test::TestClose();
//     REQUIRE(test::hermes_herr >= 0);
//   }
//   Posttest();
// }

// TEST_CASE("BatchedMixedSequential",
//           "[process=" + std::to_string(info.comm_size) +
//               "]"
//               "[operation=batched_mixed]"
//               "[request_size=type-fixed][repetition=" +
//               std::to_string(info.num_iterations) +
//               "]"
//               "[pattern=sequential][file=1]") {
//   Pretest();
//   SECTION("read after write on new file") {
//     test::TestOpen(info.new_file, H5F_ACC_RDWR | O_CREAT | H5F_ACC_EXCL, 0600);
//     REQUIRE(test::hermes_hid != H5I_INVALID_HID);
//     size_t last_offset = 0;
//     for (size_t i = 0; i < info.num_iterations; ++i) {
//       test::TestWrite(info.write_data.data(), args.request_size);
//       REQUIRE(test::size_written_orig == args.request_size);
//       test::test_seek(last_offset, SEEK_SET);
//       REQUIRE(((size_t)test::status_orig) == last_offset);
//       test::TestRead(info.read_data.data(), args.request_size);
//       REQUIRE(test::size_read_orig == args.request_size);
//       last_offset += args.request_size;
//     }
//     test::TestClose();
//     REQUIRE(test::hermes_herr >= 0);
//   }

//   SECTION("write and read alternative existing file") {
//     test::TestOpen(info.existing_file, H5F_ACC_RDWR);
//     REQUIRE(test::hermes_hid != H5I_INVALID_HID);
//     for (size_t i = 0; i < info.num_iterations; ++i) {
//       if (i % 2 == 0) {
//         test::TestWrite(info.write_data.data(), args.request_size);
//         REQUIRE(test::size_written_orig == args.request_size);
//       } else {
//         test::TestRead(info.read_data.data(), args.request_size);
//         REQUIRE(test::size_read_orig == args.request_size);
//       }
//     }
//     test::TestClose();
//     REQUIRE(test::hermes_herr >= 0);
//   }
//   SECTION("update after read existing file") {
//     test::TestOpen(info.existing_file, H5F_ACC_RDWR);
//     REQUIRE(test::hermes_hid != H5I_INVALID_HID);
//     size_t last_offset = 0;
//     for (size_t i = 0; i < info.num_iterations; ++i) {
//       test::TestRead(info.read_data.data(), args.request_size);
//       REQUIRE(test::size_read_orig == args.request_size);
//       test::test_seek(last_offset, SEEK_SET);
//       REQUIRE(((size_t)test::status_orig) == last_offset);
//       test::TestWrite(info.write_data.data(), args.request_size);
//       REQUIRE(test::size_written_orig == args.request_size);
//       last_offset += args.request_size;
//     }

//     test::TestClose();
//     REQUIRE(test::hermes_herr >= 0);
//   }
//   SECTION("read all after write all on new file in single open") {
//     test::TestOpen(info.new_file, H5F_ACC_RDWR | O_CREAT | H5F_ACC_EXCL, 0600);
//     REQUIRE(test::hermes_hid != H5I_INVALID_HID);
//     for (size_t i = 0; i < info.num_iterations; ++i) {
//       test::TestWrite(info.write_data.data(), args.request_size);
//       REQUIRE(test::size_written_orig == args.request_size);
//     }
//     test::test_seek(0, SEEK_SET);
//     REQUIRE(test::hermes_herr >= 0);
//     for (size_t i = 0; i < info.num_iterations; ++i) {
//       test::TestRead(info.read_data.data(), args.request_size);
//       REQUIRE(test::size_read_orig == args.request_size);
//     }
//     test::TestClose();
//     REQUIRE(test::hermes_herr >= 0);
//   }
//   SECTION("read all after write all on new file in different open") {
//     test::TestOpen(info.new_file, H5F_ACC_RDWR | O_CREAT, S_IRWXU | S_IRWXG);
//     REQUIRE(test::hermes_hid != H5I_INVALID_HID);
//     for (size_t i = 0; i < info.num_iterations; ++i) {
//       test::TestWrite(info.write_data.data(), args.request_size);
//       REQUIRE(test::size_written_orig == args.request_size);
//     }
//     test::TestClose();
//     REQUIRE(test::hermes_herr >= 0);
//     test::TestOpen(info.new_file, H5F_ACC_RDWR);
//     REQUIRE(test::hermes_hid != H5I_INVALID_HID);
//     for (size_t i = 0; i < info.num_iterations; ++i) {
//       test::TestRead(info.read_data.data(), args.request_size);
//       REQUIRE(test::size_read_orig == args.request_size);
//     }
//     test::TestClose();
//     REQUIRE(test::hermes_herr >= 0);
//   }
//   Posttest();
// }

// TEST_CASE("SingleMixed", "[process=" + std::to_string(info.comm_size) +
//                              "][operation=single_mixed]"
//                              "[request_size=type-fixed][repetition=1]"
//                              "[file=1]") {
//   Pretest();
//   SECTION("read after write from new file") {
//     test::TestOpen(info.new_file, H5F_ACC_RDWR | O_CREAT | H5F_ACC_EXCL, 0600);
//     REQUIRE(test::hermes_hid != H5I_INVALID_HID);
//     test::TestWrite(info.write_data.data(), args.request_size);
//     REQUIRE(test::size_written_orig == args.request_size);
//     test::test_seek(0, SEEK_SET);
//     REQUIRE(test::hermes_herr >= 0);
//     test::TestRead(info.read_data.data(), args.request_size);
//     REQUIRE(test::size_read_orig == args.request_size);
//     test::TestClose();
//     REQUIRE(test::hermes_herr >= 0);
//   }
//   SECTION("update after read from existing file") {
//     test::TestOpen(info.existing_file, H5F_ACC_RDWR);
//     REQUIRE(test::hermes_hid != H5I_INVALID_HID);
//     test::TestRead(info.read_data.data(), args.request_size);
//     REQUIRE(test::size_read_orig == args.request_size);
//     test::test_seek(0, SEEK_SET);
//     REQUIRE(test::hermes_herr >= 0);
//     test::TestWrite(info.write_data.data(), args.request_size);
//     REQUIRE(test::size_written_orig == args.request_size);

//     test::TestClose();
//     REQUIRE(test::hermes_herr >= 0);
//   }
//   SECTION("read after write from new file different opens") {
//     test::TestOpen(info.new_file, H5F_ACC_RDWR | O_CREAT | H5F_ACC_EXCL, 0600);
//     REQUIRE(test::hermes_hid != H5I_INVALID_HID);
//     test::TestWrite(info.write_data.data(), args.request_size);
//     REQUIRE(test::size_written_orig == args.request_size);

//     REQUIRE(test::hermes_herr >= 0);
//     test::TestOpen(info.new_file, H5F_ACC_RDWR);
//     test::TestRead(info.read_data.data(), args.request_size);
//     REQUIRE(test::size_read_orig == args.request_size);
//     test::TestClose();
//     REQUIRE(test::hermes_herr >= 0);
//   }
//   Posttest();
// }

