TEST_CASE("SharedSTDIORead", "[process=" + std::to_string(info.comm_size) +
                                 "]"
                                 "[operation=batched_read]"
                                 "[request_size=type-fixed][repetition=" +
                                 std::to_string(info.num_iterations) +
                                 "]"
                                 "[mode=shared]"
                                 "[pattern=sequential][file=1]") {
  pretest();

  SECTION("read from existing file") {
    test::test_fopen(info.existing_shared_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    std::string data(args.request_size, '1');
    for (size_t i = 0; i < info.num_iterations; ++i) {
      test::test_fread(data.data(), args.request_size);
      REQUIRE(test::size_read_orig == args.request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}

TEST_CASE("SharedSTDIOWrite", "[process=" + std::to_string(info.comm_size) +
                                  "]"
                                  "[operation=batched_write]"
                                  "[request_size=type-fixed][repetition=" +
                                  std::to_string(info.num_iterations) +
                                  "]"
                                  "[mode=shared]"
                                  "[pattern=sequential][file=1]") {
  pretest();

  SECTION("write into existing file") {
    test::test_fopen(info.existing_shared_file.c_str(), "r+");
    REQUIRE(test::fh_orig != nullptr);
    size_t iter_per_rank = info.num_iterations / info.comm_size;
    size_t start_iter = info.rank * iter_per_rank;
    test::test_fseek(start_iter * args.request_size, SEEK_SET);
    REQUIRE(test::status_orig == 0);
    for (size_t i = start_iter; i < start_iter + iter_per_rank; ++i) {
      test::test_fwrite(info.write_data.data(), args.request_size);
      REQUIRE(test::size_written_orig == args.request_size);
    }
    test::test_fclose();
    REQUIRE(test::status_orig == 0);
  }
  posttest();
}
