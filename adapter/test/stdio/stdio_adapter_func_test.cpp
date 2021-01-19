TEST_CASE("FFlush", "[process=" + std::to_string(info.comm_size) +
                        "]"
                        "[operation=single_fflush]"
                        "[repetition=1][file=1]") {
  pretest();
  SECTION("Flushing contents of file in different modes") {
    FILE* fd = fopen(info.existing_file.c_str(), "w");
    REQUIRE(fd != nullptr);
    int status = fflush(fd);
    REQUIRE(status == 0);
    status = fclose(fd);
    REQUIRE(status == 0);

    fd = fopen(info.existing_file.c_str(), "w+");
    REQUIRE(fd != nullptr);
    status = fflush(fd);
    REQUIRE(status == 0);
    status = fclose(fd);
    REQUIRE(status == 0);

    fd = fopen(info.existing_file.c_str(), "r+");
    REQUIRE(fd != nullptr);
    status = fflush(fd);
    REQUIRE(status == 0);
    status = fclose(fd);
    REQUIRE(status == 0);

    fd = fopen(info.existing_file.c_str(), "r");
    REQUIRE(fd != nullptr);
    status = fflush(fd);
    REQUIRE(status == 0);
    status = fclose(fd);
    REQUIRE(status == 0);

    fd = fopen(info.existing_file.c_str(), "a");
    REQUIRE(fd != nullptr);
    status = fflush(fd);
    REQUIRE(status == 0);
    status = fclose(fd);
    REQUIRE(status == 0);

    fd = fopen(info.existing_file.c_str(), "a+");
    REQUIRE(fd != nullptr);
    status = fflush(fd);
    REQUIRE(status == 0);
    status = fclose(fd);
    REQUIRE(status == 0);
  }
  SECTION("Flushing contents of all files") {
    int status = fflush(nullptr);
    REQUIRE(status == 0);
  }
  posttest();
}

TEST_CASE("Fdopen", "[process=" + std::to_string(info.comm_size) +
                        "]"
                        "[operation=single_fdopen]"
                        "[repetition=1][file=1]") {
  pretest();
  SECTION("Associate a FILE ptr with read mode") {
    int fd = open(info.existing_file.c_str(), O_RDWR);
    REQUIRE(fd != -1);

    FILE* fh = fdopen(fd, "r");
    REQUIRE(fh != nullptr);
    size_t read_size =
        fread(info.read_data.data(), sizeof(char), args.request_size, fh);
    REQUIRE(read_size == args.request_size);
    int status = fclose(fh);
    REQUIRE(status == 0);

    status = fcntl(fd, F_GETFD);
    REQUIRE(fd != -1);

    status = close(fd);
    REQUIRE(status == -1);
  }
  SECTION("Associate a FILE ptr with write mode") {
    int fd = open(info.existing_file.c_str(), O_RDWR);
    REQUIRE(fd != -1);

    FILE* fh = fdopen(fd, "w");
    REQUIRE(fh != nullptr);
    size_t write_size =
        fwrite(info.write_data.c_str(), sizeof(char), args.request_size, fh);
    REQUIRE(write_size == args.request_size);
    int status = fclose(fh);
    REQUIRE(status == 0);

    status = close(fd);
    REQUIRE(status == -1);
  }
  SECTION("Associate a FILE ptr with read plus mode") {
    int fd = open(info.existing_file.c_str(), O_RDWR);
    REQUIRE(fd != -1);

    FILE* fh = fdopen(fd, "r");
    REQUIRE(fh != nullptr);
    size_t read_size =
        fread(info.read_data.data(), sizeof(char), args.request_size, fh);
    REQUIRE(read_size == args.request_size);
    int status = fclose(fh);
    REQUIRE(status == 0);

    status = fcntl(fd, F_GETFD);
    REQUIRE(fd != -1);

    status = close(fd);
    REQUIRE(status == -1);
  }
  SECTION("Associate a FILE ptr with write plus mode") {
    int fd = open(info.existing_file.c_str(), O_RDWR);
    REQUIRE(fd != -1);

    FILE* fh = fdopen(fd, "w+");
    REQUIRE(fh != nullptr);
    size_t write_size =
        fwrite(info.write_data.c_str(), sizeof(char), args.request_size, fh);
    REQUIRE(write_size == args.request_size);
    int status = fclose(fh);
    REQUIRE(status == 0);

    status = fcntl(fd, F_GETFD);
    REQUIRE(fd != -1);

    status = close(fd);
    REQUIRE(status == -1);
  }
  SECTION("Associate a FILE ptr with append mode") {
    int fd = open(info.existing_file.c_str(), O_RDWR | O_APPEND);
    REQUIRE(fd != -1);

    FILE* fh = fdopen(fd, "a");
    REQUIRE(fh != nullptr);
    size_t write_size =
        fwrite(info.write_data.c_str(), sizeof(char), args.request_size, fh);
    REQUIRE(write_size == args.request_size);
    int status = fclose(fh);
    REQUIRE(status == 0);

    status = fcntl(fd, F_GETFD);
    REQUIRE(fd != -1);

    status = close(fd);
    REQUIRE(status == -1);
  }
  SECTION("Associate a FILE ptr with append plus mode") {
    int fd = open(info.existing_file.c_str(), O_RDWR | O_APPEND);
    REQUIRE(fd != -1);

    FILE* fh = fdopen(fd, "a+");
    REQUIRE(fh != nullptr);
    size_t write_size =
        fwrite(info.write_data.c_str(), sizeof(char), args.request_size, fh);
    REQUIRE(write_size == args.request_size);
    int status = fclose(fh);
    REQUIRE(status == 0);

    status = fcntl(fd, F_GETFD);
    REQUIRE(fd != -1);

    status = close(fd);
    REQUIRE(status == -1);
  }
  SECTION("Associate a FILE ptr with read mode twice") {
    int fd = open(info.existing_file.c_str(), O_RDWR);
    REQUIRE(fd != -1);

    FILE* fh = fdopen(fd, "r");
    REQUIRE(fh != nullptr);
    size_t read_size =
        fread(info.read_data.data(), sizeof(char), args.request_size, fh);
    REQUIRE(read_size == args.request_size);

    int status = fcntl(fd, F_GETFD);
    REQUIRE(fd != -1);

    FILE* fh2 = fdopen(fd, "r");
    REQUIRE(fh2 != nullptr);

    status = fclose(fh2);
    REQUIRE(status == 0);

    status = fclose(fh);
    REQUIRE(status == -1);

    status = close(fd);
    REQUIRE(status == -1);
  }
  SECTION("Associate a FILE ptr with read mode twice after one closes") {
    int fd = open(info.existing_file.c_str(), O_RDWR);
    REQUIRE(fd != -1);

    FILE* fh = fdopen(fd, "r");
    REQUIRE(fh != nullptr);
    size_t read_size =
        fread(info.read_data.data(), sizeof(char), args.request_size, fh);
    REQUIRE(read_size == args.request_size);

    int status = fcntl(fd, F_GETFD);
    REQUIRE(fd != -1);

    status = fclose(fh);
    REQUIRE(status == 0);

    FILE* fh2 = fdopen(fd, "r");
    REQUIRE(fh2 == nullptr);

    status = close(fd);
    REQUIRE(status == -1);
  }
  posttest();
}

TEST_CASE("Freopen", "[process=" + std::to_string(info.comm_size) +
                         "]"
                         "[operation=single_freopen]"
                         "[repetition=1][file=1]") {
  pretest();
  SECTION("change different modes") {
    FILE* fhr = fopen(info.existing_file.c_str(), "r");
    REQUIRE(fhr != nullptr);

    FILE* fhw = freopen(info.existing_file.c_str(), "w", fhr);
    REQUIRE(fhw != nullptr);
    size_t write_size =
        fwrite(info.write_data.c_str(), sizeof(char), args.request_size, fhw);
    REQUIRE(write_size == args.request_size);

    FILE* fhwp = freopen(info.existing_file.c_str(), "w+", fhw);
    REQUIRE(fhwp != nullptr);
    write_size =
        fwrite(info.write_data.c_str(), sizeof(char), args.request_size, fhwp);
    REQUIRE(write_size == args.request_size);

    FILE* fha = freopen(info.existing_file.c_str(), "a", fhwp);
    REQUIRE(fha != nullptr);
    write_size =
        fwrite(info.write_data.c_str(), sizeof(char), args.request_size, fhwp);
    REQUIRE(write_size == args.request_size);

    FILE* fhap = freopen(info.existing_file.c_str(), "a+", fha);
    REQUIRE(fhap != nullptr);
    write_size =
        fwrite(info.write_data.c_str(), sizeof(char), args.request_size, fhap);
    REQUIRE(write_size == args.request_size);

    int status = fclose(fhap);
    REQUIRE(status == 0);
  }
  posttest();
}

TEST_CASE("fgetc", "[process=" + std::to_string(info.comm_size) +
                       "]"
                       "[operation=batched_fgetc]"
                       "[repetition=1][file=1]") {
  pretest();
  SECTION("iterate and get all characters") {
    FILE* fh = fopen(info.existing_file.c_str(), "r");
    REQUIRE(fh != nullptr);
    size_t total_chars = 0;
    int c = '0', v = '0';
    do {
      c = fgetc(fh);
      total_chars++;
      if (total_chars >= info.num_iterations) break;
    } while (c != EOF);
    REQUIRE(total_chars == info.num_iterations);
    int status = fclose(fh);
    REQUIRE(status == 0);
  }
  posttest();
}

TEST_CASE("getc", "[process=" + std::to_string(info.comm_size) +
                      "]"
                      "[operation=batched_getc]"
                      "[repetition=1][file=1]") {
  pretest();
  SECTION("iterate and get all characters") {
    FILE* fh = fopen(info.existing_file.c_str(), "r");
    REQUIRE(fh != nullptr);
    size_t total_chars = 0;
    int c = '0', v = '0';
    do {
      c = getc(fh);
      total_chars++;
      if (total_chars >= info.num_iterations) break;
    } while (c != EOF);
    REQUIRE(total_chars == info.num_iterations);
    int status = fclose(fh);
    REQUIRE(status == 0);
  }
  posttest();
}

TEST_CASE("fgets", "[process=" + std::to_string(info.comm_size) +
                       "]"
                       "[operation=single_fgets]"
                       "[repetition=1][file=1]") {
  pretest();
  SECTION("iterate and get all characters") {
    FILE* fh = fopen(info.existing_file.c_str(), "r");
    REQUIRE(fh != nullptr);
    auto ret_str = fgets(info.read_data.data(), args.request_size, fh);
    REQUIRE(ret_str != NULL);
    REQUIRE(info.read_data.size() == args.request_size);
    int status = fclose(fh);
    REQUIRE(status == 0);
  }
  posttest();
}

TEST_CASE("fputc", "[process=" + std::to_string(info.comm_size) +
                       "]"
                       "[operation=batched_fputc]"
                       "[repetition=1][file=1]") {
  pretest();
  SECTION("iterate and get all characters") {
    FILE* fh = fopen(info.new_file.c_str(), "w+");
    REQUIRE(fh != nullptr);
    size_t total_chars = info.num_iterations;
    char c = 'w';
    for (size_t i = 0; i < total_chars; ++i) {
      int ret_char = fputc(c, fh);
      REQUIRE(ret_char == 0);
    }
    int status = fclose(fh);
    REQUIRE(status == 0);
  }
  posttest();
}

TEST_CASE("putc", "[process=" + std::to_string(info.comm_size) +
                      "]"
                      "[operation=batched_putc]"
                      "[repetition=1][file=1]") {
  pretest();
  SECTION("iterate and get all characters") {
    FILE* fh = fopen(info.new_file.c_str(), "w+");
    REQUIRE(fh != nullptr);
    size_t total_chars = info.num_iterations;
    char c = 'w';
    for (size_t i = 0; i < total_chars; ++i) {
      int ret_char = putc(c, fh);
      REQUIRE(ret_char == 0);
    }
    int status = fclose(fh);
    REQUIRE(status == 0);
  }
  posttest();
}

TEST_CASE("fputs", "[process=" + std::to_string(info.comm_size) +
                       "]"
                       "[operation=single_fputs]"
                       "[repetition=1][file=1]") {
  pretest();
  SECTION("iterate and get all characters") {
    FILE* fh = fopen(info.existing_file.c_str(), "w+");
    REQUIRE(fh != nullptr);
    int status = fputs(info.write_data.c_str(), fh);
    REQUIRE(status != -1);
    status = fclose(fh);
    REQUIRE(status == 0);
  }
  posttest();
}

TEST_CASE("fseek", "[process=" + std::to_string(info.comm_size) +
                       "]"
                       "[operation=single_fseek]"
                       "[repetition=1][file=1]") {
  pretest();
  SECTION("test all seek modes") {
    FILE* fh = fopen(info.existing_file.c_str(), "r");
    REQUIRE(fh != nullptr);
    int status = fseek(fh, 0, SEEK_SET);
    REQUIRE(status == 0);
    size_t offset = ftell(fh);
    REQUIRE(offset == 0);

    status = fseek(fh, 0, SEEK_CUR);
    REQUIRE(status == 0);
    offset = ftell(fh);
    REQUIRE(offset == 0);

    status = fseek(fh, 0, SEEK_END);
    REQUIRE(status == 0);
    offset = ftell(fh);
    REQUIRE(offset == info.total_size);

    status = fseek(fh, 0, SEEK_CUR);
    REQUIRE(status == 0);
    offset = ftell(fh);
    REQUIRE(offset == info.total_size);

    status = fclose(fh);
    REQUIRE(status == 0);
  }
  posttest();
}

TEST_CASE("fseeko", "[process=" + std::to_string(info.comm_size) +
                        "]"
                        "[operation=single_fseeko]"
                        "[repetition=1][file=1]") {
  pretest();
  SECTION("test all seek modes") {
    FILE* fh = fopen(info.existing_file.c_str(), "r");
    REQUIRE(fh != nullptr);
    int status = fseeko(fh, 0, SEEK_SET);
    REQUIRE(status == 0);
    size_t offset = ftell(fh);
    REQUIRE(offset == 0);

    status = fseeko(fh, 0, SEEK_CUR);
    REQUIRE(status == 0);
    offset = ftell(fh);
    REQUIRE(offset == 0);

    status = fseeko(fh, 0, SEEK_END);
    REQUIRE(status == 0);
    offset = ftell(fh);
    REQUIRE(offset == info.total_size);

    status = fseeko(fh, 0, SEEK_CUR);
    REQUIRE(status == 0);
    offset = ftell(fh);
    REQUIRE(offset == info.total_size);

    status = fclose(fh);
    REQUIRE(status == 0);
  }
  posttest();
}

TEST_CASE("rewind", "[process=" + std::to_string(info.comm_size) +
                        "]"
                        "[operation=single_rewind]"
                        "[repetition=1][file=1]") {
  pretest();
  SECTION("test all seek modes") {
    FILE* fh = fopen(info.existing_file.c_str(), "r");
    REQUIRE(fh != nullptr);
    int status = fseeko(fh, 0, SEEK_SET);
    REQUIRE(status == 0);
    size_t offset = ftell(fh);
    REQUIRE(offset == 0);
    rewind(fh);
    offset = ftell(fh);
    REQUIRE(offset == 0);

    status = fseeko(fh, 0, SEEK_END);
    REQUIRE(status == 0);
    offset = ftell(fh);
    REQUIRE(offset == info.total_size);
    rewind(fh);
    offset = ftell(fh);
    REQUIRE(offset == 0);

    status = fclose(fh);
    REQUIRE(status == 0);
  }
  posttest();
}

TEST_CASE("fsetpos", "[process=" + std::to_string(info.comm_size) +
                         "]"
                         "[operation=single_fsetpos]"
                         "[repetition=1][file=1]") {
  pretest();
  SECTION("test all seek modes") {
    FILE* fh = fopen(info.existing_file.c_str(), "r");
    REQUIRE(fh != nullptr);
    fpos_t position;
    fgetpos(fh, &position);

    position.__pos = 0;
    int status = fsetpos(fh, &position);
    REQUIRE(status == 0);
    size_t offset = ftell(fh);
    REQUIRE(offset == 0);

    position.__pos = info.total_size;
    status = fsetpos(fh, &position);
    REQUIRE(status == 0);
    offset = ftell(fh);
    REQUIRE(offset == info.total_size);

    status = fclose(fh);
    REQUIRE(status == 0);
  }
  posttest();
}

TEST_CASE("fgetpos", "[process=" + std::to_string(info.comm_size) +
                         "]"
                         "[operation=single_fgetpos]"
                         "[repetition=1][file=1]") {
  pretest();
  SECTION("test all seek modes") {
    FILE* fh = fopen(info.existing_file.c_str(), "r");
    REQUIRE(fh != nullptr);
    fpos_t position;

    int status = fseek(fh, 0, SEEK_SET);
    REQUIRE(status == 0);
    status = fgetpos(fh, &position);
    REQUIRE(position.__pos == 0);

    status = fseek(fh, 0, SEEK_END);
    REQUIRE(status == 0);
    status = fgetpos(fh, &position);
    REQUIRE(position.__pos == (long int)info.total_size);

    status = fclose(fh);
    REQUIRE(status == 0);
  }
  posttest();
}
