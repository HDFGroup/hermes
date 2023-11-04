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

#include "stdio_adapter_test.h"

TEST_CASE("FFlush", "[process=" + std::to_string(TEST_INFO->comm_size_) +
                        "]"
                        "[operation=single_fflush]"
                        "[repetition=1][file=1]") {
  TEST_INFO->Pretest();
  SECTION("Flushing contents of file in different modes") {
    FILE* fd = fopen(TEST_INFO->existing_file_.hermes_.c_str(), "w");
    REQUIRE(fd != nullptr);
    int status = fflush(fd);
    REQUIRE(status == 0);
    status = fclose(fd);
    REQUIRE(status == 0);

    fd = fopen(TEST_INFO->existing_file_.hermes_.c_str(), "w+");
    REQUIRE(fd != nullptr);
    status = fflush(fd);
    REQUIRE(status == 0);
    status = fclose(fd);
    REQUIRE(status == 0);

    fd = fopen(TEST_INFO->existing_file_.hermes_.c_str(), "r+");
    REQUIRE(fd != nullptr);
    status = fflush(fd);
    REQUIRE(status == 0);
    status = fclose(fd);
    REQUIRE(status == 0);

    fd = fopen(TEST_INFO->existing_file_.hermes_.c_str(), "r");
    REQUIRE(fd != nullptr);
    status = fflush(fd);
    REQUIRE(status == 0);
    status = fclose(fd);
    REQUIRE(status == 0);

    fd = fopen(TEST_INFO->existing_file_.hermes_.c_str(), "a");
    REQUIRE(fd != nullptr);
    status = fflush(fd);
    REQUIRE(status == 0);
    status = fclose(fd);
    REQUIRE(status == 0);

    fd = fopen(TEST_INFO->existing_file_.hermes_.c_str(), "a+");
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
  TEST_INFO->Posttest(false);
}

TEST_CASE("Fdopen", "[process=" + std::to_string(TEST_INFO->comm_size_) +
                        "]"
                        "[operation=single_fdopen]"
                        "[repetition=1][file=1]") {
  TEST_INFO->Pretest();
  SECTION("Associate a FILE ptr with read mode") {
    int fd = open(TEST_INFO->existing_file_.hermes_.c_str(), O_RDWR);
    REQUIRE(fd != -1);

    FILE* fh = fdopen(fd, "r");
    REQUIRE(fh != nullptr);
    size_t read_size =
        fread(TEST_INFO->read_data_.data(), sizeof(char), TEST_INFO->request_size_, fh);
    REQUIRE(read_size == TEST_INFO->request_size_);
    int status = fclose(fh);
    REQUIRE(status == 0);

    status = fcntl(fd, F_GETFD);
    REQUIRE(fd != -1);

    status = close(fd);
    REQUIRE(status == -1);
  }
  SECTION("Associate a FILE ptr with write mode") {
    int fd = open(TEST_INFO->existing_file_.hermes_.c_str(), O_RDWR);
    REQUIRE(fd != -1);

    FILE* fh = fdopen(fd, "w");
    REQUIRE(fh != nullptr);
    size_t write_size =
        fwrite(TEST_INFO->write_data_.data(), sizeof(char), TEST_INFO->request_size_, fh);
    REQUIRE(write_size == TEST_INFO->request_size_);
    int status = fclose(fh);
    REQUIRE(status == 0);

    status = close(fd);
    REQUIRE(status == -1);
  }
  SECTION("Associate a FILE ptr with read plus mode") {
    int fd = open(TEST_INFO->existing_file_.hermes_.c_str(), O_RDWR);
    REQUIRE(fd != -1);

    FILE* fh = fdopen(fd, "r");
    REQUIRE(fh != nullptr);
    size_t read_size =
        fread(TEST_INFO->read_data_.data(), sizeof(char), TEST_INFO->request_size_, fh);
    REQUIRE(read_size == TEST_INFO->request_size_);
    int status = fclose(fh);
    REQUIRE(status == 0);

    status = fcntl(fd, F_GETFD);
    REQUIRE(fd != -1);

    status = close(fd);
    REQUIRE(status == -1);
  }
  SECTION("Associate a FILE ptr with write plus mode") {
    int fd = open(TEST_INFO->existing_file_.hermes_.c_str(), O_RDWR);
    REQUIRE(fd != -1);

    FILE* fh = fdopen(fd, "w+");
    REQUIRE(fh != nullptr);
    size_t write_size =
        fwrite(TEST_INFO->write_data_.data(), sizeof(char), TEST_INFO->request_size_, fh);
    REQUIRE(write_size == TEST_INFO->request_size_);
    int status = fclose(fh);
    REQUIRE(status == 0);

    status = fcntl(fd, F_GETFD);
    REQUIRE(fd != -1);

    status = close(fd);
    REQUIRE(status == -1);
  }
  SECTION("Associate a FILE ptr with append mode") {
    int fd = open(TEST_INFO->existing_file_.hermes_.c_str(), O_RDWR | O_APPEND);
    REQUIRE(fd != -1);

    FILE* fh = fdopen(fd, "a");
    REQUIRE(fh != nullptr);
    size_t write_size =
        fwrite(TEST_INFO->write_data_.data(), sizeof(char), TEST_INFO->request_size_, fh);
    REQUIRE(write_size == TEST_INFO->request_size_);
    int status = fclose(fh);
    REQUIRE(status == 0);

    status = fcntl(fd, F_GETFD);
    REQUIRE(fd != -1);

    status = close(fd);
    REQUIRE(status == -1);
  }
  SECTION("Associate a FILE ptr with append plus mode") {
    int fd = open(TEST_INFO->existing_file_.hermes_.c_str(), O_RDWR | O_APPEND);
    REQUIRE(fd != -1);

    FILE* fh = fdopen(fd, "a+");
    REQUIRE(fh != nullptr);
    size_t write_size =
        fwrite(TEST_INFO->write_data_.data(), sizeof(char), TEST_INFO->request_size_, fh);
    REQUIRE(write_size == TEST_INFO->request_size_);
    int status = fclose(fh);
    REQUIRE(status == 0);

    status = fcntl(fd, F_GETFD);
    REQUIRE(fd != -1);

    status = close(fd);
    REQUIRE(status == -1);
  }
  SECTION("Associate a FILE ptr with read mode twice") {
    int fd = open(TEST_INFO->existing_file_.hermes_.c_str(), O_RDWR);
    REQUIRE(fd != -1);

    FILE* fh = fdopen(fd, "r");
    REQUIRE(fh != nullptr);
    size_t read_size =
        fread(TEST_INFO->read_data_.data(), sizeof(char), TEST_INFO->request_size_, fh);
    REQUIRE(read_size == TEST_INFO->request_size_);

    int status = fclose(fh);
    REQUIRE(status == 0);

    status = close(fd);
    REQUIRE(status == -1);
  }
  SECTION("Associate a FILE ptr with read mode twice after one closes") {
    int fd = open(TEST_INFO->existing_file_.hermes_.c_str(), O_RDWR);
    REQUIRE(fd != -1);

    FILE* fh = fdopen(fd, "r");
    REQUIRE(fh != nullptr);
    size_t read_size =
        fread(TEST_INFO->read_data_.data(), sizeof(char), TEST_INFO->request_size_, fh);
    REQUIRE(read_size == TEST_INFO->request_size_);

    int status = fcntl(fd, F_GETFD);
    REQUIRE(fd != -1);

    status = fclose(fh);
    REQUIRE(status == 0);

    FILE* fh2 = fdopen(fd, "r");
    REQUIRE(fh2 == nullptr);

    status = close(fd);
    REQUIRE(status == -1);
  }
  TEST_INFO->Posttest(false);
}

TEST_CASE("Freopen", "[process=" + std::to_string(TEST_INFO->comm_size_) +
                         "]"
                         "[operation=single_freopen]"
                         "[repetition=1][file=1]") {
  TEST_INFO->Pretest();
  SECTION("change different modes") {
    FILE* fhr = fopen(TEST_INFO->existing_file_.hermes_.c_str(), "r");
    REQUIRE(fhr != nullptr);

    FILE* fhw = freopen(TEST_INFO->existing_file_.hermes_.c_str(), "w", fhr);
    REQUIRE(fhw != nullptr);
    size_t write_size =
        fwrite(TEST_INFO->write_data_.data(), sizeof(char), TEST_INFO->request_size_, fhw);
    REQUIRE(write_size == TEST_INFO->request_size_);

    FILE* fhwp = freopen(TEST_INFO->existing_file_.hermes_.c_str(), "w+", fhw);
    REQUIRE(fhwp != nullptr);
    write_size =
        fwrite(TEST_INFO->write_data_.data(), sizeof(char), TEST_INFO->request_size_, fhwp);
    REQUIRE(write_size == TEST_INFO->request_size_);

    FILE* fha = freopen(TEST_INFO->existing_file_.hermes_.c_str(), "a", fhwp);
    REQUIRE(fha != nullptr);
    write_size =
        fwrite(TEST_INFO->write_data_.data(), sizeof(char), TEST_INFO->request_size_, fhwp);
    REQUIRE(write_size == TEST_INFO->request_size_);

    FILE* fhap = freopen(TEST_INFO->existing_file_.hermes_.c_str(), "a+", fha);
    REQUIRE(fhap != nullptr);
    write_size =
        fwrite(TEST_INFO->write_data_.data(), sizeof(char), TEST_INFO->request_size_, fhap);
    REQUIRE(write_size == TEST_INFO->request_size_);

    int status = fclose(fhap);
    REQUIRE(status == 0);
  }
  TEST_INFO->Posttest(false);
}

TEST_CASE("fgetc", "[process=" + std::to_string(TEST_INFO->comm_size_) +
                       "]"
                       "[operation=batched_fgetc]"
                       "[repetition=" +
                       std::to_string(TEST_INFO->num_iterations_) + "][file=1]") {
  TEST_INFO->Pretest();
  SECTION("iterate and get all characters") {
    FILE* fh = fopen(TEST_INFO->existing_file_.hermes_.c_str(), "r");
    REQUIRE(fh != nullptr);
    size_t total_chars = 0;
    int c = '0';
    do {
      c = fgetc(fh);
      total_chars++;
      if (total_chars >= TEST_INFO->num_iterations_) break;
    } while (c != EOF);
    REQUIRE(total_chars == TEST_INFO->num_iterations_);
    int status = fclose(fh);
    REQUIRE(status == 0);
  }
  TEST_INFO->Posttest(false);
}

TEST_CASE("getc", "[process=" + std::to_string(TEST_INFO->comm_size_) +
                      "]"
                      "[operation=batched_getc]"
                      "[repetition=" +
                      std::to_string(TEST_INFO->num_iterations_) + "][file=1]") {
  TEST_INFO->Pretest();
  SECTION("iterate and get all characters") {
    FILE* fh = fopen(TEST_INFO->existing_file_.hermes_.c_str(), "r");
    REQUIRE(fh != nullptr);
    size_t total_chars = 0;
    int c = '0';
    do {
      c = getc(fh);
      total_chars++;
      if (total_chars >= TEST_INFO->num_iterations_) break;
    } while (c != EOF);
    REQUIRE(total_chars == TEST_INFO->num_iterations_);
    int status = fclose(fh);
    REQUIRE(status == 0);
  }
  TEST_INFO->Posttest(false);
}

TEST_CASE("getw", "[process=" + std::to_string(TEST_INFO->comm_size_) +
                      "]"
                      "[operation=batched_getw]"
                      "[repetition=" +
                      std::to_string(TEST_INFO->num_iterations_) + "][file=1]") {
  TEST_INFO->Pretest();
  SECTION("iterate and get all characters") {
    FILE* fh = fopen(TEST_INFO->existing_file_.hermes_.c_str(), "r");
    REQUIRE(fh != nullptr);
    size_t total_chars = 0;
    int c = '0';
    do {
      c = getw(fh);
      total_chars++;
      if (total_chars >= TEST_INFO->num_iterations_) break;
    } while (c != EOF);
    REQUIRE(total_chars == TEST_INFO->num_iterations_);
    int status = fclose(fh);
    REQUIRE(status == 0);
  }
  TEST_INFO->Posttest(false);
}

TEST_CASE("fgets", "[process=" + std::to_string(TEST_INFO->comm_size_) +
                       "]"
                       "[operation=single_fgets]"
                       "[repetition=1][file=1]") {
  TEST_INFO->Pretest();
  SECTION("iterate and get all characters") {
    FILE* fh = fopen(TEST_INFO->existing_file_.hermes_.c_str(), "r");
    REQUIRE(fh != nullptr);
    auto ret_str = fgets(TEST_INFO->read_data_.data(), TEST_INFO->request_size_, fh);
    REQUIRE(ret_str != NULL);
    REQUIRE(strlen(ret_str) == TEST_INFO->request_size_ - 1);
    int status = fclose(fh);
    REQUIRE(status == 0);
  }
  TEST_INFO->Posttest(false);
}

TEST_CASE("fputc", "[process=" + std::to_string(TEST_INFO->comm_size_) +
                       "]"
                       "[operation=batched_fputc]"
                       "[repetition=" +
                       std::to_string(TEST_INFO->num_iterations_) + "][file=1]") {
  TEST_INFO->Pretest();
  SECTION("iterate and get all characters") {
    FILE* fh = fopen(TEST_INFO->new_file_.hermes_.c_str(), "w+");
    REQUIRE(fh != nullptr);
    size_t total_chars = TEST_INFO->num_iterations_;
    char c = 'w';
    for (size_t i = 0; i < total_chars; ++i) {
      int ret_char = fputc(c, fh);
      REQUIRE(ret_char == c);
    }
    int status = fclose(fh);
    REQUIRE(status == 0);
  }
  TEST_INFO->Posttest(false);
}

TEST_CASE("putc", "[process=" + std::to_string(TEST_INFO->comm_size_) +
                      "]"
                      "[operation=batched_putc]"
                      "[repetition=" +
                      std::to_string(TEST_INFO->num_iterations_) + "][file=1]") {
  TEST_INFO->Pretest();
  SECTION("iterate and get all characters") {
    FILE* fh = fopen(TEST_INFO->new_file_.hermes_.c_str(), "w+");
    REQUIRE(fh != nullptr);
    size_t total_chars = TEST_INFO->num_iterations_;
    char c = 'w';
    for (size_t i = 0; i < total_chars; ++i) {
      int ret_char = putc(c, fh);
      REQUIRE(ret_char == c);
    }
    int status = fclose(fh);
    REQUIRE(status == 0);
  }
  TEST_INFO->Posttest(false);
}
TEST_CASE("putw", "[process=" + std::to_string(TEST_INFO->comm_size_) +
                      "]"
                      "[operation=batched_putw]"
                      "[repetition=" +
                      std::to_string(TEST_INFO->num_iterations_) + "][file=1]") {
  TEST_INFO->Pretest();
  SECTION("iterate and get all characters") {
    FILE* fh = fopen(TEST_INFO->new_file_.hermes_.c_str(), "w+");
    REQUIRE(fh != nullptr);
    size_t total_chars = TEST_INFO->num_iterations_;
    int c = 'w';
    for (size_t i = 0; i < total_chars; ++i) {
      int ret = putw(c, fh);
      REQUIRE(ret == 0);
    }
    int status = fclose(fh);
    REQUIRE(status == 0);
  }
  TEST_INFO->Posttest(false);
}

TEST_CASE("fputs", "[process=" + std::to_string(TEST_INFO->comm_size_) +
                       "]"
                       "[operation=single_fputs]"
                       "[repetition=1][file=1]") {
  TEST_INFO->Pretest();
  SECTION("iterate and get all characters") {
    FILE* fh = fopen(TEST_INFO->existing_file_.hermes_.c_str(), "w+");
    REQUIRE(fh != nullptr);
    int status = fputs(TEST_INFO->write_data_.data(), fh);
    REQUIRE(status != -1);
    status = fclose(fh);
    REQUIRE(status == 0);
  }
  TEST_INFO->Posttest(false);
}

TEST_CASE("fseek", "[process=" + std::to_string(TEST_INFO->comm_size_) +
                       "]"
                       "[operation=single_fseek]"
                       "[repetition=1][file=1]") {
  TEST_INFO->Pretest();
  SECTION("test all seek modes") {
    FILE* fh = fopen(TEST_INFO->existing_file_.hermes_.c_str(), "r");
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
    REQUIRE(offset == TEST_INFO->total_size_);

    status = fseek(fh, 0, SEEK_CUR);
    REQUIRE(status == 0);
    offset = ftell(fh);
    REQUIRE(offset == TEST_INFO->total_size_);

    status = fclose(fh);
    REQUIRE(status == 0);
  }
  TEST_INFO->Posttest(false);
}

TEST_CASE("fseeko", "[process=" + std::to_string(TEST_INFO->comm_size_) +
                        "]"
                        "[operation=single_fseeko]"
                        "[repetition=1][file=1]") {
  TEST_INFO->Pretest();
  SECTION("test all seek modes") {
    FILE* fh = fopen(TEST_INFO->existing_file_.hermes_.c_str(), "r");
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
    REQUIRE(offset == TEST_INFO->total_size_);

    status = fseeko(fh, 0, SEEK_CUR);
    REQUIRE(status == 0);
    offset = ftell(fh);
    REQUIRE(offset == TEST_INFO->total_size_);

    status = fclose(fh);
    REQUIRE(status == 0);
  }
  TEST_INFO->Posttest(false);
}

TEST_CASE("fseeko64", "[process=" + std::to_string(TEST_INFO->comm_size_) +
                          "]"
                          "[operation=single_fseeko64]"
                          "[repetition=1][file=1]") {
  TEST_INFO->Pretest();
  SECTION("test all seek modes") {
    FILE* fh = fopen(TEST_INFO->existing_file_.hermes_.c_str(), "r");
    REQUIRE(fh != nullptr);
    int status = fseeko64(fh, 0, SEEK_SET);
    REQUIRE(status == 0);
    size_t offset = ftell(fh);
    REQUIRE(offset == 0);

    status = fseeko64(fh, 0, SEEK_CUR);
    REQUIRE(status == 0);
    offset = ftell(fh);
    REQUIRE(offset == 0);

    status = fseeko64(fh, 0, SEEK_END);
    REQUIRE(status == 0);
    offset = ftell(fh);
    REQUIRE(offset == TEST_INFO->total_size_);

    status = fseeko64(fh, 0, SEEK_CUR);
    REQUIRE(status == 0);
    offset = ftell(fh);
    REQUIRE(offset == TEST_INFO->total_size_);

    status = fclose(fh);
    REQUIRE(status == 0);
  }
  TEST_INFO->Posttest(false);
}

TEST_CASE("rewind", "[process=" + std::to_string(TEST_INFO->comm_size_) +
                        "]"
                        "[operation=single_rewind]"
                        "[repetition=1][file=1]") {
  TEST_INFO->Pretest();
  SECTION("test all seek modes") {
    FILE* fh = fopen(TEST_INFO->existing_file_.hermes_.c_str(), "r");
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
    REQUIRE(offset == TEST_INFO->total_size_);
    rewind(fh);
    offset = ftell(fh);
    REQUIRE(offset == 0);

    status = fclose(fh);
    REQUIRE(status == 0);
  }
  TEST_INFO->Posttest(false);
}

TEST_CASE("fsetpos", "[process=" + std::to_string(TEST_INFO->comm_size_) +
                         "]"
                         "[operation=single_fsetpos]"
                         "[repetition=1][file=1]") {
  TEST_INFO->Pretest();
  SECTION("test all seek modes") {
    FILE* fh = fopen(TEST_INFO->existing_file_.hermes_.c_str(), "r");
    REQUIRE(fh != nullptr);
    fpos_t position;
    fgetpos(fh, &position);

    position.__pos = 0;
    int status = fsetpos(fh, &position);
    REQUIRE(status == 0);
    size_t offset = ftell(fh);
    REQUIRE(offset == 0);

    position.__pos = TEST_INFO->total_size_;
    status = fsetpos(fh, &position);
    REQUIRE(status == 0);
    offset = ftell(fh);
    REQUIRE(offset == TEST_INFO->total_size_);

    status = fclose(fh);
    REQUIRE(status == 0);
  }
  TEST_INFO->Posttest(false);
}

TEST_CASE("fsetpos64", "[process=" + std::to_string(TEST_INFO->comm_size_) +
                           "]"
                           "[operation=single_fsetpos64]"
                           "[repetition=1][file=1]") {
  TEST_INFO->Pretest();
  SECTION("test all seek modes") {
    FILE* fh = fopen(TEST_INFO->existing_file_.hermes_.c_str(), "r");
    REQUIRE(fh != nullptr);
    fpos64_t position;
    fgetpos64(fh, &position);

    position.__pos = 0;
    int status = fsetpos64(fh, &position);
    REQUIRE(status == 0);
    size_t offset = ftell(fh);
    REQUIRE(offset == 0);

    position.__pos = TEST_INFO->total_size_;
    status = fsetpos64(fh, &position);
    REQUIRE(status == 0);
    offset = ftell(fh);
    REQUIRE(offset == TEST_INFO->total_size_);

    status = fclose(fh);
    REQUIRE(status == 0);
  }
  TEST_INFO->Posttest(false);
}

TEST_CASE("fgetpos", "[process=" + std::to_string(TEST_INFO->comm_size_) +
                         "]"
                         "[operation=single_fgetpos]"
                         "[repetition=1][file=1]") {
  TEST_INFO->Pretest();
  SECTION("test all seek modes") {
    FILE* fh = fopen(TEST_INFO->existing_file_.hermes_.c_str(), "r");
    REQUIRE(fh != nullptr);
    fpos_t position;

    int status = fseek(fh, 0, SEEK_SET);
    REQUIRE(status == 0);
    status = fgetpos(fh, &position);
    REQUIRE(position.__pos == 0);

    status = fseek(fh, 0, SEEK_END);
    REQUIRE(status == 0);
    status = fgetpos(fh, &position);
    REQUIRE(position.__pos == (long int)TEST_INFO->total_size_);

    status = fclose(fh);
    REQUIRE(status == 0);
  }
  TEST_INFO->Posttest(false);
}

TEST_CASE("fgetpos64", "[process=" + std::to_string(TEST_INFO->comm_size_) +
                           "]"
                           "[operation=single_fgetpos64]"
                           "[repetition=1][file=1]") {
  TEST_INFO->Pretest();
  SECTION("test all seek modes") {
    FILE* fh = fopen(TEST_INFO->existing_file_.hermes_.c_str(), "r");
    REQUIRE(fh != nullptr);
    fpos64_t position;

    int status = fseek(fh, 0, SEEK_SET);
    REQUIRE(status == 0);
    status = fgetpos64(fh, &position);
    REQUIRE(position.__pos == 0);

    status = fseek(fh, 0, SEEK_END);
    REQUIRE(status == 0);
    status = fgetpos64(fh, &position);
    REQUIRE(position.__pos == (long int)TEST_INFO->total_size_);

    status = fclose(fh);
    REQUIRE(status == 0);
  }
  TEST_INFO->Posttest(false);
}

TEST_CASE("Open64", "[process=" + std::to_string(TEST_INFO->comm_size_) +
                        "]"
                        "[operation=single_open]"
                        "[repetition=1][file=1]") {
  TEST_INFO->Pretest();
  SECTION("open non-existant file") {
    FILE* fh = fopen64(TEST_INFO->new_file_.hermes_.c_str(), "r");
    REQUIRE(fh == nullptr);
    fh = fopen64(TEST_INFO->new_file_.hermes_.c_str(), "r+");
    REQUIRE(fh == nullptr);
  }

  SECTION("truncate existing file and write-only") {
    FILE* fh = fopen64(TEST_INFO->existing_file_.hermes_.c_str(), "w");
    REQUIRE(fh != nullptr);
    int status = fclose(fh);
    REQUIRE(status == 0);
  }
  SECTION("truncate existing file and read/write") {
    FILE* fh = fopen64(TEST_INFO->existing_file_.hermes_.c_str(), "w+");
    REQUIRE(fh != nullptr);
    int status = fclose(fh);
    REQUIRE(status == 0);
  }

  SECTION("open existing file") {
    FILE* fh = fopen64(TEST_INFO->existing_file_.hermes_.c_str(), "r+");
    REQUIRE(fh != nullptr);
    int status = fclose(fh);
    REQUIRE(status == 0);
    fh = fopen64(TEST_INFO->existing_file_.hermes_.c_str(), "r");
    REQUIRE(fh != nullptr);
    status = fclose(fh);
    REQUIRE(status == 0);
  }

  SECTION("append write existing file") {
    FILE* fh = fopen64(TEST_INFO->existing_file_.hermes_.c_str(), "a");
    REQUIRE(fh != nullptr);
    int status = fclose(fh);
    REQUIRE(status == 0);
  }

  SECTION("append write and read existing file") {
    FILE* fh = fopen64(TEST_INFO->existing_file_.hermes_.c_str(), "a+");
    REQUIRE(fh != nullptr);
    int status = fclose(fh);
    REQUIRE(status == 0);
  }
  TEST_INFO->Posttest(false);
}

TEST_CASE("Freopen64", "[process=" + std::to_string(TEST_INFO->comm_size_) +
                           "]"
                           "[operation=single_freopen]"
                           "[repetition=1][file=1]") {
  TEST_INFO->Pretest();
  SECTION("change different modes") {
    FILE* fhr = fopen(TEST_INFO->existing_file_.hermes_.c_str(), "r");
    REQUIRE(fhr != nullptr);

    FILE* fhw = freopen64(TEST_INFO->existing_file_.hermes_.c_str(), "w", fhr);
    REQUIRE(fhw != nullptr);
    size_t write_size =
        fwrite(TEST_INFO->write_data_.data(), sizeof(char), TEST_INFO->request_size_, fhw);
    REQUIRE(write_size == TEST_INFO->request_size_);

    FILE* fhwp = freopen64(TEST_INFO->existing_file_.hermes_.c_str(), "w+", fhw);
    REQUIRE(fhwp != nullptr);
    write_size =
        fwrite(TEST_INFO->write_data_.data(), sizeof(char), TEST_INFO->request_size_, fhwp);
    REQUIRE(write_size == TEST_INFO->request_size_);

    FILE* fha = freopen64(TEST_INFO->existing_file_.hermes_.c_str(), "a", fhwp);
    REQUIRE(fha != nullptr);
    write_size =
        fwrite(TEST_INFO->write_data_.data(), sizeof(char), TEST_INFO->request_size_, fhwp);
    REQUIRE(write_size == TEST_INFO->request_size_);

    FILE* fhap = freopen64(TEST_INFO->existing_file_.hermes_.c_str(), "a+", fha);
    REQUIRE(fhap != nullptr);
    write_size =
        fwrite(TEST_INFO->write_data_.data(), sizeof(char), TEST_INFO->request_size_, fhap);
    REQUIRE(write_size == TEST_INFO->request_size_);

    int status = fclose(fhap);
    REQUIRE(status == 0);
  }
  TEST_INFO->Posttest(false);
}

TEST_CASE("MultiOpen", "[process=" + std::to_string(TEST_INFO->comm_size_) +
                           "]"
                           "[operation=multi_open]"
                           "[repetition=1][file=1]") {
  TEST_INFO->Pretest();
  SECTION("Open same file twice and then close both fps") {
    FILE* fh1 = fopen(TEST_INFO->existing_file_.hermes_.c_str(), "r");
    REQUIRE(fh1 != nullptr);
    FILE* fh2 = fopen(TEST_INFO->existing_file_.hermes_.c_str(), "r");
    REQUIRE(fh2 != nullptr);
    int status = fclose(fh1);
    REQUIRE(status == 0);
    status = fclose(fh2);
    REQUIRE(status == 0);
  }
  TEST_INFO->Posttest(false);
}
