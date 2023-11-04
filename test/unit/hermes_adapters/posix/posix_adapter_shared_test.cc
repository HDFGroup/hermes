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

#include "posix_adapter_test.h"

TEST_CASE("SharedFile", "[process=" + std::to_string(TEST_INFO->comm_size_) +
                            "]"
                            "[operation=batched_write]"
                            "[request_size=range-small][repetition=" +
                            std::to_string(TEST_INFO->num_iterations_) +
                            "]"
                            "[mode=shared]"
                            "[pattern=sequential][file=1]") {
  TEST_INFO->Pretest();
  REQUIRE(TEST_INFO->comm_size_ == 2);
  SECTION("producer-consumer") {
    bool producer = TEST_INFO->rank_ % 2 == 0;
    struct flock lock;
    lock.l_type = F_WRLCK;
    lock.l_whence = SEEK_SET;
    lock.l_start = 0;
    lock.l_len = 0;
    lock.l_pid = getpid();
    if (producer) {
      int fd = open(TEST_INFO->shared_new_file_.hermes_.c_str(), O_RDWR | O_CREAT, 0666);
      REQUIRE(fd != -1);
      MPI_Barrier(MPI_COMM_WORLD);
      int status = -1;
      for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
        status = fcntl(fd, F_SETLKW, &lock);
        REQUIRE(status != -1);
        size_t write_bytes =
            write(fd, TEST_INFO->write_data_.data(), TEST_INFO->request_size_);
        REQUIRE(write_bytes == TEST_INFO->request_size_);
        lock.l_type = F_UNLCK;
        status = fcntl(fd, F_SETLK, &lock);
        REQUIRE(status != -1);
      }
      status = close(fd);
      REQUIRE(status != -1);
    } else {
      MPI_Barrier(MPI_COMM_WORLD);
      int fd = open(TEST_INFO->shared_new_file_.hermes_.c_str(), O_RDONLY);
      REQUIRE(fd != -1);
      int status = -1;
      size_t bytes_read = 0;
      for (size_t i = 0; i < TEST_INFO->num_iterations_; ++i) {
        lock.l_type = F_RDLCK;
        status = fcntl(fd, F_SETLKW, &lock);
        REQUIRE(status != -1);
        size_t file_size = lseek(fd, 0, SEEK_END);
        size_t cur_offset = lseek(fd, bytes_read, SEEK_SET);
        REQUIRE(cur_offset == bytes_read);
        if (file_size > bytes_read) {
          size_t read_size = TEST_INFO->request_size_ < file_size - bytes_read
                                 ? TEST_INFO->request_size_
                                 : file_size - bytes_read;
          size_t read_bytes = read(fd, TEST_INFO->read_data_.data(), read_size);
          REQUIRE(read_bytes == read_size);
          bytes_read += read_bytes;
        }
        lock.l_type = F_UNLCK;
        status = fcntl(fd, F_SETLK, &lock);
        REQUIRE(status != -1);
      }
      status = close(fd);
      REQUIRE(status != -1);
    }
  }
  TEST_INFO->Posttest();
}
