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

#ifndef HERMES_TEST_UNIT_HERMES_ADAPTERS_POSIX_POSIX_ADAPTER_BASE_TEST_H_
#define HERMES_TEST_UNIT_HERMES_ADAPTERS_POSIX_POSIX_ADAPTER_BASE_TEST_H_

#include "binary_file_tests.h"

namespace hermes::adapter::test {
template<bool WITH_MPI>
class MpiioTest : public BinaryFileTests {
 public:
  FileInfo new_file_;
  FileInfo existing_file_;
  FileInfo shared_new_file_;
  FileInfo shared_existing_file_;
  FileInfo tmp_file_;
  unsigned int offset_seed_ = 1;
  unsigned int rs_seed_ = 1;
  unsigned int temporal_interval_seed_ = 5;
  size_t stride_size_ = 1024;
  unsigned int temporal_interval_ms_ = 1;
  size_t small_min_ = 1;
  size_t small_max_ = 4 * 1024;
  size_t medium_min_ = 4 * 1024 + 1;
  size_t medium_max_ = 256 * 1024;
  size_t large_min_ = 256 * 1024 + 1;
  size_t large_max_ = 3 * 1024 * 1024;

  MPI_File fh_orig_;
  MPI_File fh_cmp_;
  int status_orig_;
  int size_read_orig_;
  int size_written_orig_;

 public:
  void RegisterFiles() override {
    RegisterPath("new", 0, new_file_);
    RegisterPath("ext", TEST_DO_CREATE, existing_file_);
    if constexpr(WITH_MPI) {
      RegisterPath("shared_new", TEST_FILE_SHARED, shared_new_file_);
      RegisterPath("shared_ext", TEST_DO_CREATE | TEST_FILE_SHARED,
                   shared_existing_file_);
    }
    RegisterTmpPath(tmp_file_);
  }

  void test_read_data(size_t size_read, size_t count, int type_size,
                      char* read_data, char* ptr) {
    if (size_read > 0) {
      size_t unmatching_chars = 0;
      for (size_t i = 0; i < count * type_size; ++i) {
        if (read_data[i] != ptr[i]) {
          unmatching_chars = i;
          break;
        }
      }
      REQUIRE(unmatching_chars == 0);
    }
  }

  void test_open(FileInfo &info, int mode, MPI_Comm comm) {
    status_orig_ = MPI_File_open(comm, info.hermes_.c_str(),
                                 mode, MPI_INFO_NULL, &fh_orig_);
    auto status_cmp =
        MPI_File_open(comm, info.cmp_.c_str(), mode, MPI_INFO_NULL, &fh_cmp_);
    bool is_same = (status_orig_ != MPI_SUCCESS && status_cmp != MPI_SUCCESS) ||
        (status_orig_ == MPI_SUCCESS && status_cmp == MPI_SUCCESS);
    REQUIRE(is_same);
  }
  void test_close() {
    status_orig_ = MPI_File_close(&fh_orig_);
    int status = MPI_File_close(&fh_cmp_);
    REQUIRE(status == status_orig_);
  }

  void test_preallocate(MPI_Offset size) {
    status_orig_ = MPI_File_preallocate(fh_orig_, size);
    int status = MPI_File_preallocate(fh_cmp_, size);
    REQUIRE(status == status_orig_);
  }

  void test_write(const void* ptr, size_t count, MPI_Datatype datatype) {
    MPI_Status stat_orig, stat_cmp;
    auto ret_orig = MPI_File_write(fh_orig_, ptr, count, datatype, &stat_orig);
    int size_written;
    auto ret_cmp = MPI_File_write(fh_cmp_, ptr, count, datatype, &stat_cmp);
    REQUIRE(ret_orig == ret_cmp);
    MPI_Get_count(&stat_orig, datatype, &size_written_orig_);
    MPI_Get_count(&stat_cmp, datatype, &size_written);
    REQUIRE(size_written == size_written_orig_);
  }

  void test_iwrite(const void* ptr, size_t count, MPI_Datatype datatype) {
    MPI_Status stat[2];
    MPI_Request request[2];
    auto ret_orig = MPI_File_iwrite(fh_orig_, ptr, count,
                                    datatype, &request[0]);
    int size_written;
    auto ret_cmp = MPI_File_iwrite(fh_cmp_, ptr, count,
                                   datatype, &request[1]);
    REQUIRE(ret_orig == ret_cmp);
    MPI_Waitall(2, request, stat);
    MPI_Get_count(&stat[0], datatype, &size_written_orig_);
    MPI_Get_count(&stat[1], datatype, &size_written);
    REQUIRE(size_written == size_written_orig_);
  }

  void test_write_shared(const void* ptr, size_t count,
                         MPI_Datatype datatype) {
    MPI_Status stat_orig, stat_cmp;
    auto ret_orig =
        MPI_File_write_shared(fh_orig_, ptr, count, datatype, &stat_orig);
    int size_written;
    auto ret_cmp = MPI_File_write_shared(fh_cmp_, ptr, count,
                                         datatype, &stat_cmp);
    REQUIRE(ret_orig == ret_cmp);
    MPI_Get_count(&stat_orig, datatype, &size_written_orig_);
    MPI_Get_count(&stat_cmp, datatype, &size_written);
    REQUIRE(size_written == size_written_orig_);
  }

  void test_iwrite_shared(const void* ptr, size_t count,
                          MPI_Datatype datatype) {
    MPI_Status stat[2];
    MPI_Request request[2];
    auto ret_orig =
        MPI_File_iwrite_shared(fh_orig_, ptr, count, datatype, &request[0]);
    int size_written;
    auto ret_cmp =
        MPI_File_iwrite_shared(fh_cmp_, ptr, count, datatype, &request[1]);
    REQUIRE(ret_orig == ret_cmp);
    MPI_Waitall(2, request, stat);
    MPI_Get_count(&stat[0], datatype, &size_written_orig_);
    MPI_Get_count(&stat[1], datatype, &size_written);
    REQUIRE(size_written == size_written_orig_);
  }

  void test_write_all(const void* ptr, size_t count, MPI_Datatype datatype) {
    MPI_Status stat_orig, stat_cmp;
    auto ret_orig = MPI_File_write_all(fh_orig_, ptr, count,
                                       datatype, &stat_orig);
    int size_written;
    auto ret_cmp = MPI_File_write_all(fh_cmp_, ptr, count,
                                      datatype, &stat_cmp);
    REQUIRE(ret_orig == ret_cmp);
    MPI_Get_count(&stat_orig, datatype, &size_written_orig_);
    MPI_Get_count(&stat_cmp, datatype, &size_written);
    REQUIRE(size_written == size_written_orig_);
  }

  void test_iwrite_all(const void* ptr, size_t count, MPI_Datatype datatype) {
    MPI_Status stat[2];
    MPI_Request request[2];
    auto ret_orig =
        MPI_File_iwrite_all(fh_orig_, ptr, count, datatype, &request[0]);
    int size_written;
    auto ret_cmp = MPI_File_iwrite_all(fh_cmp_, ptr, count,
                                       datatype, &request[1]);
    REQUIRE(ret_orig == ret_cmp);
    MPI_Waitall(2, request, stat);
    MPI_Get_count(&stat[0], datatype, &size_written_orig_);
    MPI_Get_count(&stat[1], datatype, &size_written);
    REQUIRE(size_written == size_written_orig_);
  }

  void test_write_at(const void* ptr, size_t count, MPI_Datatype datatype,
                     MPI_Offset offset) {
    MPI_Status stat_orig, stat_cmp;
    auto ret_orig =
        MPI_File_write_at(fh_orig_, offset, ptr, count, datatype, &stat_orig);
    int size_written;
    auto ret_cmp =
        MPI_File_write_at(fh_cmp_, offset, ptr, count, datatype, &stat_cmp);
    REQUIRE(ret_orig == ret_cmp);
    MPI_Get_count(&stat_orig, datatype, &size_written_orig_);
    MPI_Get_count(&stat_cmp, datatype, &size_written);
    REQUIRE(size_written == size_written_orig_);
  }

  void test_iwrite_at(const void* ptr, size_t count, MPI_Datatype datatype,
                      MPI_Offset offset) {
    MPI_Status stat[2];
    MPI_Request request[2];
    auto ret_orig =
        MPI_File_iwrite_at(fh_orig_, offset, ptr, count, datatype, &request[0]);
    int size_written;
    auto ret_cmp =
        MPI_File_iwrite_at(fh_cmp_, offset, ptr, count, datatype, &request[1]);
    REQUIRE(ret_orig == ret_cmp);
    MPI_Waitall(2, request, stat);
    MPI_Get_count(&stat[0], datatype, &size_written_orig_);
    MPI_Get_count(&stat[0], datatype, &size_written);
    REQUIRE(size_written == size_written_orig_);
  }

  void test_write_at_all(const void* ptr, size_t count,
                         MPI_Datatype datatype,
                         MPI_Offset offset) {
    MPI_Status stat_orig, stat_cmp;
    auto ret_orig =
        MPI_File_write_at_all(fh_orig_, offset, ptr, count,
                              datatype, &stat_orig);
    int size_written;
    auto ret_cmp =
        MPI_File_write_at_all(fh_cmp_, offset, ptr, count,
                              datatype, &stat_cmp);
    REQUIRE(ret_orig == ret_cmp);
    MPI_Get_count(&stat_orig, datatype, &size_written_orig_);
    MPI_Get_count(&stat_cmp, datatype, &size_written);
    REQUIRE(size_written == size_written_orig_);
  }

  void test_iwrite_at_all(const void* ptr, size_t count,
                          MPI_Datatype datatype,
                          MPI_Offset offset) {
    MPI_Status stat[2];
    MPI_Request request[2];
    auto ret_orig = MPI_File_iwrite_at_all(fh_orig_, offset, ptr,
                                           count, datatype,
                                           &request[0]);
    int size_written;
    auto ret_cmp =
        MPI_File_iwrite_at_all(fh_cmp_, offset, ptr, count,
                               datatype, &request[1]);
    REQUIRE(ret_orig == ret_cmp);
    MPI_Waitall(2, request, stat);
    MPI_Get_count(&stat[0], datatype, &size_written_orig_);
    MPI_Get_count(&stat[1], datatype, &size_written);
    REQUIRE(size_written == size_written_orig_);
  }

  void test_write_ordered(const void* ptr, size_t count,
                          MPI_Datatype datatype) {
    MPI_Status stat_orig, stat_cmp;
    auto ret_orig =
        MPI_File_write_ordered(fh_orig_, ptr, count, datatype, &stat_orig);
    int size_written;
    auto ret_cmp =
        MPI_File_write_ordered(fh_cmp_, ptr, count, datatype, &stat_cmp);
    REQUIRE(ret_orig == ret_cmp);
    MPI_Get_count(&stat_orig, datatype, &size_written_orig_);
    MPI_Get_count(&stat_cmp, datatype, &size_written);
    REQUIRE(size_written == size_written_orig_);
  }

  void test_read(char* ptr, size_t count, MPI_Datatype datatype) {
    MPI_Status stat_orig, stat_cmp;
    auto ret_orig = MPI_File_read(fh_orig_, ptr, count,
                                  datatype, &stat_orig);
    int type_size;
    MPI_Type_size(datatype, &type_size);
    std::vector<unsigned char> read_data(count * type_size, 'r');
    int size_read;
    auto ret_cmp =
        MPI_File_read(fh_cmp_, read_data.data(), count,
                      datatype, &stat_cmp);
    REQUIRE(ret_orig == ret_cmp);
    MPI_Get_count(&stat_orig, datatype, &size_read_orig_);
    MPI_Get_count(&stat_cmp, datatype, &size_read);
    REQUIRE(size_read == size_read_orig_);
    test_read_data(size_read, count, type_size,
                   reinterpret_cast<char*>(read_data.data()), ptr);
  }

  void test_iread(char* ptr, size_t count, MPI_Datatype datatype) {
    MPI_Status stat[2];
    MPI_Request request[2];
    auto ret_orig = MPI_File_iread(fh_orig_, ptr, count,
                                   datatype, &request[0]);
    int type_size;
    MPI_Type_size(datatype, &type_size);
    std::vector<unsigned char> read_data(count * type_size, 'r');
    int size_read;
    auto ret_cmp =
        MPI_File_iread(fh_cmp_, read_data.data(), count,
                       datatype, &request[1]);
    REQUIRE(ret_orig == ret_cmp);
    MPI_Waitall(2, request, stat);
    MPI_Get_count(&stat[0], datatype, &size_read_orig_);
    MPI_Get_count(&stat[1], datatype, &size_read);
    REQUIRE(size_read == size_read_orig_);
    test_read_data(size_read, count, type_size,
                   reinterpret_cast<char*>(read_data.data()), ptr);
  }

  void test_read_shared(char* ptr, size_t count, MPI_Datatype datatype) {
    MPI_Status stat_orig, stat_cmp;
    auto ret_orig =
        MPI_File_read_shared(fh_orig_, ptr, count, datatype, &stat_orig);
    int type_size;
    MPI_Type_size(datatype, &type_size);
    std::vector<unsigned char> read_data(count * type_size, 'r');
    int size_read;
    auto ret_cmp = MPI_File_read_shared(fh_cmp_, read_data.data(),
                                        count, datatype,
                                        &stat_cmp);
    REQUIRE(ret_orig == ret_cmp);
    MPI_Get_count(&stat_orig, datatype, &size_read_orig_);
    MPI_Get_count(&stat_cmp, datatype, &size_read);
    REQUIRE(size_read == size_read_orig_);
    test_read_data(size_read, count, type_size,
                   reinterpret_cast<char*>(read_data.data()), ptr);
  }

  void test_iread_shared(char* ptr, size_t count, MPI_Datatype datatype) {
    MPI_Status stat[2];
    MPI_Request request[2];
    auto ret_orig =
        MPI_File_iread_shared(fh_orig_, ptr, count, datatype, &request[0]);
    int type_size;
    MPI_Type_size(datatype, &type_size);
    std::vector<unsigned char> read_data(count * type_size, 'r');
    int size_read;
    auto ret_cmp = MPI_File_iread_shared(fh_cmp_,
                                         read_data.data(), count,
                                         datatype, &request[1]);
    REQUIRE(ret_orig == ret_cmp);
    MPI_Waitall(2, request, stat);
    MPI_Get_count(&stat[0], datatype, &size_read_orig_);
    MPI_Get_count(&stat[1], datatype, &size_read);
    REQUIRE(size_read == size_read_orig_);
    test_read_data(size_read, count, type_size,
                   reinterpret_cast<char*>(read_data.data()), ptr);
  }

  void test_read_all(char* ptr, size_t count, MPI_Datatype datatype) {
    MPI_Status stat_orig, stat_cmp;
    auto ret_orig = MPI_File_read_all(fh_orig_, ptr, count,
                                      datatype, &stat_orig);
    int type_size;
    MPI_Type_size(datatype, &type_size);
    std::vector<unsigned char> read_data(count * type_size, 'r');
    int size_read;
    auto ret_cmp =
        MPI_File_read_all(fh_cmp_, read_data.data(), count,
                          datatype, &stat_cmp);
    REQUIRE(ret_orig == ret_cmp);
    MPI_Get_count(&stat_orig, datatype, &size_read_orig_);
    MPI_Get_count(&stat_cmp, datatype, &size_read);
    REQUIRE(size_read == size_read_orig_);
    test_read_data(size_read, count, type_size,
                   reinterpret_cast<char*>(read_data.data()), ptr);
  }

  void test_iread_all(char* ptr, size_t count, MPI_Datatype datatype) {
    MPI_Status stat[2];
    MPI_Request request[2];
    auto ret_orig =
        MPI_File_iread_all(fh_orig_, ptr, count, datatype, &request[0]);
    int type_size;
    MPI_Type_size(datatype, &type_size);
    std::vector<unsigned char> read_data(count * type_size, 'r');
    int size_read;
    auto ret_cmp = MPI_File_iread_all(fh_cmp_, read_data.data(),
                                      count, datatype,
                                      &request[1]);
    REQUIRE(ret_orig == ret_cmp);
    MPI_Waitall(2, request, stat);
    MPI_Get_count(&stat[0], datatype, &size_read_orig_);
    MPI_Get_count(&stat[1], datatype, &size_read);
    REQUIRE(size_read == size_read_orig_);
    test_read_data(size_read, count, type_size,
                   reinterpret_cast<char*>(read_data.data()), ptr);
  }

  void test_read_ordered(char* ptr, size_t count, MPI_Datatype datatype) {
    MPI_Status stat_orig, stat_cmp;
    auto ret_orig =
        MPI_File_read_ordered(fh_orig_, ptr, count, datatype, &stat_orig);
    int type_size;
    MPI_Type_size(datatype, &type_size);
    std::vector<unsigned char> read_data(count * type_size, 'r');
    int size_read;
    auto ret_cmp = MPI_File_read_ordered(fh_cmp_, read_data.data(), count,
                                         datatype, &stat_cmp);
    REQUIRE(ret_orig == ret_cmp);
    MPI_Get_count(&stat_orig, datatype, &size_read_orig_);
    MPI_Get_count(&stat_cmp, datatype, &size_read);
    REQUIRE(size_read == size_read_orig_);
    test_read_data(size_read, count, type_size,
                   reinterpret_cast<char*>(read_data.data()), ptr);
  }

  void test_read_at(char* ptr, size_t count, MPI_Datatype datatype,
                    MPI_Offset offset) {
    MPI_Status stat_orig, stat_cmp;
    auto ret_orig =
        MPI_File_read_at(fh_orig_, offset, ptr, count, datatype, &stat_orig);
    int type_size;
    MPI_Type_size(datatype, &type_size);
    std::vector<unsigned char> read_data(count * type_size, 'r');
    int size_read;
    auto ret_cmp = MPI_File_read_at(fh_cmp_, offset, read_data.data(), count,
                                    datatype, &stat_cmp);
    REQUIRE(ret_orig == ret_cmp);
    MPI_Get_count(&stat_orig, datatype, &size_read_orig_);
    MPI_Get_count(&stat_cmp, datatype, &size_read);
    REQUIRE(size_read == size_read_orig_);
    test_read_data(size_read, count, type_size,
                   reinterpret_cast<char*>(read_data.data()), ptr);
  }

  void test_iread_at(char* ptr, size_t count, MPI_Datatype datatype,
                     MPI_Offset offset) {
    MPI_Status stat[2];
    MPI_Request request[2];
    auto ret_orig =
        MPI_File_iread_at(fh_orig_, offset, ptr, count, datatype, &request[0]);
    int type_size;
    MPI_Type_size(datatype, &type_size);
    std::vector<unsigned char> read_data(count * type_size, 'r');
    int size_read;
    auto ret_cmp = MPI_File_iread_at(fh_cmp_, offset, read_data.data(), count,
                                     datatype, &request[1]);
    REQUIRE(ret_orig == ret_cmp);
    MPI_Waitall(2, request, stat);
    MPI_Get_count(&stat[0], datatype, &size_read_orig_);
    MPI_Get_count(&stat[1], datatype, &size_read);
    REQUIRE(size_read == size_read_orig_);
    test_read_data(size_read, count, type_size,
                   reinterpret_cast<char*>(read_data.data()), ptr);
  }

  void test_read_at_all(char* ptr, size_t count, MPI_Datatype datatype,
                        MPI_Offset offset) {
    MPI_Status stat_orig, stat_cmp;
    auto ret_orig =
        MPI_File_read_at_all(fh_orig_, offset, ptr, count,
                             datatype, &stat_orig);
    int type_size;
    MPI_Type_size(datatype, &type_size);
    std::vector<unsigned char> read_data(count * type_size, 'r');
    int size_read;
    auto ret_cmp = MPI_File_read_at_all(fh_cmp_, offset,
                                        read_data.data(), count,
                                        datatype, &stat_cmp);
    REQUIRE(ret_orig == ret_cmp);
    MPI_Get_count(&stat_orig, datatype, &size_read_orig_);
    MPI_Get_count(&stat_cmp, datatype, &size_read);
    REQUIRE(size_read == size_read_orig_);
    test_read_data(size_read, count, type_size,
                   reinterpret_cast<char*>(read_data.data()), ptr);
  }

  void test_iread_at_all(char* ptr, size_t count, MPI_Datatype datatype,
                         MPI_Offset offset) {
    MPI_Status stat[2];
    MPI_Request request[2];
    auto ret_orig =
        MPI_File_iread_at_all(fh_orig_, offset, ptr, count,
                              datatype, &request[0]);
    int type_size;
    MPI_Type_size(datatype, &type_size);
    std::vector<unsigned char> read_data(count * type_size, 'r');
    int size_read;
    auto ret_cmp = MPI_File_iread_at_all(fh_cmp_, offset,
                                         read_data.data(), count,
                                         datatype, &request[1]);
    REQUIRE(ret_orig == ret_cmp);
    MPI_Waitall(2, request, stat);
    MPI_Get_count(&stat[0], datatype, &size_read_orig_);
    MPI_Get_count(&stat[1], datatype, &size_read);
    REQUIRE(size_read == size_read_orig_);
    test_read_data(size_read, count, type_size,
                   reinterpret_cast<char*>(read_data.data()), ptr);
  }

  void test_seek(MPI_Offset offset, int whence) {
    status_orig_ = MPI_File_seek(fh_orig_, offset, whence);
    int status = MPI_File_seek(fh_cmp_, offset, whence);
    REQUIRE(status == status_orig_);
  }

  void test_seek_shared(MPI_Offset offset, int whence) {
    status_orig_ = MPI_File_seek_shared(fh_orig_, offset, whence);
    int status = MPI_File_seek_shared(fh_cmp_, offset, whence);
    REQUIRE(status == status_orig_);
  }
};

}  // namespace hermes::adapter::test

#define TESTER \
  hshm::EasySingleton<hermes::adapter::test::MpiioTest<true>>::GetInstance()

#endif  // HERMES_TEST_UNIT_HERMES_ADAPTERS_POSIX_POSIX_ADAPTER_BASE_TEST_H_
