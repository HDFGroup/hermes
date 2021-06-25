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

#include <adapter_utils.h>
#include <catch_config.h>
#include <unistd.h>

#include <experimental/filesystem>
#include <iostream>

#if HERMES_INTERCEPT == 1
#include <hermes/adapter/mpiio.h>
#endif

namespace fs = std::experimental::filesystem;

namespace hermes::adapter::mpiio::test {
struct Arguments {
  std::string filename = "test.dat";
  std::string directory = "/tmp";
  size_t request_size = 65536;
};
struct Info {
  bool debug = false;
  int rank = 0;
  int comm_size = 1;
  std::string write_data;
  std::string read_data;
  std::string new_file;
  std::string existing_file;
  std::string shared_new_file;
  std::string shared_existing_file;
  std::string new_file_cmp;
  std::string existing_file_cmp;
  std::string shared_new_file_cmp;
  std::string shared_existing_file_cmp;
  size_t num_iterations = 64;
  unsigned int offset_seed = 1;
  unsigned int rs_seed = 1;
  unsigned int temporal_interval_seed = 5;
  size_t total_size;
  size_t stride_size = 512;
  unsigned int temporal_interval_ms = 1;
  size_t small_min = 1, small_max = 4 * 1024;
  size_t medium_min = 4 * 1024 + 1, medium_max = 256 * 1024;
  size_t large_min = 256 * 1024 + 1, large_max = 3 * 1024 * 1024;
};
}  // namespace hermes::adapter::mpiio::test
hermes::adapter::mpiio::test::Arguments args;
hermes::adapter::mpiio::test::Info info;
std::string gen_random(const int len) {
  std::string tmp_s;
  static const char alphanum[] =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";

  srand(100);

  tmp_s.reserve(len);

  for (int i = 0; i < len; ++i)
    tmp_s += alphanum[rand() % (sizeof(alphanum) - 1)];

  return tmp_s;
}
int init(int* argc, char*** argv) {
  MPI_Init(argc, argv);
  info.write_data = gen_random(args.request_size);
  info.read_data = std::string(args.request_size, 'r');
  MPI_Comm_rank(MPI_COMM_WORLD, &info.rank);
  MPI_Comm_size(MPI_COMM_WORLD, &info.comm_size);
  if (info.debug && info.rank == 0) {
    printf("ready for attach\n");
    fflush(stdout);
    sleep(30);
  }
  MPI_Barrier(MPI_COMM_WORLD);
  return 0;
}
int finalize() {
  MPI_Finalize();
  return 0;
}

const char* kUser = "USER";

int pretest() {
  fs::path fullpath = args.directory;
  fullpath /= args.filename + "_" + std::string(getenv(kUser));
  info.new_file = fullpath.string() + "_new_" + std::to_string(getpid());
  info.existing_file = fullpath.string() + "_ext_" + std::to_string(getpid());
  info.new_file_cmp =
      fullpath.string() + "_new_cmp" + "_" + std::to_string(getpid());
  info.existing_file_cmp =
      fullpath.string() + "_ext_cmp" + "_" + std::to_string(getpid());
  info.shared_new_file =
      fullpath.string() + "_new_" + std::to_string(info.comm_size);
  info.shared_existing_file =
      fullpath.string() + "_ext_" + std::to_string(info.comm_size);
  info.shared_new_file_cmp =
      fullpath.string() + "_new_cmp_" + std::to_string(info.comm_size);
  info.shared_existing_file_cmp =
      fullpath.string() + "_ext_cmp_" + std::to_string(info.comm_size);
  if (fs::exists(info.new_file)) fs::remove(info.new_file);
  if (fs::exists(info.new_file_cmp)) fs::remove(info.new_file_cmp);
  if (fs::exists(info.existing_file)) fs::remove(info.existing_file);
  if (fs::exists(info.existing_file_cmp)) fs::remove(info.existing_file_cmp);
  if (!fs::exists(info.existing_file)) {
    std::string cmd = "{ tr -dc '[:alnum:]' < /dev/urandom | head -c " +
                      std::to_string(args.request_size * info.num_iterations) +
                      "; } > " + info.existing_file + " 2> /dev/null";
    int status = system(cmd.c_str());
    REQUIRE(status != -1);
    REQUIRE(fs::file_size(info.existing_file) ==
            args.request_size * info.num_iterations);
    info.total_size = fs::file_size(info.existing_file);
  }
  MPI_Barrier(MPI_COMM_WORLD);
  if (!fs::exists(info.existing_file_cmp)) {
    std::string cmd = "cp " + info.existing_file + " " + info.existing_file_cmp;
    int status = system(cmd.c_str());
    REQUIRE(status != -1);
    REQUIRE(fs::file_size(info.existing_file_cmp) ==
            args.request_size * info.num_iterations);
  }
  MPI_Barrier(MPI_COMM_WORLD);
  if (info.rank == 0) {
    if (fs::exists(info.shared_new_file)) fs::remove(info.shared_new_file);
    if (fs::exists(info.shared_existing_file))
      fs::remove(info.shared_existing_file);
    if (!fs::exists(info.shared_existing_file)) {
      std::string cmd =
          "cp " + info.existing_file + " " + info.shared_existing_file;
      int status = system(cmd.c_str());
      REQUIRE(status != -1);
      REQUIRE(fs::file_size(info.shared_existing_file) ==
              args.request_size * info.num_iterations);
    }
    if (fs::exists(info.shared_new_file_cmp))
      fs::remove(info.shared_new_file_cmp);
    if (fs::exists(info.shared_existing_file_cmp))
      fs::remove(info.shared_existing_file_cmp);
    if (!fs::exists(info.shared_existing_file_cmp)) {
      std::string cmd =
          "cp " + info.existing_file + " " + info.shared_existing_file_cmp;
      int status = system(cmd.c_str());
      REQUIRE(status != -1);
      REQUIRE(fs::file_size(info.shared_existing_file_cmp) ==
              args.request_size * info.num_iterations);
    }
  }
  MPI_Barrier(MPI_COMM_WORLD);
  REQUIRE(info.total_size > 0);
#if HERMES_INTERCEPT == 1
  INTERCEPTOR_LIST->hermes_flush_exclusion.insert(info.existing_file_cmp);
  INTERCEPTOR_LIST->hermes_flush_exclusion.insert(info.new_file_cmp);
#endif
  return 0;
}

int posttest(bool compare_data = true) {
#if HERMES_INTERCEPT == 1
  INTERCEPTOR_LIST->hermes_flush_exclusion.insert(info.existing_file);
  INTERCEPTOR_LIST->hermes_flush_exclusion.insert(info.new_file);
#endif
  if (compare_data && fs::exists(info.new_file) &&
      fs::exists(info.new_file_cmp)) {
    size_t size = fs::file_size(info.new_file);
    REQUIRE(size == fs::file_size(info.new_file_cmp));
    if (size > 0) {
      std::vector<unsigned char> d1(size, '0');
      std::vector<unsigned char> d2(size, '1');

      FILE* fh1 = fopen(info.new_file.c_str(), "r");
      REQUIRE(fh1 != nullptr);
      size_t read_d1 = fread(d1.data(), size, sizeof(unsigned char), fh1);
      REQUIRE(read_d1 == sizeof(unsigned char));
      int status = fclose(fh1);
      REQUIRE(status == 0);

      FILE* fh2 = fopen(info.new_file_cmp.c_str(), "r");
      REQUIRE(fh2 != nullptr);
      size_t read_d2 = fread(d2.data(), size, sizeof(unsigned char), fh2);
      REQUIRE(read_d2 == sizeof(unsigned char));
      status = fclose(fh2);
      REQUIRE(status == 0);

      size_t char_mismatch = 0;
      for (size_t pos = 0; pos < size; ++pos) {
        if (d1[pos] != d2[pos]) char_mismatch++;
      }
      REQUIRE(char_mismatch == 0);
    }
  }
  if (compare_data && fs::exists(info.existing_file) &&
      fs::exists(info.existing_file_cmp)) {
    size_t size = fs::file_size(info.existing_file);
    if (size != fs::file_size(info.existing_file_cmp)) sleep(1);
    REQUIRE(size == fs::file_size(info.existing_file_cmp));
    if (size > 0) {
      std::vector<unsigned char> d1(size, '0');
      std::vector<unsigned char> d2(size, '1');

      FILE* fh1 = fopen(info.existing_file.c_str(), "r");
      REQUIRE(fh1 != nullptr);
      size_t read_d1 = fread(d1.data(), size, sizeof(unsigned char), fh1);
      REQUIRE(read_d1 == sizeof(unsigned char));
      int status = fclose(fh1);
      REQUIRE(status == 0);

      FILE* fh2 = fopen(info.existing_file_cmp.c_str(), "r");
      REQUIRE(fh2 != nullptr);
      size_t read_d2 = fread(d2.data(), size, sizeof(unsigned char), fh2);
      REQUIRE(read_d2 == sizeof(unsigned char));
      status = fclose(fh2);
      REQUIRE(status == 0);
      size_t char_mismatch = 0;
      for (size_t pos = 0; pos < size; ++pos) {
        if (d1[pos] != d2[pos]) char_mismatch++;
      }
      REQUIRE(char_mismatch == 0);
    }
  }
  /* Clean up. */
  if (fs::exists(info.new_file)) fs::remove(info.new_file);
  if (fs::exists(info.existing_file)) fs::remove(info.existing_file);
  if (fs::exists(info.new_file_cmp)) fs::remove(info.new_file_cmp);
  if (fs::exists(info.existing_file_cmp)) fs::remove(info.existing_file_cmp);

#if HERMES_INTERCEPT == 1
  INTERCEPTOR_LIST->hermes_flush_exclusion.erase(info.existing_file_cmp);
  INTERCEPTOR_LIST->hermes_flush_exclusion.erase(info.new_file_cmp);
  INTERCEPTOR_LIST->hermes_flush_exclusion.erase(info.new_file);
  INTERCEPTOR_LIST->hermes_flush_exclusion.erase(info.existing_file);
#endif
  return 0;
}

cl::Parser define_options() {
  return cl::Opt(args.filename, "filename")["-f"]["--filename"](
             "Filename used for performing I/O") |
         cl::Opt(args.directory, "dir")["-d"]["--directory"](
             "Directory used for performing I/O") |
         cl::Opt(args.request_size, "request_size")["-s"]["--request_size"](
             "Request size used for performing I/O");
}

namespace test {
MPI_File fh_orig;
MPI_File fh_cmp;
int status_orig;
int size_read_orig;
int size_written_orig;

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

void test_open(const char* path, int mode, MPI_Comm comm) {
  std::string cmp_path;
  if (strcmp(path, info.new_file.c_str()) == 0) {
    cmp_path = info.new_file_cmp;
  } else if (strcmp(path, info.existing_file.c_str()) == 0) {
    cmp_path = info.existing_file_cmp;
  } else if (strcmp(path, info.shared_new_file.c_str()) == 0) {
    cmp_path = info.shared_new_file_cmp;
  } else {
    cmp_path = info.shared_existing_file_cmp;
  }
  status_orig = MPI_File_open(comm, path, mode, MPI_INFO_NULL, &fh_orig);
  auto status_cmp =
      MPI_File_open(comm, cmp_path.c_str(), mode, MPI_INFO_NULL, &fh_cmp);
  bool is_same = (status_orig != MPI_SUCCESS && status_cmp != MPI_SUCCESS) ||
                 (status_orig == MPI_SUCCESS && status_cmp == MPI_SUCCESS);
  REQUIRE(is_same);
}
void test_close() {
  status_orig = MPI_File_close(&fh_orig);
  int status = MPI_File_close(&fh_cmp);
  REQUIRE(status == status_orig);
}

void test_preallocate(MPI_Offset size) {
  status_orig = MPI_File_preallocate(fh_orig, size);
  int status = MPI_File_preallocate(fh_cmp, size);
  REQUIRE(status == status_orig);
}

void test_write(const void* ptr, size_t count, MPI_Datatype datatype) {
  MPI_Status stat_orig, stat_cmp;
  auto ret_orig = MPI_File_write(fh_orig, ptr, count, datatype, &stat_orig);
  int size_written;
  auto ret_cmp = MPI_File_write(fh_cmp, ptr, count, datatype, &stat_cmp);
  REQUIRE(ret_orig == ret_cmp);
  MPI_Get_count(&stat_orig, datatype, &size_written_orig);
  MPI_Get_count(&stat_cmp, datatype, &size_written);
  REQUIRE(size_written == size_written_orig);
}

void test_iwrite(const void* ptr, size_t count, MPI_Datatype datatype) {
  MPI_Status stat[2];
  MPI_Request request[2];
  auto ret_orig = MPI_File_iwrite(fh_orig, ptr, count, datatype, &request[0]);
  int size_written;
  auto ret_cmp = MPI_File_iwrite(fh_cmp, ptr, count, datatype, &request[1]);
  REQUIRE(ret_orig == ret_cmp);
  MPI_Waitall(2, request, stat);
  MPI_Get_count(&stat[0], datatype, &size_written_orig);
  MPI_Get_count(&stat[1], datatype, &size_written);
  REQUIRE(size_written == size_written_orig);
}

void test_write_shared(const void* ptr, size_t count, MPI_Datatype datatype) {
  MPI_Status stat_orig, stat_cmp;
  auto ret_orig =
      MPI_File_write_shared(fh_orig, ptr, count, datatype, &stat_orig);
  int size_written;
  auto ret_cmp = MPI_File_write_shared(fh_cmp, ptr, count, datatype, &stat_cmp);
  REQUIRE(ret_orig == ret_cmp);
  MPI_Get_count(&stat_orig, datatype, &size_written_orig);
  MPI_Get_count(&stat_cmp, datatype, &size_written);
  REQUIRE(size_written == size_written_orig);
}

void test_iwrite_shared(const void* ptr, size_t count, MPI_Datatype datatype) {
  MPI_Status stat[2];
  MPI_Request request[2];
  auto ret_orig =
      MPI_File_iwrite_shared(fh_orig, ptr, count, datatype, &request[0]);
  int size_written;
  auto ret_cmp =
      MPI_File_iwrite_shared(fh_cmp, ptr, count, datatype, &request[1]);
  REQUIRE(ret_orig == ret_cmp);
  MPI_Waitall(2, request, stat);
  MPI_Get_count(&stat[0], datatype, &size_written_orig);
  MPI_Get_count(&stat[1], datatype, &size_written);
  REQUIRE(size_written == size_written_orig);
}

void test_write_all(const void* ptr, size_t count, MPI_Datatype datatype) {
  MPI_Status stat_orig, stat_cmp;
  auto ret_orig = MPI_File_write_all(fh_orig, ptr, count, datatype, &stat_orig);
  int size_written;
  auto ret_cmp = MPI_File_write_all(fh_cmp, ptr, count, datatype, &stat_cmp);
  REQUIRE(ret_orig == ret_cmp);
  MPI_Get_count(&stat_orig, datatype, &size_written_orig);
  MPI_Get_count(&stat_cmp, datatype, &size_written);
  REQUIRE(size_written == size_written_orig);
}

void test_iwrite_all(const void* ptr, size_t count, MPI_Datatype datatype) {
  MPI_Status stat[2];
  MPI_Request request[2];
  auto ret_orig =
      MPI_File_iwrite_all(fh_orig, ptr, count, datatype, &request[0]);
  int size_written;
  auto ret_cmp = MPI_File_iwrite_all(fh_cmp, ptr, count, datatype, &request[1]);
  REQUIRE(ret_orig == ret_cmp);
  MPI_Waitall(2, request, stat);
  MPI_Get_count(&stat[0], datatype, &size_written_orig);
  MPI_Get_count(&stat[1], datatype, &size_written);
  REQUIRE(size_written == size_written_orig);
}

void test_write_at(const void* ptr, size_t count, MPI_Datatype datatype,
                   MPI_Offset offset) {
  MPI_Status stat_orig, stat_cmp;
  auto ret_orig =
      MPI_File_write_at(fh_orig, offset, ptr, count, datatype, &stat_orig);
  int size_written;
  auto ret_cmp =
      MPI_File_write_at(fh_cmp, offset, ptr, count, datatype, &stat_cmp);
  REQUIRE(ret_orig == ret_cmp);
  MPI_Get_count(&stat_orig, datatype, &size_written_orig);
  MPI_Get_count(&stat_cmp, datatype, &size_written);
  REQUIRE(size_written == size_written_orig);
}

void test_iwrite_at(const void* ptr, size_t count, MPI_Datatype datatype,
                    MPI_Offset offset) {
  MPI_Status stat[2];
  MPI_Request request[2];
  auto ret_orig =
      MPI_File_iwrite_at(fh_orig, offset, ptr, count, datatype, &request[0]);
  int size_written;
  auto ret_cmp =
      MPI_File_iwrite_at(fh_cmp, offset, ptr, count, datatype, &request[1]);
  REQUIRE(ret_orig == ret_cmp);
  MPI_Waitall(2, request, stat);
  MPI_Get_count(&stat[0], datatype, &size_written_orig);
  MPI_Get_count(&stat[0], datatype, &size_written);
  REQUIRE(size_written == size_written_orig);
}

void test_write_at_all(const void* ptr, size_t count, MPI_Datatype datatype,
                       MPI_Offset offset) {
  MPI_Status stat_orig, stat_cmp;
  auto ret_orig =
      MPI_File_write_at_all(fh_orig, offset, ptr, count, datatype, &stat_orig);
  int size_written;
  auto ret_cmp =
      MPI_File_write_at_all(fh_cmp, offset, ptr, count, datatype, &stat_cmp);
  REQUIRE(ret_orig == ret_cmp);
  MPI_Get_count(&stat_orig, datatype, &size_written_orig);
  MPI_Get_count(&stat_cmp, datatype, &size_written);
  REQUIRE(size_written == size_written_orig);
}

void test_iwrite_at_all(const void* ptr, size_t count, MPI_Datatype datatype,
                        MPI_Offset offset) {
  MPI_Status stat[2];
  MPI_Request request[2];
  auto ret_orig = MPI_File_iwrite_at_all(fh_orig, offset, ptr, count, datatype,
                                         &request[0]);
  int size_written;
  auto ret_cmp =
      MPI_File_iwrite_at_all(fh_cmp, offset, ptr, count, datatype, &request[1]);
  REQUIRE(ret_orig == ret_cmp);
  MPI_Waitall(2, request, stat);
  MPI_Get_count(&stat[0], datatype, &size_written_orig);
  MPI_Get_count(&stat[1], datatype, &size_written);
  REQUIRE(size_written == size_written_orig);
}

void test_write_ordered(const void* ptr, size_t count, MPI_Datatype datatype) {
  MPI_Status stat_orig, stat_cmp;
  auto ret_orig =
      MPI_File_write_ordered(fh_orig, ptr, count, datatype, &stat_orig);
  int size_written;
  auto ret_cmp =
      MPI_File_write_ordered(fh_cmp, ptr, count, datatype, &stat_cmp);
  REQUIRE(ret_orig == ret_cmp);
  MPI_Get_count(&stat_orig, datatype, &size_written_orig);
  MPI_Get_count(&stat_cmp, datatype, &size_written);
  REQUIRE(size_written == size_written_orig);
}

void test_read(char* ptr, size_t count, MPI_Datatype datatype) {
  MPI_Status stat_orig, stat_cmp;
  auto ret_orig = MPI_File_read(fh_orig, ptr, count, datatype, &stat_orig);
  int type_size;
  MPI_Type_size(datatype, &type_size);
  std::vector<unsigned char> read_data(count * type_size, 'r');
  int size_read;
  auto ret_cmp =
      MPI_File_read(fh_cmp, read_data.data(), count, datatype, &stat_cmp);
  REQUIRE(ret_orig == ret_cmp);
  MPI_Get_count(&stat_orig, datatype, &size_read_orig);
  MPI_Get_count(&stat_cmp, datatype, &size_read);
  REQUIRE(size_read == size_read_orig);
  test_read_data(size_read, count, type_size,
                 reinterpret_cast<char*>(read_data.data()), ptr);
}

void test_iread(char* ptr, size_t count, MPI_Datatype datatype) {
  MPI_Status stat[2];
  MPI_Request request[2];
  auto ret_orig = MPI_File_iread(fh_orig, ptr, count, datatype, &request[0]);
  int type_size;
  MPI_Type_size(datatype, &type_size);
  std::vector<unsigned char> read_data(count * type_size, 'r');
  int size_read;
  auto ret_cmp =
      MPI_File_iread(fh_cmp, read_data.data(), count, datatype, &request[1]);
  REQUIRE(ret_orig == ret_cmp);
  MPI_Waitall(2, request, stat);
  MPI_Get_count(&stat[0], datatype, &size_read_orig);
  MPI_Get_count(&stat[1], datatype, &size_read);
  REQUIRE(size_read == size_read_orig);
  test_read_data(size_read, count, type_size,
                 reinterpret_cast<char*>(read_data.data()), ptr);
}

void test_read_shared(char* ptr, size_t count, MPI_Datatype datatype) {
  MPI_Status stat_orig, stat_cmp;
  auto ret_orig =
      MPI_File_read_shared(fh_orig, ptr, count, datatype, &stat_orig);
  int type_size;
  MPI_Type_size(datatype, &type_size);
  std::vector<unsigned char> read_data(count * type_size, 'r');
  int size_read;
  auto ret_cmp = MPI_File_read_shared(fh_cmp, read_data.data(), count, datatype,
                                      &stat_cmp);
  REQUIRE(ret_orig == ret_cmp);
  MPI_Get_count(&stat_orig, datatype, &size_read_orig);
  MPI_Get_count(&stat_cmp, datatype, &size_read);
  REQUIRE(size_read == size_read_orig);
  test_read_data(size_read, count, type_size,
                 reinterpret_cast<char*>(read_data.data()), ptr);
}

void test_iread_shared(char* ptr, size_t count, MPI_Datatype datatype) {
  MPI_Status stat[2];
  MPI_Request request[2];
  auto ret_orig =
      MPI_File_iread_shared(fh_orig, ptr, count, datatype, &request[0]);
  int type_size;
  MPI_Type_size(datatype, &type_size);
  std::vector<unsigned char> read_data(count * type_size, 'r');
  int size_read;
  auto ret_cmp = MPI_File_iread_shared(fh_cmp, read_data.data(), count,
                                       datatype, &request[1]);
  REQUIRE(ret_orig == ret_cmp);
  MPI_Waitall(2, request, stat);
  MPI_Get_count(&stat[0], datatype, &size_read_orig);
  MPI_Get_count(&stat[1], datatype, &size_read);
  REQUIRE(size_read == size_read_orig);
  test_read_data(size_read, count, type_size,
                 reinterpret_cast<char*>(read_data.data()), ptr);
}

void test_read_all(char* ptr, size_t count, MPI_Datatype datatype) {
  MPI_Status stat_orig, stat_cmp;
  auto ret_orig = MPI_File_read_all(fh_orig, ptr, count, datatype, &stat_orig);
  int type_size;
  MPI_Type_size(datatype, &type_size);
  std::vector<unsigned char> read_data(count * type_size, 'r');
  int size_read;
  auto ret_cmp =
      MPI_File_read_all(fh_cmp, read_data.data(), count, datatype, &stat_cmp);
  REQUIRE(ret_orig == ret_cmp);
  MPI_Get_count(&stat_orig, datatype, &size_read_orig);
  MPI_Get_count(&stat_cmp, datatype, &size_read);
  REQUIRE(size_read == size_read_orig);
  test_read_data(size_read, count, type_size,
                 reinterpret_cast<char*>(read_data.data()), ptr);
}

void test_iread_all(char* ptr, size_t count, MPI_Datatype datatype) {
  MPI_Status stat[2];
  MPI_Request request[2];
  auto ret_orig =
      MPI_File_iread_all(fh_orig, ptr, count, datatype, &request[0]);
  int type_size;
  MPI_Type_size(datatype, &type_size);
  std::vector<unsigned char> read_data(count * type_size, 'r');
  int size_read;
  auto ret_cmp = MPI_File_iread_all(fh_cmp, read_data.data(), count, datatype,
                                    &request[1]);
  REQUIRE(ret_orig == ret_cmp);
  MPI_Waitall(2, request, stat);
  MPI_Get_count(&stat[0], datatype, &size_read_orig);
  MPI_Get_count(&stat[1], datatype, &size_read);
  REQUIRE(size_read == size_read_orig);
  test_read_data(size_read, count, type_size,
                 reinterpret_cast<char*>(read_data.data()), ptr);
}

void test_read_ordered(char* ptr, size_t count, MPI_Datatype datatype) {
  MPI_Status stat_orig, stat_cmp;
  auto ret_orig =
      MPI_File_read_ordered(fh_orig, ptr, count, datatype, &stat_orig);
  int type_size;
  MPI_Type_size(datatype, &type_size);
  std::vector<unsigned char> read_data(count * type_size, 'r');
  int size_read;
  auto ret_cmp = MPI_File_read_ordered(fh_cmp, read_data.data(), count,
                                       datatype, &stat_cmp);
  REQUIRE(ret_orig == ret_cmp);
  MPI_Get_count(&stat_orig, datatype, &size_read_orig);
  MPI_Get_count(&stat_cmp, datatype, &size_read);
  REQUIRE(size_read == size_read_orig);
  test_read_data(size_read, count, type_size,
                 reinterpret_cast<char*>(read_data.data()), ptr);
}

void test_read_at(char* ptr, size_t count, MPI_Datatype datatype,
                  MPI_Offset offset) {
  MPI_Status stat_orig, stat_cmp;
  auto ret_orig =
      MPI_File_read_at(fh_orig, offset, ptr, count, datatype, &stat_orig);
  int type_size;
  MPI_Type_size(datatype, &type_size);
  std::vector<unsigned char> read_data(count * type_size, 'r');
  int size_read;
  auto ret_cmp = MPI_File_read_at(fh_cmp, offset, read_data.data(), count,
                                  datatype, &stat_cmp);
  REQUIRE(ret_orig == ret_cmp);
  MPI_Get_count(&stat_orig, datatype, &size_read_orig);
  MPI_Get_count(&stat_cmp, datatype, &size_read);
  REQUIRE(size_read == size_read_orig);
  test_read_data(size_read, count, type_size,
                 reinterpret_cast<char*>(read_data.data()), ptr);
}

void test_iread_at(char* ptr, size_t count, MPI_Datatype datatype,
                   MPI_Offset offset) {
  MPI_Status stat[2];
  MPI_Request request[2];
  auto ret_orig =
      MPI_File_iread_at(fh_orig, offset, ptr, count, datatype, &request[0]);
  int type_size;
  MPI_Type_size(datatype, &type_size);
  std::vector<unsigned char> read_data(count * type_size, 'r');
  int size_read;
  auto ret_cmp = MPI_File_iread_at(fh_cmp, offset, read_data.data(), count,
                                   datatype, &request[1]);
  REQUIRE(ret_orig == ret_cmp);
  MPI_Waitall(2, request, stat);
  MPI_Get_count(&stat[0], datatype, &size_read_orig);
  MPI_Get_count(&stat[1], datatype, &size_read);
  REQUIRE(size_read == size_read_orig);
  test_read_data(size_read, count, type_size,
                 reinterpret_cast<char*>(read_data.data()), ptr);
}

void test_read_at_all(char* ptr, size_t count, MPI_Datatype datatype,
                      MPI_Offset offset) {
  MPI_Status stat_orig, stat_cmp;
  auto ret_orig =
      MPI_File_read_at_all(fh_orig, offset, ptr, count, datatype, &stat_orig);
  int type_size;
  MPI_Type_size(datatype, &type_size);
  std::vector<unsigned char> read_data(count * type_size, 'r');
  int size_read;
  auto ret_cmp = MPI_File_read_at_all(fh_cmp, offset, read_data.data(), count,
                                      datatype, &stat_cmp);
  REQUIRE(ret_orig == ret_cmp);
  MPI_Get_count(&stat_orig, datatype, &size_read_orig);
  MPI_Get_count(&stat_cmp, datatype, &size_read);
  REQUIRE(size_read == size_read_orig);
  test_read_data(size_read, count, type_size,
                 reinterpret_cast<char*>(read_data.data()), ptr);
}

void test_iread_at_all(char* ptr, size_t count, MPI_Datatype datatype,
                       MPI_Offset offset) {
  MPI_Status stat[2];
  MPI_Request request[2];
  auto ret_orig =
      MPI_File_iread_at_all(fh_orig, offset, ptr, count, datatype, &request[0]);
  int type_size;
  MPI_Type_size(datatype, &type_size);
  std::vector<unsigned char> read_data(count * type_size, 'r');
  int size_read;
  auto ret_cmp = MPI_File_iread_at_all(fh_cmp, offset, read_data.data(), count,
                                       datatype, &request[1]);
  REQUIRE(ret_orig == ret_cmp);
  MPI_Waitall(2, request, stat);
  MPI_Get_count(&stat[0], datatype, &size_read_orig);
  MPI_Get_count(&stat[1], datatype, &size_read);
  REQUIRE(size_read == size_read_orig);
  test_read_data(size_read, count, type_size,
                 reinterpret_cast<char*>(read_data.data()), ptr);
}

void test_seek(MPI_Offset offset, int whence) {
  status_orig = MPI_File_seek(fh_orig, offset, whence);
  int status = MPI_File_seek(fh_cmp, offset, whence);
  REQUIRE(status == status_orig);
}

void test_seek_shared(MPI_Offset offset, int whence) {
  status_orig = MPI_File_seek_shared(fh_orig, offset, whence);
  int status = MPI_File_seek_shared(fh_cmp, offset, whence);
  REQUIRE(status == status_orig);
}
}  // namespace test

#include "mpiio_adapter_basic_test.cpp"
