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

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>

#include <experimental/filesystem>
#include <string>

#include <mpi.h>
#include <hdf5.h>
#include <hdf5_hl.h>

#include "hermes_types.h"
#include "adapter_test_utils.h"
#include "catch_config.h"

namespace fs = std::experimental::filesystem;

namespace hermes::adapter::vfd::test {

struct Arguments {
  std::string filename = "test";
  std::string extension = ".h5";
  std::string directory = "/tmp";
  size_t request_size = 65536;
};

struct TestInfo {
  int rank = 0;
  int comm_size = 1;
  std::string write_data;
  std::string read_data;
  std::string new_file;
  std::string existing_file;
  std::string new_file_cmp;
  std::string existing_file_cmp;
  size_t num_iterations = 64;
  int offset_seed = 1;
  unsigned int rs_seed = 1;
  unsigned int temporal_interval_seed = 5;
  size_t total_size;
  size_t stride_size = 512;
  unsigned int temporal_interval_ms = 1;
  size_t small_min = 1;
  size_t small_max = KILOBYTES(4);
  size_t medium_min = KILOBYTES(4) + 1;
  size_t medium_max = KILOBYTES(256);
  size_t large_min = KILOBYTES(256) + 1;
  size_t large_max = MEGABYTES(3);
};

class MuteHdf5Errors {
  H5E_auto2_t old_func;
  void *old_client_data;

 public:
  MuteHdf5Errors() {
    // Save old error handler
    H5Eget_auto(H5E_DEFAULT, &old_func, &old_client_data);

    // Turn off error stack printing
    H5Eset_auto(H5E_DEFAULT, NULL, NULL);
  }

  ~MuteHdf5Errors() {
    // Restore previous error handler
    H5Eset_auto(H5E_DEFAULT, old_func, old_client_data);
  }
};

void AssertFunc(bool expr, const char *file, int lineno, const char *message) {
  if (!expr) {
    fprintf(stderr, "Assertion failed at %s: line %d: %s\n", file, lineno,
            message);
    exit(-1);
  }
}

#define Assert(expr) AssertFunc((expr), __FILE__, __LINE__, #expr)

struct VfdApi {
  hid_t Open(const std::string &fname, unsigned flags) {
    hid_t result = H5Fopen(fname.c_str(), flags, H5P_DEFAULT);

    return result;
  }

  hid_t Create(const std::string &fname, unsigned flags) {
    hid_t result = H5Fcreate(fname.c_str(), flags, H5P_DEFAULT, H5P_DEFAULT);

    return result;
  }

  // Read() {
  // }

  herr_t Write(hid_t hid, const std::string &dset_name,
               const std::vector<float> &data) {
    herr_t result = Write(hid, dset_name, data.data(), data.size());

    return result;
  }

  herr_t Write(hid_t hid, const std::string &dset_name, const float *data,
               size_t size) {
    hsize_t dims[1] = {size};
    herr_t result = H5LTmake_dataset_float(hid, dset_name.c_str(), 1, dims,
                                           data);

    return result;
  }

  herr_t Close(hid_t id) {
    herr_t result = H5Fclose(id);

    return result;
  }
};

static inline u32 RotateLeft(const u32 x, int k) {
  u32 result = (x << k) | (x >> (32 - k));

  return result;
}


// xoshiro128+ random number generation: https://prng.di.unimi.it/xoshiro128plus.c
static u32 random_state[4] = {111, 222, 333, 444};

f32 GenNextRandom() {
  const uint32_t random = random_state[0] + random_state[3];

  const uint32_t t = random_state[1] << 9;

  random_state[2] ^= random_state[0];
  random_state[3] ^= random_state[1];
  random_state[1] ^= random_state[2];
  random_state[0] ^= random_state[3];

  random_state[2] ^= t;

  random_state[3] = RotateLeft(random_state[3], 11);

  f32 result = (random >> 8) * 0x1.0p-24f;

  return result;
}

void GenHdf5File(std::string fname, size_t dataset_size, size_t num_datasets) {
  size_t total_bytes = dataset_size * num_datasets;
  size_t total_floats = total_bytes / sizeof(f32);
  size_t floats_per_dataset = dataset_size / sizeof(f32);
  std::vector<float> data(total_floats);

  for (size_t i = 0; i < total_floats; ++i) {
    data[i] = GenNextRandom();
  }

  VfdApi api;
  float *at = data.data();

  hid_t file_id = api.Create(fname, H5F_ACC_TRUNC);
  Assert(file_id != H5I_INVALID_HID);

  for (size_t i = 0; i < num_datasets; ++i) {
    Assert(api.Write(file_id, std::to_string(i), at, dataset_size) > -1);
    at += floats_per_dataset;
  }

  Assert(api.Close(file_id) > -1);
}

}  // namespace hermes::adapter::vfd::test


hermes::adapter::vfd::test::Arguments args;
hermes::adapter::vfd::test::TestInfo info;

int init(int* argc, char*** argv) {
  MPI_Init(argc, argv);
  info.write_data = GenRandom(args.request_size);
  info.read_data = std::string(args.request_size, 'r');
  return 0;
}

int finalize() {
  MPI_Finalize();
  return 0;
}

int pretest() {
  fs::path fullpath = args.directory;
  fullpath /= args.filename;

  std::string suffix = std::to_string(getpid()) + args.extension;
  info.new_file = fullpath.string() + "_new_" + suffix;
  info.existing_file = fullpath.string() + "_ext_" + suffix;
  info.new_file_cmp = fullpath.string() + "_new_cmp_" + suffix;
  info.existing_file_cmp = fullpath.string() + "_ext_cmp_" + suffix;

  if (fs::exists(info.new_file)) fs::remove(info.new_file);
  if (fs::exists(info.new_file_cmp)) fs::remove(info.new_file_cmp);
  if (fs::exists(info.existing_file)) fs::remove(info.existing_file);
  if (fs::exists(info.existing_file_cmp)) fs::remove(info.existing_file_cmp);

  hermes::adapter::vfd::test::GenHdf5File(info.existing_file, args.request_size,
                                          info.num_iterations);
  info.total_size = fs::file_size(info.existing_file);

  std::string cmd = "cp " + info.existing_file + " " + info.existing_file_cmp;
  int status = system(cmd.c_str());
  REQUIRE(status != -1);
  REQUIRE(info.total_size > 0);

  return 0;
}

int posttest(bool compare_data = true) {
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
        if (d1[pos] != d2[pos]) {
          char_mismatch++;
        }
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

// int main(int argc, char *argv[]) {
//   hid_t file_id;
//   hid_t dset_id;
//   hid_t dataspace_id;
//   hid_t fapl_id;
//   hsize_t dims[2];
//   const char *file_name = "hermes_test.h5";
//   const int kNx = 128;
//   const int kNy = 128;
//   int data_in[kNx][kNy];  /* data to write */
//   int data_out[kNx][kNy];  /* data to read */
//   int i, j;

//   int mpi_threads_provided;
//   MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &mpi_threads_provided);
//   if (mpi_threads_provided < MPI_THREAD_MULTIPLE) {
//     fprintf(stderr,
//             "Didn't receive appropriate MPI threading specification\n");
//     return 1;
//   }

//   const char kDatasetName[] = "IntArray";
//   dims[0] = kNx;
//   dims[1] = kNy;

//   Assert((fapl_id = H5Pcreate(H5P_FILE_ACCESS)) >= 0);
//   Assert(H5Pset_fapl_hermes(fapl_id, true, 1024) >= 0);

//   Assert((file_id = H5Fcreate(file_name, H5F_ACC_TRUNC, H5P_DEFAULT,
//                               fapl_id)) >= 0);
//   Assert((dataspace_id = H5Screate_simple(2, dims, NULL)) >= 0);
//   dset_id = H5Dcreate(file_id, kDatasetName, H5T_NATIVE_INT, dataspace_id,
//                       H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
//   Assert(dset_id >= 0);

//   for (j = 0; j < kNx; j++) {
//     for (i = 0; i < kNy; i++) {
//       data_in[j][i] = i + j;
//       data_out[j][i] = 0;
//     }
//   }

//   Assert(H5Dwrite(dset_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT,
//                   data_in) >= 0);
//   Assert(H5Dclose(dset_id) >= 0);
//   Assert(H5Fclose(file_id) >= 0);

//   hid_t fid;
//   Assert((fid = H5Fopen(file_name, H5F_ACC_RDONLY, H5P_DEFAULT)) >= 0);
//   Assert((dset_id = H5Dopen(fid, kDatasetName, H5P_DEFAULT)) >= 0);
//   Assert(H5Dread(dset_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT,
//                  data_out) >= 0);

//   for (j = 0; j < kNx; j++) {
//     for (i = 0; i < kNy; i++) {
//       Assert(data_out[j][i] == data_in[j][i]);
//     }
//   }

//   Assert(H5Dclose(dset_id) >= 0);
//   Assert(H5Fclose(fid) >= 0);
//   Assert(H5Sclose(dataspace_id) >= 0);
//   Assert(H5Pclose(fapl_id) >= 0);
//   remove(file_name);

//   return 0;
// }

#include "hermes_vfd_basic_test.cc"
