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
using hermes::f32;
using hermes::u32;

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
  std::vector<f32> write_data;
  std::vector<f32> read_data;
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

struct RwIds {
  hid_t dset_id;
  hid_t dspace_id;
  hid_t mspace_id;
};

// TODO(chogan): Document
// Working towards a common adapter api that adapters can subclass
struct VfdApi {
  hid_t sec2_fapl;

  VfdApi() : sec2_fapl(H5I_INVALID_HID) {
    sec2_fapl = H5Pcreate(H5P_FILE_ACCESS);
    Assert(sec2_fapl >= 0);
    Assert(H5Pset_fapl_sec2(sec2_fapl) >= 0);
  }

  ~VfdApi() {
    Assert(H5Pclose(sec2_fapl) >= 0);
    sec2_fapl = H5I_INVALID_HID;
  }

  hid_t Open(const std::string &fname, unsigned flags) {
    hid_t result = H5Fopen(fname.c_str(), flags, H5P_DEFAULT);

    return result;
  }

  hid_t OpenPosix(const std::string &fname, unsigned flags) {
    hid_t result = H5Fopen(fname.c_str(), flags, sec2_fapl);

    return result;
  }

  hid_t Create(const std::string &fname, unsigned flags) {
    hid_t result = H5Fcreate(fname.c_str(), flags, H5P_DEFAULT, H5P_DEFAULT);

    return result;
  }

  hid_t CreatePosix(const std::string &fname, unsigned flags) {
    hid_t result = H5Fcreate(fname.c_str(), flags, H5P_DEFAULT, sec2_fapl);

    return result;
  }

  RwIds RwPreamble(hid_t hid, const std::string &dset_name, hsize_t offset,
                   hsize_t num_bytes, hsize_t stride = 1) {
    hid_t dset_id = H5Dopen2(hid, dset_name.c_str(), H5P_DEFAULT);
    REQUIRE(dset_id != H5I_INVALID_HID);
    hsize_t num_elements = num_bytes / sizeof(f32);
    hid_t memspace_id = H5Screate_simple(1, &num_elements, NULL);
    REQUIRE(memspace_id != H5I_INVALID_HID);
    hid_t dspace_id = H5Dget_space(dset_id);
    REQUIRE(dspace_id != H5I_INVALID_HID);
    herr_t status = H5Sselect_hyperslab(dspace_id, H5S_SELECT_SET, &offset,
                                        &stride, &num_elements, NULL);
    REQUIRE(status >= 0);

    RwIds result = {dset_id, dspace_id, memspace_id};

    return result;
  }

  void RwCleanup(RwIds *ids) {
    REQUIRE(H5Sclose(ids->mspace_id) >= 0);
    REQUIRE(H5Sclose(ids->dspace_id) >= 0);
    REQUIRE(H5Dclose(ids->dset_id) >= 0);
  }

  void Read(hid_t hid, const std::string &dset_name, std::vector<f32> &buf,
            hsize_t offset, hsize_t num_bytes) {

    RwIds ids = RwPreamble(hid, dset_name, offset, num_bytes);
    herr_t status = H5Dread(ids.dset_id, H5T_NATIVE_FLOAT, ids.mspace_id,
                            ids.dspace_id, H5P_DEFAULT, buf.data());
    REQUIRE(status >= 0);

    RwCleanup(&ids);
  }

  herr_t WriteDataset(hid_t hid, const std::string &dset_name,
               const std::vector<f32> &data) {
    herr_t result = WriteDataset(hid, dset_name, data.data(), data.size());

    return result;
  }

  herr_t WriteDataset(hid_t hid, const std::string &dset_name,
                      const f32 *data, size_t num_elements) {
    hsize_t dims[1] = {num_elements};
    herr_t result = H5LTmake_dataset_float(hid, dset_name.c_str(), 1, dims,
                                           data);

    return result;
  }

  void WritePartial1d(hid_t hid, const std::string &dset_name,
                        const f32 *data, hsize_t offset, hsize_t num_bytes) {
    RwIds ids = RwPreamble(hid, dset_name, offset, num_bytes);
    herr_t status = H5Dwrite(ids.dset_id, H5T_NATIVE_FLOAT, ids.mspace_id,
                             ids.dspace_id, H5P_DEFAULT, data);
    REQUIRE(status >= 0);

    RwCleanup(&ids);
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

// xoshiro128+ random number generation:
// https://prng.di.unimi.it/xoshiro128plus.c
static u32 random_state[4] = {111, 222, 333, 444};


u32 GenNextRandom() {
  const u32 random = random_state[0] + random_state[3];

  const u32 t = random_state[1] << 9;

  random_state[2] ^= random_state[0];
  random_state[3] ^= random_state[1];
  random_state[1] ^= random_state[2];
  random_state[0] ^= random_state[3];

  random_state[2] ^= t;

  random_state[3] = RotateLeft(random_state[3], 11);

  return random;
}

f32 GenRandom0to1() {
  u32 random_u32 = GenNextRandom();

  f32 result = (random_u32 >> 8) * 0x1.0p-24f;

  return result;
}

void GenHdf5File(std::string fname, size_t dataset_size, size_t num_datasets) {
  size_t total_bytes = dataset_size * num_datasets;
  size_t total_f32s = total_bytes / sizeof(f32);
  size_t f32s_per_dataset = dataset_size / sizeof(f32);
  std::vector<f32> data(total_f32s);

  for (size_t i = 0; i < data.size(); ++i) {
    data[i] = GenRandom0to1();
  }

  VfdApi api;
  f32 *at = data.data();

  hid_t file_id = api.CreatePosix(fname, H5F_ACC_TRUNC);
  Assert(file_id != H5I_INVALID_HID);

  for (size_t i = 0; i < num_datasets; ++i) {
    Assert(api.WriteDataset(file_id, std::to_string(i), at, f32s_per_dataset)
           > -1);
    at += f32s_per_dataset;
  }

  Assert(api.Close(file_id) > -1);
}

}  // namespace hermes::adapter::vfd::test


hermes::adapter::vfd::test::Arguments args;
hermes::adapter::vfd::test::TestInfo info;

using hermes::adapter::vfd::test::GenNextRandom;
using hermes::adapter::vfd::test::GenRandom0to1;

int init(int* argc, char*** argv) {
  MPI_Init(argc, argv);
  info.write_data.resize(args.request_size / sizeof(f32));
  for (size_t i = 0; i < info.write_data.size(); ++i) {
    info.write_data[i] = 755.0f;  // GenRandom0to1();
  }
  info.read_data.resize(args.request_size / sizeof(f32));
  for (size_t i = 0; i < info.read_data.size(); ++i) {
    info.read_data[i] = 0.0f;
  }

  fs::path fullpath = args.directory;
  fullpath /= args.filename;
  std::string suffix = std::to_string(getpid()) + args.extension;
  info.new_file = fullpath.string() + "_new_" + suffix;
  info.existing_file = fullpath.string() + "_ext_" + suffix;
  info.new_file_cmp = fullpath.string() + "_new_cmp_" + suffix;
  info.existing_file_cmp = fullpath.string() + "_ext_cmp_" + suffix;

  return 0;
}

int finalize() {
  MPI_Finalize();
  return 0;
}

void CleanupFiles() {
  if (fs::exists(info.new_file)) fs::remove(info.new_file);
  if (fs::exists(info.new_file_cmp)) fs::remove(info.new_file_cmp);
  if (fs::exists(info.existing_file)) fs::remove(info.existing_file);
  if (fs::exists(info.existing_file_cmp)) fs::remove(info.existing_file_cmp);
}

int Pretest() {
  CleanupFiles();

  hermes::adapter::vfd::test::GenHdf5File(info.existing_file, args.request_size,
                                          info.num_iterations);
  info.total_size = fs::file_size(info.existing_file);

  std::string cmd = "cp " + info.existing_file + " " + info.existing_file_cmp;
  int status = system(cmd.c_str());
  REQUIRE(status != -1);
  REQUIRE(info.total_size > 0);

  return 0;
}

void CheckResults(const std::string &file1, const std::string &file2) {
  if (fs::exists(file1) && fs::exists(file2)) {
    std::string h5diff_cmd = "h5diff " + file1 + " " + file2;
    LOG(INFO) << "===Running " << h5diff_cmd;
    int status = system(h5diff_cmd.c_str());
    REQUIRE(status == 0);
  }
}

int Posttest(bool compare_data = true) {
  if (compare_data) {
    unsetenv("LD_PRELOAD");
    unsetenv("HDF5_DRIVER");
    CheckResults(info.new_file, info.new_file_cmp);
    CheckResults(info.existing_file, info.existing_file_cmp);
    setenv("HDF5_DRIVER", "hermes", 1);
  }

  CleanupFiles();

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

using hermes::adapter::vfd::test::VfdApi;

namespace test {

hid_t hermes_hid;
hid_t sec2_hid;
herr_t hermes_herr;
size_t hermes_size_read;

void TestOpen(const std::string &path, unsigned flags, bool create = false) {
  VfdApi api;

  std::string cmp_path;
  if (path == info.new_file) {
    cmp_path = info.new_file_cmp;
  } else {
    cmp_path = info.existing_file_cmp;
  }

  if (create) {
    hermes_hid = api.Create(path, flags);
    sec2_hid = api.CreatePosix(cmp_path, flags);
  } else {
    hermes_hid = api.Open(path, flags);
    sec2_hid = api.OpenPosix(cmp_path, flags);
  }
  bool is_same =
    (sec2_hid != H5I_INVALID_HID && hermes_hid != H5I_INVALID_HID) ||
    (sec2_hid == H5I_INVALID_HID && hermes_hid == H5I_INVALID_HID);

  REQUIRE(is_same);
}

void TestClose() {
  VfdApi api;
  hermes_herr = api.Close(hermes_hid);
  herr_t status = api.Close(sec2_hid);
  REQUIRE(status == hermes_herr);
}

void TestWritePartial1d(const std::string &dset_name, const f32 *data,
                        hsize_t offset, hsize_t num_bytes) {
  VfdApi api;
  api.WritePartial1d(test::hermes_hid, dset_name, data, offset, num_bytes);
  api.WritePartial1d(test::sec2_hid, dset_name, data, offset, num_bytes);
}

void TestWriteDataset(const std::string &dset_name,
                      const std::vector<f32> &data) {
  VfdApi api;
  api.WriteDataset(test::hermes_hid, dset_name, data);
  api.WriteDataset(test::sec2_hid, dset_name, data);
}

void TestRead(const std::string &dset_name, std::vector<f32> &buf,
              hsize_t offset, hsize_t num_bytes) {
  VfdApi api;
  hsize_t num_elements = num_bytes / sizeof(f32);
  api.Read(test::hermes_hid, dset_name, buf, offset, num_bytes);
  std::vector<f32> sec2_read_buf(num_elements, 0.0f);
  api.Read(test::sec2_hid, dset_name, sec2_read_buf, offset, num_bytes);

  REQUIRE(std::equal(buf.begin(), buf.begin() + num_elements,
                     sec2_read_buf.begin()));
}
}  // namespace test

#include "hermes_vfd_basic_test.cc"
