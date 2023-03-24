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

#include "hermes.h"
#include "hermes_types.h"
#include "adapter_test_utils.h"
#include "catch_config.h"

using hermes::f32;
using hermes::u32;

namespace hermes::adapter::vfd::test {

/**
   A structure to represent test arguments
*/
struct Arguments {
  std::string filename = "test";  /**< test file name */
  std::string directory = "/tmp/test_hermes"; /**< test directory name */
  size_t request_size = 65536;    /**< test request size */
};

/**
   A structure to represent test information
*/
struct TestInfo {
  static const int element_size = sizeof(f32); /**< test element size */

  // int rank = 0;
  int comm_size = 1;            /**< communicator size */
  std::vector<f32> write_data;  /**< test data for writing */
  std::vector<f32> read_data;   /**< test data for reading */
  std::string new_file;         /**< new file name */
  std::string existing_file;    /**< existing file name */
  std::string new_file_cmp;     /**< new file name to compare */
  std::string existing_file_cmp; /**< existing file name to compare */
  std::string hdf5_extension = ".h5"; /**< HDF5 file extention to use */
  size_t num_iterations = 64;         /**< number of iterations */
  // int offset_seed = 1;
  // unsigned int rs_seed = 1;
  // unsigned int temporal_interval_seed = 5;
  size_t total_size;                     /**< total size */
  // size_t stride_size = 512;
  unsigned int temporal_interval_ms = 1; /**< interval in milliseconds */
  // size_t small_min = 1;
  // size_t small_max = KILOBYTES(4);
  // size_t medium_min = KILOBYTES(4) + 1;
  // size_t medium_max = KILOBYTES(256);
  // size_t large_min = KILOBYTES(256) + 1;
  // size_t large_max = MEGABYTES(3);
  size_t nelems_per_dataset;    /**< number of elements per dataset */
  bool scratch_mode = false;    /**< flag for scratch mode */
};

/**
 * Temporarily disable printing of the HDF5 error stack.
 *
 * Some tests intentionally trigger HDF5 errors, and in those cases we don't
 * want to clutter the output with HDF5 error messages.
 */
class MuteHdf5Errors {
  H5E_auto2_t old_func;         /**<  error handler callback function */
  void *old_client_data;        /**<  pointer to client data for old_func */

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

/**
 * HDF5 identifiers required for reads and writes.
 */
struct RwIds {
  hid_t dset_id;                /**< dataset ID */
  hid_t dspace_id;              /**< data space ID */
  hid_t mspace_id;              /**< memory space ID */
};

/**
 * The I/O API for this adapter layer.
 *
 * Ideally we would have a high level Adapter I/O API that each adapter inherits
 * from so that the adapter tests can reuse more code. This is a step in that
 * direction.
 */
struct Hdf5Api {
  /**
   * A file access property list representing the sec2 (POSIX) VFD.
   */
  hid_t sec2_fapl;

  Hdf5Api() : sec2_fapl(H5I_INVALID_HID) {
    sec2_fapl = H5Pcreate(H5P_FILE_ACCESS);
    REQUIRE(sec2_fapl != H5I_INVALID_HID);
    REQUIRE(H5Pset_fapl_sec2(sec2_fapl) >= 0);
  }

  ~Hdf5Api() {
    REQUIRE(H5Pclose(sec2_fapl) >= 0);
    sec2_fapl = H5I_INVALID_HID;
  }

  /**
   * Open an existing file using the default VFD. Since the tests are run with
   * HDF5_DRIVER=hermes, the default VFD will be the Hermes VFD.
   */
  hid_t Open(const std::string &fname, unsigned flags) {
    hid_t result = H5Fopen(fname.c_str(), flags, H5P_DEFAULT);

    return result;
  }

  /**
   * Open an existing file using the POSIX VFD. This will bypass Hermes.
   */
  hid_t OpenPosix(const std::string &fname, unsigned flags) {
    hid_t result = H5Fopen(fname.c_str(), flags, sec2_fapl);

    return result;
  }

  /**
   * Create a file using the default VFD.
   */
  hid_t Create(const std::string &fname, unsigned flags) {
    hid_t result = H5Fcreate(fname.c_str(), flags, H5P_DEFAULT, H5P_DEFAULT);

    return result;
  }

  /**
   * Create a file using the POSIX VFD.
   */
  hid_t CreatePosix(const std::string &fname, unsigned flags) {
    hid_t result = H5Fcreate(fname.c_str(), flags, H5P_DEFAULT, sec2_fapl);

    return result;
  }

  /**
   * Boilerplate necessary before calling H5Dread or H5Dwrite.
   */
  RwIds RwPreamble(hid_t hid, const std::string &dset_name, hsize_t offset,
                   hsize_t nelems, hsize_t stride = 1) {
    hid_t dset_id = H5Dopen2(hid, dset_name.c_str(), H5P_DEFAULT);

    hid_t memspace_id = H5Screate_simple(1, &nelems, NULL);
    REQUIRE(memspace_id != H5I_INVALID_HID);

    if (dset_id == H5I_INVALID_HID) {
      dset_id = H5Dcreate2(hid, dset_name.c_str(), H5T_NATIVE_FLOAT,
                           memspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    }

    REQUIRE(dset_id != H5I_INVALID_HID);
    hid_t dspace_id = H5Dget_space(dset_id);
    REQUIRE(dspace_id != H5I_INVALID_HID);
    herr_t status = H5Sselect_hyperslab(dspace_id, H5S_SELECT_SET, &offset,
                                        &stride, &nelems, NULL);
    REQUIRE(status >= 0);

    RwIds result = {dset_id, dspace_id, memspace_id};

    return result;
  }

  /**
   * Cleanup code required after H5Dread and H5Dwrite.
   */
  void RwCleanup(RwIds *ids) {
    REQUIRE(H5Sclose(ids->mspace_id) >= 0);
    REQUIRE(H5Sclose(ids->dspace_id) >= 0);
    REQUIRE(H5Dclose(ids->dset_id) >= 0);
  }

  /**
   * Reads @p nelems elements from the object represented by @p hid into @p buf,
   * starting at element @p offset.
   */
  void Read(hid_t hid, const std::string &dset_name, std::vector<f32> &buf,
            hsize_t offset, hsize_t nelems) {
    RwIds ids = RwPreamble(hid, dset_name, offset, nelems);
    herr_t status = H5Dread(ids.dset_id, H5T_NATIVE_FLOAT, ids.mspace_id,
                            ids.dspace_id, H5P_DEFAULT, buf.data());
    REQUIRE(status >= 0);

    RwCleanup(&ids);
  }
  /**
    Create a 1-dimensional dataset using \a data vector.
  */
  void MakeDataset(hid_t hid, const std::string &dset_name,
                   const std::vector<f32> &data, bool compact = false) {
    MakeDataset(hid, dset_name, data.data(), data.size(), compact);
  }

  /**
   * Create a 1-dimensional dataset named @p dset_name in object @p hid with @p
   * nelems elements from the array @p data.
   */
  void MakeDataset(hid_t hid, const std::string &dset_name, const f32 *data,
                   hsize_t nelems, bool compact = false) {
    hid_t dcpl = H5P_DEFAULT;
    herr_t status = 0;

    if (compact) {
      REQUIRE(nelems * sizeof(f32) <= KILOBYTES(64));
      dcpl = H5Pcreate(H5P_DATASET_CREATE);
      REQUIRE(dcpl != H5I_INVALID_HID);
      status = H5Pset_layout(dcpl, H5D_COMPACT);
      REQUIRE(status >= 0);
    }

    hid_t memspace_id = H5Screate_simple(1, &nelems, NULL);
    REQUIRE(memspace_id != H5I_INVALID_HID);

    hid_t dset_id = H5Dcreate2(hid, dset_name.c_str(), H5T_NATIVE_FLOAT,
                               memspace_id, H5P_DEFAULT, dcpl, H5P_DEFAULT);
    REQUIRE(dset_id != H5I_INVALID_HID);

    hid_t dspace_id = H5Dget_space(dset_id);
    REQUIRE(dspace_id != H5I_INVALID_HID);

    status = H5Dwrite(dset_id, H5T_NATIVE_FLOAT, memspace_id,
                      dspace_id, H5P_DEFAULT, data);
    REQUIRE(status >= 0);
    REQUIRE(H5Sclose(memspace_id) >= 0);
    REQUIRE(H5Sclose(dspace_id) >= 0);
    REQUIRE(H5Dclose(dset_id) >= 0);

    if (compact) {
      REQUIRE(H5Pclose(dcpl) >= 0);
    }
  }

  /**
   * Write @p nelems elements to the dataset @p dset_name in file @p hid
   * starting at element @p offset. The dataset will be created if it doesn't
   * already exist.
   */
  void WritePartial1d(hid_t hid, const std::string &dset_name,
                        const f32 *data, hsize_t offset, hsize_t nelems) {
    RwIds ids = RwPreamble(hid, dset_name, offset, nelems);
    herr_t status = H5Dwrite(ids.dset_id, H5T_NATIVE_FLOAT, ids.mspace_id,
                             ids.dspace_id, H5P_DEFAULT, data);
    REQUIRE(status >= 0);

    RwCleanup(&ids);
  }
  /**
    Close HDF5 file.
  */
  herr_t Close(hid_t id) {
    herr_t result = H5Fclose(id);

    return result;
  }
};

// xoshiro128+ random number generation. 2x speedup over std::mt19937:
// https://prng.di.unimi.it/xoshiro128plus.c

static inline u32 RotateLeft(const u32 x, int k) {
  u32 result = (x << k) | (x >> (32 - k));

  return result;
}

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

/**
 * Return a random float in the range [0.0f, 1.0f]
 */
f32 GenRandom0to1() {
  u32 random_u32 = GenNextRandom();

  f32 result = (random_u32 >> 8) * 0x1.0p-24f;

  return result;
}

/**
 * Create an HDF5 file called @p fname with @p num_datasets datasets, each with
 * @p num_dataset_elems elements.
 */
void GenHdf5File(std::string fname, size_t num_dataset_elems,
                 size_t num_datasets) {
  std::vector<f32> data(num_dataset_elems * num_datasets);

  for (size_t i = 0; i < data.size(); ++i) {
    data[i] = GenRandom0to1();
  }

  Hdf5Api api;
  f32 *at = data.data();

  hid_t file_id = api.CreatePosix(fname, H5F_ACC_TRUNC);
  REQUIRE(file_id != H5I_INVALID_HID);

  for (size_t i = 0; i < num_datasets; ++i) {
    api.MakeDataset(file_id, std::to_string(i), at, num_dataset_elems);
    at += num_dataset_elems;
  }

  REQUIRE(api.Close(file_id) > -1);
}

}  // namespace hermes::adapter::vfd::test

hermes::adapter::vfd::test::Arguments args;
hermes::adapter::vfd::test::TestInfo info;

using hermes::adapter::vfd::test::GenHdf5File;
using hermes::adapter::vfd::test::GenNextRandom;
using hermes::adapter::vfd::test::GenRandom0to1;

void IgnoreAllFiles() {
  HERMES->client_config_.SetAdapterPathTracking(info.existing_file_cmp, false);
  HERMES->client_config_.SetAdapterPathTracking(info.new_file_cmp, false);
  HERMES->client_config_.SetAdapterPathTracking(info.new_file, false);
  HERMES->client_config_.SetAdapterPathTracking(info.existing_file, false);
}

void TrackFiles() {
  HERMES->client_config_.SetAdapterPathTracking(info.new_file, true);
  HERMES->client_config_.SetAdapterPathTracking(info.existing_file, true);
}

void RemoveFile(const std::string &path) {
  stdfs::remove(path);
  if (stdfs::exists(path)) {
    LOG(FATAL) << "Failed to remove: " << path << std::endl;
  }
}

void RemoveFiles() {
  RemoveFile(info.new_file);
  RemoveFile(info.new_file_cmp);
  RemoveFile(info.existing_file);
  RemoveFile(info.existing_file_cmp);
}

/**
 * Called in the Catch2 main function (see catch_config.h) before any tests are
 * run. Initialize sizes, filenames, and read/write buffers.
 */
int init(int* argc, char*** argv) {
  MPI_Init(argc, argv);

  if (args.request_size % info.element_size != 0) {
    LOG(FATAL) << "request_size must be a multiple of " << info.element_size;
  }
  info.nelems_per_dataset = args.request_size / info.element_size;

  info.write_data.resize(info.nelems_per_dataset);
  for (size_t i = 0; i < info.write_data.size(); ++i) {
    info.write_data[i] = GenRandom0to1();
  }
  info.read_data.resize(info.nelems_per_dataset);
  for (size_t i = 0; i < info.read_data.size(); ++i) {
    info.read_data[i] = 0.0f;
  }

  stdfs::path fullpath = args.directory;
  fullpath /= args.filename;
  std::string suffix = std::to_string(getpid()) + info.hdf5_extension;
  info.new_file = fullpath.string() + "_new_" + suffix;
  info.existing_file = fullpath.string() + "_ext_" + suffix;
  info.new_file_cmp = fullpath.string() + "_new_cmp_" + suffix;
  info.existing_file_cmp = fullpath.string() + "_ext_cmp_" + suffix;
  
  IgnoreAllFiles();
  RemoveFiles();

  char *driver_config = getenv("HDF5_DRIVER_CONFIG");
  if (driver_config) {
    std::string looking_for("false");
    std::string conf_str(driver_config);
    if (!conf_str.compare(0, looking_for.size(), looking_for)) {
      info.scratch_mode = true;
    }
  }

  return 0;
}

/**
 * Called from catch_config.h after all tests are run.
 */
int finalize() {
  MPI_Finalize();
  return 0;
}

/**
 * Called before each individual test.
 *
 * Generates files for tests that operate on existing files.
 */
int Pretest() {
  IgnoreAllFiles();
  RemoveFiles();

  GenHdf5File(info.existing_file, info.nelems_per_dataset, info.num_iterations);
  info.total_size = stdfs::file_size(info.existing_file);
  std::string cmd = "cp " + info.existing_file + " " + info.existing_file_cmp;
  int status = system(cmd.c_str());
  REQUIRE(status != -1);
  REQUIRE(info.total_size > 0);

  TrackFiles();
  return 0;
}

/**
 * Use h5diff to ensure that the resulting files from the Hermes VFD and the
 * POSIX VFD are the same.
 */
void CheckResults(const std::string &file1, const std::string &file2) {
  if (stdfs::exists(file1) && stdfs::exists(file2)) {
    std::string h5diff_cmd = "h5diff " + file1 + " " + file2;
    int status = system(h5diff_cmd.c_str());
    if (status != 0) {
      LOG(ERROR) << "Failing h5diff command: " << h5diff_cmd;
    }
    REQUIRE(status == 0);
  }
}

/**
 * Called after each individual test.
 */
int Posttest() {
  if (!info.scratch_mode) {
    // NOTE(chogan): This is necessary so that h5diff doesn't use the Hermes VFD
    // in CheckResults. We don't need to reset LD_PRELOAD because it only has an
    // effect when an application first starts.
    unsetenv("LD_PRELOAD");
    unsetenv("HDF5_DRIVER");
    CheckResults(info.new_file, info.new_file_cmp);
    CheckResults(info.existing_file, info.existing_file_cmp);
    setenv("HDF5_DRIVER", "hermes", 1);
  }

  RemoveFiles();

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

using hermes::adapter::vfd::test::Hdf5Api;

/**
 * The functions in this namespace perform operations on 2 files: the "main"
 * file, which is the one going through the Hermes VFD, and a comparison file
 * (with a "_cmp" suffix) which goes through the POSIX VFD. The idea is to
 * perfrom each test on 2 files (Hermes VFD and POSIX VFD) and then compare the
 * results at the end with h5diff. In persistent mode, the file produced by the
 * Hermes VFD should be exactly the same as the one produced by the POSIX VFD.
 */
namespace test {

hid_t hermes_hid;               /**< Hermes handle ID */
hid_t sec2_hid;                 /**< POSIX driver handle ID */
herr_t hermes_herr;             /**< Hermes error return value */
/**
   Test creating and opening a new file.
*/
void TestOpen(const std::string &path, unsigned flags, bool create = false) {
  Hdf5Api api;

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
/**
   Test Close() calls.
*/    
void TestClose() {
  Hdf5Api api;
  hermes_herr = api.Close(hermes_hid);
  herr_t status = api.Close(sec2_hid);
  REQUIRE(status == hermes_herr);
}

/**
   Test writing partial 1-D dataset.
*/
void TestWritePartial1d(const std::string &dset_name, const f32 *data,
                        hsize_t offset, hsize_t nelems) {
  Hdf5Api api;
  api.WritePartial1d(test::hermes_hid, dset_name, data, offset, nelems);
  api.WritePartial1d(test::sec2_hid, dset_name, data, offset, nelems);
}

/**
   Test making dataset.
*/  
void TestWriteDataset(const std::string &dset_name,
                      const std::vector<f32> &data) {
  Hdf5Api api;
  api.MakeDataset(test::hermes_hid, dset_name, data);
  api.MakeDataset(test::sec2_hid, dset_name, data);
}


/**
   Test making compact dataset.
*/
void TestMakeCompactDataset(const std::string &dset_name,
                            const std::vector<f32> &data) {
  Hdf5Api api;
  api.MakeDataset(test::hermes_hid, dset_name, data, true);
  api.MakeDataset(test::sec2_hid, dset_name, data, true);
}

/**
   Test reading dataset.
*/  
void TestRead(const std::string &dset_name, std::vector<f32> &buf,
              hsize_t offset, hsize_t nelems) {
  Hdf5Api api;
  api.Read(test::hermes_hid, dset_name, buf, offset, nelems);
  std::vector<f32> sec2_read_buf(nelems, 0.0f);
  api.Read(test::sec2_hid, dset_name, sec2_read_buf, offset, nelems);

  REQUIRE(std::equal(buf.begin(), buf.begin() + nelems, sec2_read_buf.begin()));
}
}  // namespace test

#include "hermes_vfd_basic_test.cc"
