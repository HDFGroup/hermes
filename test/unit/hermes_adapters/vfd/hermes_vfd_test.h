//
// Created by lukemartinlogan on 11/3/23.
//

#ifndef HERMES_TEST_UNIT_HERMES_ADAPTERS_HDF5_VFD_TESTS_H_
#define HERMES_TEST_UNIT_HERMES_ADAPTERS_HDF5_VFD_TESTS_H_

#include "filesystem_tests.h"
#include <hdf5.h>
#include <fcntl.h>
#include <stdarg.h>
#include <unistd.h>

#include <filesystem>
#include <iostream>
#include <stdio.h>

#include <cmath>
#include <cstdio>
#include <string>
#include <basic_test.h>

#include "hermes_shm/util/singleton.h"
#include "hermes/hermes.h"

#define CATCH_CONFIG_RUNNER
#include <mpi.h>
#include <catch2/catch_all.hpp>

namespace hermes::adapter::test {

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

template<bool WITH_MPI>
class Hdf5VfdTests : public FilesystemTests<f32> {
 public:
  FileInfo new_file_;
  FileInfo existing_file_;
  FileInfo shared_new_file_;
  FileInfo shared_existing_file_;
  // int offset_seed = 1;
  // unsigned int rs_seed = 1;
  // unsigned int temporal_interval_seed = 5;
  // size_t stride_size = 512;
  unsigned int temporal_interval_ms_ = 1; /**< interval in milliseconds */
  // size_t small_min = 1;
  // size_t small_max = KILOBYTES(4);
  // size_t medium_min = KILOBYTES(4) + 1;
  // size_t medium_max = KILOBYTES(256);
  // size_t large_min = KILOBYTES(256) + 1;
  // size_t large_max = MEGABYTES(3);

  hid_t hermes_hid_;               /**< Hermes handle ID */
  hid_t sec2_hid_;                 /**< POSIX driver handle ID */
  herr_t hermes_herr_;             /**< Hermes error return value */

 public:
  void RegisterFiles() override {
    RegisterPath("new", 0, new_file_);
    RegisterPath("ext", TEST_DO_CREATE, existing_file_);
    if constexpr(WITH_MPI) {
      RegisterPath("shared_new", TEST_FILE_SHARED, shared_new_file_);
      RegisterPath("shared_ext", TEST_DO_CREATE | TEST_FILE_SHARED,
                   shared_existing_file_);
    }
  }

  void CompareFiles(FileInfo &info) override {
    const char *driver = getenv("HDF5_DRIVER");
    if (driver != nullptr) {
      unsetenv("HDF5_DRIVER");
    }
    std::string h5diff_cmd = "h5diff " + info.hermes_ + " " + info.cmp_;
    int status = system(h5diff_cmd.c_str());
    if (status != 0) {
      HELOG(kError, "Failing h5diff command: {}", h5diff_cmd)
    }
    if (driver != nullptr) {
      setenv("HDF5_DRIVER", driver, 1);
    }
    REQUIRE(status == 0);
  }

  std::vector<f32> GenerateData() override {
    std::vector<f32> data(request_size_ * num_iterations_);
    for (size_t i = 0; i < data.size(); ++i) {
      data[i] = GenRandom0to1();
    }
    return data;
  }

  /**
   * Create an HDF5 file called @p fname with @p num_datasets datasets, each with
   * @p num_dataset_elems elements.
   * */
  void CreateFile(const std::string &path,
                  std::vector<f32> &data) override {
    Hdf5Api api;
    f32 *at = (f32*)data.data();
    hid_t file_id = api.CreatePosix(path, H5F_ACC_TRUNC);
    REQUIRE(file_id != H5I_INVALID_HID);
    for (size_t i = 0; i < num_iterations_; ++i) {
      api.MakeDataset(file_id, std::to_string(i), at, request_size_);
      at += request_size_;
    }
    REQUIRE(api.Close(file_id) > -1);
  }

 public:
  /**
   * Test creating and opening a new file.
   * */
  void TestOpen(FileInfo &info, unsigned flags, bool create = false) {
    Hdf5Api api;
    if (create) {
      hermes_hid_ = api.Create(info.hermes_, flags);
      sec2_hid_ = api.CreatePosix(info.cmp_, flags);
    } else {
      hermes_hid_ = api.Open(info.hermes_, flags);
      sec2_hid_ = api.OpenPosix(info.cmp_, flags);
    }
    bool is_same =
        (sec2_hid_ != H5I_INVALID_HID && hermes_hid_ != H5I_INVALID_HID) ||
            (sec2_hid_ == H5I_INVALID_HID && hermes_hid_ == H5I_INVALID_HID);

    REQUIRE(is_same);
  }
  /**
   * @return Test Close() calls
   * */
  void TestClose() {
    Hdf5Api api;
    hermes_herr_ = api.Close(hermes_hid_);
    herr_t status = api.Close(sec2_hid_);
    REQUIRE(status == hermes_herr_);
  }

  /**
   * Test writing partial 1-D dataset.
   * */
  void TestWritePartial1d(const std::string &dset_name, const f32 *data,
                          hsize_t offset, hsize_t nelems) {
    Hdf5Api api;
    api.WritePartial1d(hermes_hid_, dset_name, data, offset, nelems);
    api.WritePartial1d(sec2_hid_, dset_name, data, offset, nelems);
  }

  /**
   * Test making dataset.
   * */
  void TestWriteDataset(const std::string &dset_name,
                        const std::vector<f32> &data) {
    Hdf5Api api;
    api.MakeDataset(hermes_hid_, dset_name, data);
    api.MakeDataset(sec2_hid_, dset_name, data);
  }


  /**
   * Test making compact dataset.
   * */
  void TestMakeCompactDataset(const std::string &dset_name,
                              const std::vector<f32> &data) {
    Hdf5Api api;
    api.MakeDataset(hermes_hid_, dset_name, data, true);
    api.MakeDataset(sec2_hid_, dset_name, data, true);
  }

  /**
   * Test reading dataset.
   * */
  void TestRead(const std::string &dset_name, std::vector<f32> &buf,
                hsize_t offset, hsize_t nelems) {
    Hdf5Api api;
    api.Read(hermes_hid_, dset_name, buf, offset, nelems);
    std::vector<f32> sec2_read_buf(nelems, 0.0f);
    api.Read(sec2_hid_, dset_name, sec2_read_buf, offset, nelems);
    REQUIRE(std::equal(buf.begin(), buf.begin() + nelems,
                       sec2_read_buf.begin()));
  }
};

}  // namespace hermes::adapter::test

#define TEST_INFO \
  hshm::EasySingleton<hermes::adapter::test::Hdf5VfdTests<true>>::GetInstance()

#endif  // HERMES_TEST_UNIT_HERMES_ADAPTERS_HDF5_VFD_TESTS_H_
