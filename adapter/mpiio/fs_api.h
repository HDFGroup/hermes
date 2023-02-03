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

#ifndef HERMES_ADAPTER_MPIIO_FS_API_H_
#define HERMES_ADAPTER_MPIIO_FS_API_H_

#include <fcntl.h>
#include <glog/logging.h>
#include <mpi.h>
#include <mpio.h>
#include <stdarg.h>
#include <unistd.h>

#include <filesystem>
#include <memory>

#include "real_api.h"

namespace hermes::adapter::mpiio {

using hermes::Singleton;
using hermes::adapter::fs::AdapterStat;
using hermes::adapter::fs::File;
using hermes::adapter::fs::HermesRequest;
using hermes::adapter::fs::IoOptions;
using hermes::adapter::fs::IoStatus;
using hermes::adapter::fs::MetadataManager;
using hermes::adapter::fs::SeekMode;
using hermes::adapter::mpiio::API;

/**
   A class to represent MPI IO seek mode conversion
*/
class MpiioSeekModeConv {
 public:
  /** normalize \a mpi_seek MPI seek mode */
  static SeekMode Normalize(int mpi_seek) {
    switch (mpi_seek) {
      case MPI_SEEK_SET:
        return SeekMode::kSet;
      case MPI_SEEK_CUR:
        return SeekMode::kCurrent;
      case MPI_SEEK_END:
        return SeekMode::kEnd;
      default:
        return SeekMode::kNone;
    }
  }
};

/**
   A class to represent MPI IO file system
*/
class MpiioFS : public hermes::adapter::fs::Filesystem {
 private:
  API *real_api;                          /**< pointer to real APIs */
  hermes::adapter::fs::API *posix_api; /**< pointer to POSIX APIs */

 public:
  MpiioFS() {
    real_api = Singleton<API>::GetInstance();
    posix_api = Singleton<hermes::adapter::fs::API>::GetInstance();
  }
  ~MpiioFS() = default;

  void InitFile(File &f) override;

 public:
  /** get file name from \a fh MPI file pointer */
  static inline std::string GetFilenameFromFP(MPI_File *fh) {
    MPI_Info info;
    int status = MPI_File_get_info(*fh, &info);
    if (status != MPI_SUCCESS) {
      LOG(ERROR) << "MPI_File_get_info on file handler failed." << std::endl;
    }
    const int kMaxSize = 0xFFF;
    int flag;
    char filename[kMaxSize] = {0};
    MPI_Info_get(info, "filename", kMaxSize, filename, &flag);
    return filename;
  }
  /** Read */
  int Read(File &f, AdapterStat &stat, void *ptr, size_t offset, int count,
           MPI_Datatype datatype, MPI_Status *status,
           IoOptions opts = IoOptions());
  /** ARead */
  int ARead(File &f, AdapterStat &stat, void *ptr, size_t offset, int count,
            MPI_Datatype datatype, MPI_Request *request,
            IoOptions opts = IoOptions());
  /** ReadAll */
  int ReadAll(File &f, AdapterStat &stat, void *ptr, size_t offset, int count,
              MPI_Datatype datatype, MPI_Status *status,
              IoOptions opts = IoOptions());
  /** ReadOrdered */
  int ReadOrdered(File &f, AdapterStat &stat, void *ptr, int count,
                  MPI_Datatype datatype, MPI_Status *status,
                  IoOptions opts = IoOptions());
  /** Write */
  int Write(File &f, AdapterStat &stat, const void *ptr, size_t offset,
            int count, MPI_Datatype datatype, MPI_Status *status,
            IoOptions opts = IoOptions());
  /** AWrite */
  int AWrite(File &f, AdapterStat &stat, const void *ptr, size_t offset,
             int count, MPI_Datatype datatype, MPI_Request *request,
             IoOptions opts = IoOptions());
  /** WriteAll */
  int WriteAll(File &f, AdapterStat &stat, const void *ptr, size_t offset,
               int count, MPI_Datatype datatype, MPI_Status *status,
               IoOptions opts = IoOptions());
  /** WriteOrdered */
  int WriteOrdered(File &f, AdapterStat &stat, const void *ptr, int count,
                   MPI_Datatype datatype, MPI_Status *status,
                   IoOptions opts = IoOptions());
  /** AWriteOrdered */
  int AWriteOrdered(File &f, AdapterStat &stat, const void *ptr, int count,
                    MPI_Datatype datatype, MPI_Request *request,
                    IoOptions opts = IoOptions());
  /** Wait */
  int Wait(MPI_Request *req, MPI_Status *status);
  /** WaitAll */
  int WaitAll(int count, MPI_Request *req, MPI_Status *status);
  /** Seek */
  int Seek(File &f, AdapterStat &stat, MPI_Offset offset, int whence);
  /** SeekShared */
  int SeekShared(File &f, AdapterStat &stat, MPI_Offset offset, int whence);

  /**
   * Variants which internally find the correct offset
   */

 public:
  /** Read */
  int Read(File &f, AdapterStat &stat, void *ptr, int count,
           MPI_Datatype datatype, MPI_Status *status);
  /** ARead */
  int ARead(File &f, AdapterStat &stat, void *ptr, int count,
            MPI_Datatype datatype, MPI_Request *request);
  /** ReadAll */
  int ReadAll(File &f, AdapterStat &stat, void *ptr, int count,
              MPI_Datatype datatype, MPI_Status *status);
  /** Write */
  int Write(File &f, AdapterStat &stat, const void *ptr, int count,
            MPI_Datatype datatype, MPI_Status *status);
  /** AWrite */
  int AWrite(File &f, AdapterStat &stat, const void *ptr, int count,
             MPI_Datatype datatype, MPI_Request *request);
  /** WriteAll */
  int WriteAll(File &f, AdapterStat &stat, const void *ptr, int count,
               MPI_Datatype datatype, MPI_Status *status);

  /**
   * Variants which retrieve the stat data structure internally
   */

 public:
  /** Read */
  int Read(File &f, bool &stat_exists, void *ptr, size_t offset, int count,
           MPI_Datatype datatype, MPI_Status *status);
  /** ARead */
  int ARead(File &f, bool &stat_exists, void *ptr, size_t offset, int count,
            MPI_Datatype datatype, MPI_Request *request);
  /** ReadAll */
  int ReadAll(File &f, bool &stat_exists, void *ptr, size_t offset, int count,
              MPI_Datatype datatype, MPI_Status *status);
  /** ReadOrdered */
  int ReadOrdered(File &f, bool &stat_exists, void *ptr, int count,
                  MPI_Datatype datatype, MPI_Status *status);
  /** Write */
  int Write(File &f, bool &stat_exists, const void *ptr, size_t offset,
            int count, MPI_Datatype datatype, MPI_Status *status);
  /** AWrite */
  int AWrite(File &f, bool &stat_exists, const void *ptr, size_t offset,
             int count, MPI_Datatype datatype, MPI_Request *request);
  /** WriteAll */
  int WriteAll(File &f, bool &stat_exists, const void *ptr, size_t offset,
               int count, MPI_Datatype datatype, MPI_Status *status);
  /** WriteOrdered */
  int WriteOrdered(File &f, bool &stat_exists, const void *ptr, int count,
                   MPI_Datatype datatype, MPI_Status *status);
  /** AWriteOrdered */
  int AWriteOrdered(File &f, bool &stat_exists, const void *ptr, int count,
                    MPI_Datatype datatype, MPI_Request *request);
  /** Read */
  int Read(File &f, bool &stat_exists, void *ptr, int count,
           MPI_Datatype datatype, MPI_Status *status);
  /** ARead */
  int ARead(File &f, bool &stat_exists, void *ptr, int count,
            MPI_Datatype datatype, MPI_Request *request);
  /** ReadAll */
  int ReadAll(File &f, bool &stat_exists, void *ptr, int count,
              MPI_Datatype datatype, MPI_Status *status);
  /** Write */
  int Write(File &f, bool &stat_exists, const void *ptr, int count,
            MPI_Datatype datatype, MPI_Status *status);
  /** AWrite */
  int AWrite(File &f, bool &stat_exists, const void *ptr, int count,
             MPI_Datatype datatype, MPI_Request *request);
  /** WriteAll */
  int WriteAll(File &f, bool &stat_exists, const void *ptr, int count,
               MPI_Datatype datatype, MPI_Status *status);
  /** Seek */
  int Seek(File &f, bool &stat_exists, MPI_Offset offset, int whence);
  /** SeekShared */
  int SeekShared(File &f, bool &stat_exists, MPI_Offset offset, int whence);

  /**
   * Internal overrides
   */

 private:
  /** OpenInitStat */
  void OpenInitStat(File &f, AdapterStat &stat) override;
  /** RealOpen */
  File RealOpen(AdapterStat &stat, const std::string &path) override;
  /** RealWrite */
  size_t _RealWrite(const std::string &filename, off_t offset, size_t size,
                    const u8 *data_ptr, IoStatus &io_status,
                    IoOptions &opts) override;
  /** RealRead */
  size_t _RealRead(const std::string &filename, off_t offset, size_t size,
                   u8 *data_ptr, IoStatus &io_status, IoOptions &opts) override;
  /** IoStats */
  void _IoStats(size_t count, IoStatus &io_status, IoOptions &opts) override;
  /** RealSync */
  int RealSync(File &f) override;
  /** RealClose */
  int RealClose(File &f) override;
};

}  // namespace hermes::adapter::mpiio

#endif  // HERMES_ADAPTER_MPIIO_FS_API_H_
