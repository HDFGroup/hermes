//
// Created by lukemartinlogan on 10/3/22.
//

#ifndef HERMES_ADAPTER_MPIIO_FS_API_H_
#define HERMES_ADAPTER_MPIIO_FS_API_H_

#include <mpi.h>
#include <mpio.h>
#include <memory>

#include <fcntl.h>
#include <stdarg.h>
#include <unistd.h>
#include <experimental/filesystem>
#include <glog/logging.h>

#include "filesystem/filesystem.h"
#include "filesystem/filesystem.cc"
#include "filesystem/metadata_manager.h"
#include "filesystem/metadata_manager.cc"
#include "real_api.h"

namespace hermes::adapter::mpiio {

using hermes::adapter::fs::AdapterStat;
using hermes::adapter::fs::File;
using hermes::adapter::Singleton;
using hermes::adapter::mpiio::API;
using hermes::adapter::fs::IoOptions;
using hermes::adapter::fs::MetadataManager;
using hermes::adapter::fs::SeekMode;

class MpiioSeekModeConv {
 public:
  static SeekMode Normalize(int mpi_seek) {
    switch(mpi_seek) {
      case MPI_SEEK_SET: return SeekMode::kSet;
      case MPI_SEEK_CUR: return SeekMode::kCurrent;
      case MPI_SEEK_END: return SeekMode::kEnd;
      default: return SeekMode::kNone;
    }
  }
};

class MpiioFS : public hermes::adapter::fs::Filesystem {
 private:
  API *real_api;

 public:
  MpiioFS() {
    real_api = Singleton<API>::GetInstance();
  }
  ~MpiioFS() = default;

  void _InitFile(File &f) override;


 public:
  size_t Read(File &f, AdapterStat &stat,
              void *ptr, size_t offset,
              int count, MPI_Datatype datatype,
              MPI_Status *status, IoOptions opts = IoOptions());
  int ARead(File &f, AdapterStat &stat,
            void *ptr, size_t offset,
            int count, MPI_Datatype datatype,
            MPI_Request *request, IoOptions opts = IoOptions());
  size_t ReadAll(File &f, AdapterStat &stat,
                 void *ptr, size_t offset,
                 int count, MPI_Datatype datatype,
                 MPI_Status *status, IoOptions opts = IoOptions());
  size_t ReadOrdered(File &f, AdapterStat &stat,
                     void *ptr, int count,
                     MPI_Datatype datatype,
                     MPI_Status *status, IoOptions opts = IoOptions());

  size_t Write(File &f, AdapterStat &stat,
               const void *ptr, size_t offset,
               int count, MPI_Datatype datatype,
               MPI_Status *status, IoOptions opts = IoOptions());
  int AWrite(File &f, AdapterStat &stat,
             const void *ptr, size_t offset,
             int count, MPI_Datatype datatype, MPI_Request *request, IoOptions opts = IoOptions());
  size_t WriteAll(File &f, AdapterStat &stat,
                  const void *ptr, size_t offset,
                  int count, MPI_Datatype datatype,
                  MPI_Status *status, IoOptions opts = IoOptions());
  size_t WriteOrdered(File &f, AdapterStat &stat,
                      const void *ptr, int count,
                      MPI_Datatype datatype,
                      MPI_Status *status, IoOptions opts = IoOptions());
  
  int Wait(MPI_Request *req, MPI_Status *status);
  int WaitAll(int count, MPI_Request *req, MPI_Status *status);
  int SeekShared(File &f, AdapterStat &stat, MPI_Offset offset, int whence);
  
  /**
   * Variants which internally find the correct offset
   * */
   
 public:
  size_t Read(File &f, AdapterStat &stat,
              void *ptr, int count, MPI_Datatype datatype,
              MPI_Status *status);
  int ARead(File &f, AdapterStat &stat,
            void *ptr, int count, MPI_Datatype datatype, MPI_Request *request);
  size_t ReadAll(File &f, AdapterStat &stat,
                 void *ptr, int count, MPI_Datatype datatype,
                 MPI_Status *status);

  size_t Write(File &f, AdapterStat &stat,
               const void *ptr, int count, MPI_Datatype datatype,
               MPI_Status *status);
  int AWrite(File &f, AdapterStat &stat,
             const void *ptr, int count, MPI_Datatype datatype, MPI_Request *request);
  size_t WriteAll(File &f, AdapterStat &stat,
                  const void *ptr, int count, MPI_Datatype datatype,
                  MPI_Status *status);

  /**
   * Variants which retrieve the stat data structure internally
   * */

 public:
  size_t Read(File &f, bool &stat_exists,
              void *ptr, size_t offset,
              int count, MPI_Datatype datatype,
              MPI_Status *status);
  int ARead(File &f, bool &stat_exists,
            void *ptr, size_t offset,
            int count, MPI_Datatype datatype, MPI_Request *request);
  size_t ReadAll(File &f, bool &stat_exists,
                 void *ptr, size_t offset,
                 int count, MPI_Datatype datatype,
                 MPI_Status *status);
  size_t ReadOrdered(File &f, bool &stat_exists,
                     void *ptr, int count,
                     MPI_Datatype datatype,
                     MPI_Status *status);

  size_t Write(File &f, bool &stat_exists,
               const void *ptr, size_t offset,
               int count, MPI_Datatype datatype,
               MPI_Status *status);
  int AWrite(File &f, bool &stat_exists,
             const void *ptr, size_t offset,
             int count, MPI_Datatype datatype, MPI_Request *request);
  size_t WriteAll(File &f, bool &stat_exists,
                  const void *ptr, size_t offset,
                  int count, MPI_Datatype datatype,
                  MPI_Status *status);
  size_t WriteOrdered(File &f, bool &stat_exists,
                      const void *ptr, int count,
                      MPI_Datatype datatype,
                      MPI_Status *status);

  size_t Read(File &f, bool &stat_exists,
              void *ptr, int count, MPI_Datatype datatype,
              MPI_Status *status);
  int ARead(File &f, bool &stat_exists,
            void *ptr, int count, MPI_Datatype datatype, MPI_Request *request);
  size_t ReadAll(File &f, bool &stat_exists,
                 void *ptr, int count, MPI_Datatype datatype,
                 MPI_Status *status);

  size_t Write(File &f, bool &stat_exists,
               const void *ptr, int count, MPI_Datatype datatype,
               MPI_Status *status);
  int AWrite(File &f, bool &stat_exists,
             const void *ptr, int count, MPI_Datatype datatype, MPI_Request *request);
  size_t WriteAll(File &f, bool &stat_exists,
                  const void *ptr, int count, MPI_Datatype datatype,
                  MPI_Status *status);

  int SeekShared(File &f, bool &stat_exists, MPI_Offset offset, int whence);


  /**
   * Internal overrides
   * */

 private:
  void _OpenInitStats(File &f, AdapterStat &stat, bool bucket_exists) override;
  File _RealOpen(AdapterStat &stat, const std::string &path) override;
  size_t _RealWrite(const std::string &filename, off_t offset, size_t size,
                    const u8 *data_ptr, IoOptions &opts) override;
  size_t _RealRead(const std::string &filename, off_t offset, size_t size,
                   u8 *data_ptr, IoOptions &opts) override;
  int _RealSync(File &f) override;
  int _RealClose(File &f) override;
};

}  // namespace hermes::adapter::mpiio

#endif  // HERMES_ADAPTER_MPIIO_FS_API_H_
