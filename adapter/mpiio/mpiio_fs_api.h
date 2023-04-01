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

#ifndef HERMES_ADAPTER_MPIIO_MPIIO_FS_API_H_
#define HERMES_ADAPTER_MPIIO_MPIIO_FS_API_H_

#include <memory>

#include "adapter/filesystem/filesystem.h"
#include "adapter/filesystem/filesystem_mdm.h"
#include "io_client/mpiio/mpiio_api.h"
#include "io_client/mpiio/mpiio_io_client.h"

namespace hermes::adapter::fs {

/** A class to represent MPI IO seek mode conversion */
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

/** A class to represent POSIX IO file system */
class MpiioFs : public Filesystem {
 public:
  MpiioFs()
      : Filesystem(HERMES_MPIIO_IO_CLIENT,
                   AdapterType::kMpiio) {
    HERMES->RegisterTrait<MpiioIoClient>("mpiio_trait_");
  }

  inline bool IsMpiFpTracked(MPI_File *fh, std::shared_ptr<AdapterStat> &stat) {
    auto mdm = HERMES_FS_METADATA_MANAGER;
    if (fh == nullptr) { return false; }
    File f; f.hermes_mpi_fh_ = (*fh);
    stat = mdm->Find(f);
    return stat != nullptr;
  }

  inline bool IsMpiFpTracked(MPI_File *fh) {
    std::shared_ptr<AdapterStat> stat;
    return IsMpiFpTracked(fh, stat);
  }

  int Read(File &f, AdapterStat &stat, void *ptr, size_t offset, int count,
           MPI_Datatype datatype, MPI_Status *status, FsIoOptions opts) {
    opts.mpi_type_ = datatype;
    IoStatus io_status;
    io_status.mpi_status_ptr_ = status;
    size_t total_size = MpiioIoClient::IoSizeFromCount(count, datatype, opts);
    Filesystem::Read(f, stat, ptr, offset, total_size, io_status, opts);
    return io_status.mpi_ret_;
  }

  int ARead(File &f, AdapterStat &stat, void *ptr, size_t offset, int count,
            MPI_Datatype datatype, MPI_Request *request, FsIoOptions opts) {
    opts.mpi_type_ = datatype;
    IoStatus io_status;
    size_t total_size = MpiioIoClient::IoSizeFromCount(count, datatype, opts);
    Filesystem::ARead(f, stat, ptr, offset, total_size,
                      reinterpret_cast<size_t>(request), io_status, opts);
    return io_status.mpi_ret_;
  }

  int ReadAll(File &f, AdapterStat &stat, void *ptr, size_t offset, int count,
              MPI_Datatype datatype, MPI_Status *status, FsIoOptions opts) {
    opts.mpi_type_ = datatype;
    MPI_Barrier(stat.comm_);
    size_t ret = Read(f, stat, ptr, offset, count, datatype, status, opts);
    MPI_Barrier(stat.comm_);
    return ret;
  }

  int ReadOrdered(File &f, AdapterStat &stat, void *ptr, int count,
                  MPI_Datatype datatype, MPI_Status *status, FsIoOptions opts) {
    opts.mpi_type_ = datatype;

    int total;
    MPI_Scan(&count, &total, 1, MPI_INT, MPI_SUM, stat.comm_);
    MPI_Offset my_offset = total - count;
    size_t ret =
        ReadAll(f, stat, ptr, my_offset, count, datatype, status, opts);
    return ret;
  }

  int Write(File &f, AdapterStat &stat, const void *ptr, size_t offset,
            int count, MPI_Datatype datatype, MPI_Status *status,
            FsIoOptions opts) {
    opts.mpi_type_ = datatype;
    IoStatus io_status;
    io_status.mpi_status_ptr_ = status;
    size_t total_size = MpiioIoClient::IoSizeFromCount(count, datatype, opts);
    Filesystem::Write(f, stat, ptr, offset, total_size, io_status, opts);
    return io_status.mpi_ret_;
  }

  int AWrite(File &f, AdapterStat &stat, const void *ptr, size_t offset,
             int count, MPI_Datatype datatype, MPI_Request *request,
             FsIoOptions opts) {
    opts.mpi_type_ = datatype;
    IoStatus io_status;
    size_t total_size = MpiioIoClient::IoSizeFromCount(count, datatype, opts);
    Filesystem::AWrite(f, stat, ptr, offset, total_size,
                       reinterpret_cast<size_t>(request), io_status, opts);
    return io_status.mpi_ret_;
  }

  int WriteAll(File &f, AdapterStat &stat, const void *ptr, size_t offset,
               int count, MPI_Datatype datatype, MPI_Status *status,
               FsIoOptions opts) {
    opts.mpi_type_ = datatype;
    MPI_Barrier(stat.comm_);
    int ret = Write(f, stat, ptr, offset, count, datatype, status, opts);
    MPI_Barrier(stat.comm_);
    return ret;
  }

  int WriteOrdered(File &f, AdapterStat &stat, const void *ptr, int count,
                   MPI_Datatype datatype,
                   MPI_Status *status, FsIoOptions opts) {
    opts.mpi_type_ = datatype;
    int total;
    MPI_Scan(&count, &total, 1, MPI_INT, MPI_SUM, stat.comm_);
    MPI_Offset my_offset = total - count;
    size_t ret =
        WriteAll(f, stat, ptr, my_offset, count, datatype, status, opts);
    return ret;
  }

  int AWriteOrdered(File &f, AdapterStat &stat, const void *ptr, int count,
                    MPI_Datatype datatype, MPI_Request *request,
                    FsIoOptions opts) {
    HILOG(kDebug, "Starting an asynchronous write")
    auto mdm = HERMES_FS_METADATA_MANAGER;
    auto pool = HERMES_FS_THREAD_POOL;
    HermesRequest *hreq = new HermesRequest();
    auto lambda = [](MpiioFs *fs, File &f, AdapterStat &stat, const void *ptr,
                     int count, MPI_Datatype datatype, MPI_Status *status,
                     FsIoOptions opts) {
      int ret = fs->WriteOrdered(f, stat, ptr, count, datatype, status, opts);
      return static_cast<size_t>(ret);
    };
    auto func = std::bind(lambda, this, f, stat, ptr, count, datatype,
                          &hreq->io_status.mpi_status_, opts);
    hreq->return_future = pool->run(func);
    mdm->request_map.emplace(reinterpret_cast<size_t>(request), hreq);
    return MPI_SUCCESS;
  }

  int Wait(MPI_Request *req, MPI_Status *status) {
    auto mdm = HERMES_FS_METADATA_MANAGER;
    auto real_api = HERMES_MPIIO_API;
    auto iter = mdm->request_map.find(reinterpret_cast<size_t>(req));
    if (iter != mdm->request_map.end()) {
      HermesRequest *hreq = iter->second;
      hreq->return_future.get();
      memcpy(status, hreq->io_status.mpi_status_ptr_, sizeof(MPI_Status));
      mdm->request_map.erase(iter);
      delete (hreq);
      return MPI_SUCCESS;
    }
    return real_api->MPI_Wait(req, status);
  }

  int WaitAll(int count, MPI_Request *req, MPI_Status *status) {
    int ret = 0;
    for (int i = 0; i < count; i++) {
      auto sub_ret = Wait(&req[i], &status[i]);
      if (sub_ret != MPI_SUCCESS) {
        ret = sub_ret;
      }
    }
    return ret;
  }

  int Seek(File &f, AdapterStat &stat, MPI_Offset offset, int whence) {
    Filesystem::Seek(f, stat, MpiioSeekModeConv::Normalize(whence), offset);
    return MPI_SUCCESS;
  }

  int SeekShared(File &f, AdapterStat &stat, MPI_Offset offset, int whence) {
    MPI_Offset sum_offset;
    int sum_whence;
    int comm_participators;
    MPI_Comm_size(stat.comm_, &comm_participators);
    MPI_Allreduce(&offset, &sum_offset, 1, MPI_LONG_LONG_INT, MPI_SUM,
                  stat.comm_);
    MPI_Allreduce(&whence, &sum_whence, 1, MPI_INT, MPI_SUM, stat.comm_);
    if (sum_offset / comm_participators != offset) {
      HELOG(kError, "Same offset should be passed "
            "across the opened file communicator.")
    }
    if (sum_whence / comm_participators != whence) {
      HELOG(kError, "Same whence should be passed "
            "across the opened file communicator.")
    }
    Seek(f, stat, offset, whence);
    return 0;
  }

  //////////////////////////
  /// NO OFFSET PARAM
  //////////////////////////

  int Read(File &f, AdapterStat &stat, void *ptr, int count,
           MPI_Datatype datatype, MPI_Status *status) {
    FsIoOptions opts = FsIoOptions::DataType(datatype, true);
    return Read(f, stat, ptr, Tell(f, stat), count, datatype, status, opts);
  }

  int ARead(File &f, AdapterStat &stat, void *ptr, int count,
            MPI_Datatype datatype, MPI_Request *request) {
    FsIoOptions opts = FsIoOptions::DataType(datatype, true);
    return ARead(f, stat, ptr, Tell(f, stat), count, datatype, request, opts);
  }

  int ReadAll(File &f, AdapterStat &stat, void *ptr, int count,
              MPI_Datatype datatype, MPI_Status *status) {
    FsIoOptions opts = FsIoOptions::DataType(datatype, true);
    return ReadAll(f, stat, ptr, Tell(f, stat), count, datatype, status, opts);
  }

  int Write(File &f, AdapterStat &stat, const void *ptr, int count,
            MPI_Datatype datatype, MPI_Status *status) {
    FsIoOptions opts = FsIoOptions::DataType(datatype, true);
    return Write(f, stat, ptr, Tell(f, stat), count, datatype, status, opts);
  }

  int AWrite(File &f, AdapterStat &stat, const void *ptr, int count,
             MPI_Datatype datatype, MPI_Request *request) {
    FsIoOptions opts = FsIoOptions::DataType(datatype, true);
    return AWrite(f, stat, ptr, Tell(f, stat), count, datatype, request, opts);
  }

  int WriteAll(File &f, AdapterStat &stat, const void *ptr, int count,
               MPI_Datatype datatype, MPI_Status *status) {
    FsIoOptions opts = FsIoOptions::DataType(datatype, true);
    return WriteAll(f, stat, ptr, Tell(f, stat), count, datatype, status, opts);
  }

  //////////////////////////
  /// NO STAT PARAM
  //////////////////////////

  int Read(File &f, bool &stat_exists, void *ptr, size_t offset, int count,
           MPI_Datatype datatype, MPI_Status *status) {
    auto mdm = HERMES_FS_METADATA_MANAGER;
    auto stat = mdm->Find(f);
    if (!stat) {
      stat_exists = false;
      return -1;
    }
    stat_exists = true;
    FsIoOptions opts = FsIoOptions::DataType(datatype, false);
    return Read(f, *stat, ptr, offset, count, datatype, status, opts);
  }

  int ARead(File &f, bool &stat_exists, void *ptr, size_t offset, int count,
            MPI_Datatype datatype, MPI_Request *request) {
    auto mdm = HERMES_FS_METADATA_MANAGER;
    auto stat = mdm->Find(f);
    if (!stat) {
      stat_exists = false;
      return -1;
    }
    stat_exists = true;
    FsIoOptions opts = FsIoOptions::DataType(datatype, false);
    return ARead(f, *stat, ptr, offset, count, datatype, request, opts);
  }

  int ReadAll(File &f, bool &stat_exists, void *ptr, size_t offset, int count,
              MPI_Datatype datatype, MPI_Status *status) {
    auto mdm = HERMES_FS_METADATA_MANAGER;
    auto stat = mdm->Find(f);
    if (!stat) {
      stat_exists = false;
      return -1;
    }
    stat_exists = true;
    FsIoOptions opts = FsIoOptions::DataType(datatype, false);
    return ReadAll(f, *stat, ptr, offset, count, datatype, status, opts);
  }

  int ReadOrdered(File &f, bool &stat_exists, void *ptr, int count,
                  MPI_Datatype datatype, MPI_Status *status) {
    auto mdm = HERMES_FS_METADATA_MANAGER;
    auto stat = mdm->Find(f);
    if (!stat) {
      stat_exists = false;
      return -1;
    }
    stat_exists = true;
    FsIoOptions opts = FsIoOptions::DataType(datatype, false);
    return ReadOrdered(f, *stat, ptr, count, datatype, status, opts);
  }

  int Write(File &f, bool &stat_exists, const void *ptr, size_t offset,
            int count, MPI_Datatype datatype, MPI_Status *status) {
    auto mdm = HERMES_FS_METADATA_MANAGER;
    auto stat = mdm->Find(f);
    if (!stat) {
      stat_exists = false;
      return -1;
    }
    stat_exists = true;
    FsIoOptions opts = FsIoOptions::DataType(datatype, false);
    return Write(f, *stat, ptr, offset, count, datatype, status, opts);
  }

  int AWrite(File &f, bool &stat_exists, const void *ptr, size_t offset,
             int count, MPI_Datatype datatype, MPI_Request *request) {
    auto mdm = HERMES_FS_METADATA_MANAGER;
    auto stat = mdm->Find(f);
    if (!stat) {
      stat_exists = false;
      return -1;
    }
    stat_exists = true;
    FsIoOptions opts = FsIoOptions::DataType(datatype, false);
    return AWrite(f, *stat, ptr, offset, count, datatype, request, opts);
  }

  int WriteAll(File &f, bool &stat_exists, const void *ptr, size_t offset,
               int count, MPI_Datatype datatype, MPI_Status *status) {
    auto mdm = HERMES_FS_METADATA_MANAGER;
    auto stat = mdm->Find(f);
    if (!stat) {
      stat_exists = false;
      return -1;
    }
    stat_exists = true;
    FsIoOptions opts = FsIoOptions::DataType(datatype, false);
    return WriteAll(f, *stat, ptr, offset, count, datatype, status, opts);
  }

  int WriteOrdered(File &f, bool &stat_exists, const void *ptr, int count,
                   MPI_Datatype datatype, MPI_Status *status) {
    auto mdm = HERMES_FS_METADATA_MANAGER;
    auto stat = mdm->Find(f);
    if (!stat) {
      stat_exists = false;
      return -1;
    }
    stat_exists = true;
    FsIoOptions opts = FsIoOptions::DataType(datatype, false);
    return WriteOrdered(f, *stat, ptr, count, datatype, status, opts);
  }

  int AWriteOrdered(File &f, bool &stat_exists, const void *ptr, int count,
                    MPI_Datatype datatype, MPI_Request *request) {
    auto mdm = HERMES_FS_METADATA_MANAGER;
    auto stat = mdm->Find(f);
    if (!stat) {
      stat_exists = false;
      return -1;
    }
    stat_exists = true;
    FsIoOptions opts = FsIoOptions::DataType(datatype, false);
    return AWriteOrdered(f, *stat, ptr, count, datatype, request, opts);
  }

  int Read(File &f, bool &stat_exists, void *ptr, int count,
           MPI_Datatype datatype, MPI_Status *status) {
    auto mdm = HERMES_FS_METADATA_MANAGER;
    auto stat = mdm->Find(f);
    if (!stat) {
      stat_exists = false;
      return -1;
    }
    stat_exists = true;
    return Read(f, *stat, ptr, count, datatype, status);
  }

  int ARead(File &f, bool &stat_exists, void *ptr, int count,
            MPI_Datatype datatype, MPI_Request *request) {
    auto mdm = HERMES_FS_METADATA_MANAGER;
    auto stat = mdm->Find(f);
    if (!stat) {
      stat_exists = false;
      return -1;
    }
    stat_exists = true;
    return ARead(f, *stat, ptr, count, datatype, request);
  }

  int ReadAll(File &f, bool &stat_exists, void *ptr, int count,
              MPI_Datatype datatype, MPI_Status *status) {
    auto mdm = HERMES_FS_METADATA_MANAGER;
    auto stat = mdm->Find(f);
    if (!stat) {
      stat_exists = false;
      return -1;
    }
    stat_exists = true;
    return ReadAll(f, *stat, ptr, count, datatype, status);
  }

  int Write(File &f, bool &stat_exists, const void *ptr, int count,
            MPI_Datatype datatype, MPI_Status *status) {
    auto mdm = HERMES_FS_METADATA_MANAGER;
    auto stat = mdm->Find(f);
    if (!stat) {
      stat_exists = false;
      return -1;
    }
    stat_exists = true;
    return Write(f, *stat, ptr, count, datatype, status);
  }

  int AWrite(File &f, bool &stat_exists, const void *ptr, int count,
             MPI_Datatype datatype, MPI_Request *request) {
    auto mdm = HERMES_FS_METADATA_MANAGER;
    auto stat = mdm->Find(f);
    if (!stat) {
      stat_exists = false;
      return -1;
    }
    stat_exists = true;
    return AWrite(f, *stat, ptr, count, datatype, request);
  }

  int WriteAll(File &f, bool &stat_exists, const void *ptr, int count,
               MPI_Datatype datatype, MPI_Status *status) {
    auto mdm = HERMES_FS_METADATA_MANAGER;
    auto stat = mdm->Find(f);
    if (!stat) {
      stat_exists = false;
      return -1;
    }
    stat_exists = true;
    return WriteAll(f, *stat, ptr, count, datatype, status);
  }

  int Seek(File &f, bool &stat_exists, MPI_Offset offset, int whence) {
    auto mdm = HERMES_FS_METADATA_MANAGER;
    auto stat = mdm->Find(f);
    if (!stat) {
      stat_exists = false;
      return -1;
    }
    stat_exists = true;
    return Seek(f, *stat, offset, whence);
  }

  int SeekShared(File &f, bool &stat_exists, MPI_Offset offset, int whence) {
    auto mdm = HERMES_FS_METADATA_MANAGER;
    auto stat = mdm->Find(f);
    if (!stat) {
      stat_exists = false;
      return -1;
    }
    stat_exists = true;
    return SeekShared(f, *stat, offset, whence);
  }
};

}  // namespace hermes::adapter::fs

/** Simplify access to the stateless StdioFs Singleton */
#define HERMES_MPIIO_FS \
  hshm::EasySingleton<hermes::adapter::fs::MpiioFs>::GetInstance()
#define HERMES_STDIO_FS_T hermes::adapter::fs::MpiioFs*

#endif  // HERMES_ADAPTER_MPIIO_MPIIO_FS_API_H_
