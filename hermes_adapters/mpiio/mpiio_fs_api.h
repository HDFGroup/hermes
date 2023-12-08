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

#include "hermes_adapters/filesystem/filesystem.h"
#include "hermes_adapters/filesystem/filesystem_mdm.h"
#include "mpiio_api.h"

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
  HERMES_MPIIO_API_T real_api_; /**< pointer to real APIs */
  
  MpiioFs() : Filesystem(AdapterType::kMpiio) {
    real_api_ = HERMES_MPIIO_API;
  }

  /** Initialize I/O opts using count + datatype */
  static size_t IoSizeFromCount(int count,
                                MPI_Datatype datatype,
                                FsIoOptions &opts) {
    opts.mpi_type_ = datatype;
    opts.mpi_count_ = count;
    MPI_Type_size(datatype, &opts.type_size_);
    return static_cast<size_t>(count * opts.type_size_);
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
    IoStatus io_status;
    io_status.mpi_status_ptr_ = status;
    size_t total_size = IoSizeFromCount(count, datatype, opts);
    Filesystem::Read(f, stat, ptr, offset, total_size, io_status, opts);
    return io_status.mpi_ret_;
  }

  int ARead(File &f, AdapterStat &stat, void *ptr, size_t offset, int count,
            MPI_Datatype datatype, MPI_Request *request, FsIoOptions opts) {
    auto mdm = HERMES_FS_METADATA_MANAGER;
    IoStatus io_status;
    size_t total_size = IoSizeFromCount(count, datatype, opts);
    FsAsyncTask *fstask = Filesystem::ARead(f, stat, ptr, offset, total_size,
                                            reinterpret_cast<size_t>(request), io_status, opts);
    mdm->EmplaceTask(reinterpret_cast<size_t>(request), fstask);
    return io_status.mpi_ret_;
  }

  int ReadAll(File &f, AdapterStat &stat, void *ptr, size_t offset, int count,
              MPI_Datatype datatype, MPI_Status *status, FsIoOptions opts) {
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
    IoStatus io_status;
    io_status.mpi_status_ptr_ = status;
    size_t total_size = IoSizeFromCount(count, datatype, opts);
    Filesystem::Write(f, stat, ptr, offset, total_size, io_status, opts);
    return io_status.mpi_ret_;
  }

  int AWrite(File &f, AdapterStat &stat, const void *ptr, size_t offset,
             int count, MPI_Datatype datatype, MPI_Request *request,
             FsIoOptions opts) {
    auto mdm = HERMES_FS_METADATA_MANAGER;
    IoStatus io_status;
    size_t total_size = IoSizeFromCount(count, datatype, opts);
    FsAsyncTask *fstask = Filesystem::AWrite(f, stat, ptr, offset, total_size,
                                             reinterpret_cast<size_t>(request), io_status, opts);
    mdm->EmplaceTask(reinterpret_cast<size_t>(request), fstask);
    return io_status.mpi_ret_;
  }

  template<bool ASYNC>
  int BaseWriteAll(File &f, AdapterStat &stat, const void *ptr, size_t offset,
                   int count, MPI_Datatype datatype, MPI_Status *status,
                   MPI_Request *request, FsIoOptions opts) {
    if constexpr(!ASYNC) {
      MPI_Barrier(stat.comm_);
      int ret = Write(f, stat, ptr, offset, count, datatype, status, opts);
      MPI_Barrier(stat.comm_);
      return ret;
    } else {
      return AWrite(f, stat, ptr, offset, count, datatype, request, opts);
    }
  }

  int WriteAll(File &f, AdapterStat &stat, const void *ptr, size_t offset,
               int count, MPI_Datatype datatype, MPI_Status *status,
               FsIoOptions opts) {
    return BaseWriteAll<false>(f, stat, ptr, offset, count, datatype, status,
                               nullptr, opts);
  }

  int AWriteAll(File &f, AdapterStat &stat, const void *ptr, size_t offset,
               int count, MPI_Datatype datatype, MPI_Request *request,
               FsIoOptions opts) {
    return BaseWriteAll<true>(f, stat, ptr, offset, count, datatype, nullptr,
                              request, opts);
  }

  template<bool ASYNC>
  int BaseWriteOrdered(File &f, AdapterStat &stat, const void *ptr, int count,
                       MPI_Datatype datatype, MPI_Status *status,
                       MPI_Request *request, FsIoOptions opts) {
    int total;
    MPI_Scan(&count, &total, 1, MPI_INT, MPI_SUM, stat.comm_);
    MPI_Offset my_offset = total - count;
    if constexpr(!ASYNC) {
      size_t ret =
          WriteAll(f, stat, ptr, my_offset, count, datatype, status, opts);
      return ret;
    } else {
      return AWriteAll(f, stat, ptr, my_offset, count, datatype, request, opts);
    }
  }

  int WriteOrdered(File &f, AdapterStat &stat, const void *ptr, int count,
                   MPI_Datatype datatype,
                   MPI_Status *status, FsIoOptions opts) {
    return BaseWriteOrdered<false>(f, stat, ptr, count, datatype, status,
                                   nullptr, opts);
  }

  int AWriteOrdered(File &f, AdapterStat &stat, const void *ptr, int count,
                    MPI_Datatype datatype, MPI_Request *request,
                    FsIoOptions opts) {
    HILOG(kDebug, "Starting an asynchronous write")
    return BaseWriteOrdered<true>(f, stat, ptr, count, datatype, nullptr,
                                  request, opts);
  }

  int Wait(MPI_Request *req, MPI_Status *status) {
    auto mdm = HERMES_FS_METADATA_MANAGER;
    FsAsyncTask *fstask = mdm->FindTask(reinterpret_cast<size_t>(req));
    if (fstask) {
      Filesystem::Wait(fstask);
      memcpy(status, fstask->io_status_.mpi_status_ptr_, sizeof(MPI_Status));
      mdm->DeleteTask(reinterpret_cast<size_t>(req));
      delete (fstask);
      return MPI_SUCCESS;
    }
    return real_api_->MPI_Wait(req, status);
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

 public:
  /** Allocate an fd for the file f */
  void RealOpen(File &f,
                AdapterStat &stat,
                const std::string &path) override {
    if (stat.amode_ & MPI_MODE_CREATE) {
      stat.hflags_.SetBits(HERMES_FS_CREATE);
      stat.hflags_.SetBits(HERMES_FS_TRUNC);
    }
    if (stat.amode_ & MPI_MODE_APPEND) {
      stat.hflags_.SetBits(HERMES_FS_APPEND);
    }

    // NOTE(llogan): Allowing scratch mode to create empty files for MPI to
    // satisfy IOR.
    f.mpi_status_ = real_api_->MPI_File_open(
        stat.comm_, path.c_str(), stat.amode_, stat.info_, &stat.mpi_fh_);
    if (f.mpi_status_ != MPI_SUCCESS) {
      f.status_ = false;
    }

    /*if (stat.hflags_.Any(HERMES_FS_CREATE)) {
      if (stat.adapter_mode_ != AdapterMode::kScratch) {
        f.mpi_status_ = real_api_->MPI_File_open(
            stat.comm_, path.c_str(), stat.amode_, stat.info_, &stat.mpi_fh_);
      }
    } else {
      f.mpi_status_ = real_api_->MPI_File_open(
          stat.comm_, path.c_str(), stat.amode_, stat.info_, &stat.mpi_fh_);
    }

    if (f.mpi_status_ == MPI_SUCCESS) {
      stat.hflags_.SetBits(HERMES_FS_EXISTS);
    }
    if (f.mpi_status_ != MPI_SUCCESS &&
        stat.adapter_mode_ != AdapterMode::kScratch) {
      f.status_ = false;
    }*/
  }

  /**
   * Called after real open. Allocates the Hermes representation of
   * identifying file information, such as a hermes file descriptor
   * and hermes file handler. These are not the same as STDIO file
   * descriptor and STDIO file handler.
   * */
  void HermesOpen(File &f,
                  const AdapterStat &stat,
                  FilesystemIoClientState &fs_mdm) override {
    // f.hermes_mpi_fh_ = (MPI_File)fs_mdm.stat_;
    f.hermes_mpi_fh_ = stat.mpi_fh_;
  }

  /** Synchronize \a file FILE f */
  int RealSync(const File &f,
               const AdapterStat &stat) override {
    return real_api_->MPI_File_sync(stat.mpi_fh_);
  }

  /** Close \a file FILE f */
  int RealClose(const File &f,
                AdapterStat &stat) override {
    return real_api_->MPI_File_close(&stat.mpi_fh_);
  }

  /**
   * Called before RealClose. Releases information provisioned during
   * the allocation phase.
   * */
  void HermesClose(File &f,
                   const AdapterStat &stat,
                   FilesystemIoClientState &fs_mdm) override {
    (void) f; (void) stat; (void) fs_mdm;
  }

  /** Remove \a file FILE f */
  int RealRemove(const std::string &path) override {
    return remove(path.c_str());
  }

  /** Get initial statistics from the backend */
  size_t GetBackendSize(const hipc::charbuf &bkt_name) override {
    size_t true_size = 0;
    std::string filename = bkt_name.str();
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd < 0) { return 0; }
    struct stat buf;
    fstat(fd, &buf);
    true_size = buf.st_size;
    close(fd);

    HILOG(kDebug, "The size of the file {} on disk is {} bytes",
          filename, true_size)
    return true_size;
  }

  /** Write blob to backend */
  void WriteBlob(const std::string &bkt_name,
                 const Blob &full_blob,
                 const FsIoOptions &opts,
                 IoStatus &status) override {
    status.success_ = true;
    HILOG(kDebug,
          "Write called for: {}"
          " on offset: {}"
          " and size: {}",
          bkt_name, opts.backend_off_, full_blob.size())
    MPI_File fh;
    int write_count = 0;
    status.mpi_ret_ = real_api_->MPI_File_open(MPI_COMM_SELF, bkt_name.c_str(),
                                              MPI_MODE_RDONLY,
                                              MPI_INFO_NULL, &fh);
    if (status.mpi_ret_ != MPI_SUCCESS) {
      status.success_ = false;
      return;
    }

    status.mpi_ret_ = real_api_->MPI_File_seek(fh, opts.backend_off_,
                                              MPI_SEEK_SET);
    if (status.mpi_ret_ != MPI_SUCCESS) {
      status.success_ = false;
      goto ERROR;
    }
    status.mpi_ret_ = real_api_->MPI_File_write(fh,
                                               full_blob.data(),
                                               opts.mpi_count_,
                                               opts.mpi_type_,
                                               status.mpi_status_ptr_);
    MPI_Get_count(status.mpi_status_ptr_,
                  opts.mpi_type_, &write_count);
    if (write_count != opts.mpi_count_) {
      status.success_ = false;
      HELOG(kError, "writing failed: wrote {} / {}",
            write_count, opts.mpi_count_)
    }

    ERROR:
    real_api_->MPI_File_close(&fh);
    status.size_ = full_blob.size();
    UpdateIoStatus(opts, status);
  }

  /** Read blob from the backend */
  void ReadBlob(const std::string &bkt_name,
                Blob &full_blob,
                const FsIoOptions &opts,
                IoStatus &status) override {
    status.success_ = true;
    HILOG(kDebug,
          "Reading from: {}"
          " on offset: {}"
          " and size: {}",
          bkt_name, opts.backend_off_, full_blob.size())
    MPI_File fh;
    int read_count = 0;
    status.mpi_ret_ = real_api_->MPI_File_open(MPI_COMM_SELF, bkt_name.c_str(),
                                              MPI_MODE_RDONLY, MPI_INFO_NULL,
                                              &fh);
    if (status.mpi_ret_ != MPI_SUCCESS) {
      status.success_ = false;
      return;
    }

    status.mpi_ret_ = real_api_->MPI_File_seek(fh, opts.backend_off_,
                                              MPI_SEEK_SET);
    if (status.mpi_ret_ != MPI_SUCCESS) {
      status.success_ = false;
      goto ERROR;
    }
    status.mpi_ret_ = real_api_->MPI_File_read(fh,
                                              full_blob.data(),
                                              opts.mpi_count_,
                                              opts.mpi_type_,
                                              status.mpi_status_ptr_);
    MPI_Get_count(status.mpi_status_ptr_,
                  opts.mpi_type_, &read_count);
    if (read_count != opts.mpi_count_) {
      status.success_ = false;
      HELOG(kError, "reading failed: read {} / {}",
            read_count, opts.mpi_count_)
    }

    ERROR:
    real_api_->MPI_File_close(&fh);
    status.size_ = full_blob.size();
    UpdateIoStatus(opts, status);
  }

  /** Update the I/O status after a ReadBlob or WriteBlob */
  void UpdateIoStatus(const FsIoOptions &opts, IoStatus &status) override {
#ifdef HERMES_OPENMPI
    status.mpi_status_ptr_->_cancelled = 0;
  status.mpi_status_ptr_->_ucount = (int) (status.size_ / opts.type_size_);
#elif defined(HERMES_MPICH)
    status.mpi_status_ptr_->count_hi_and_cancelled = 0;
    status.mpi_status_ptr_->count_lo = (int) (status.size_ / opts.type_size_);
#else
#error "No MPI implementation specified for MPIIO adapter"
#endif
  }
};

}  // namespace hermes::adapter::fs

/** Simplify access to the stateless StdioFs Singleton */
#define HERMES_MPIIO_FS \
  hshm::EasySingleton<::hermes::adapter::fs::MpiioFs>::GetInstance()
#define HERMES_STDIO_FS_T hermes::adapter::fs::MpiioFs*

#endif  // HERMES_ADAPTER_MPIIO_MPIIO_FS_API_H_
