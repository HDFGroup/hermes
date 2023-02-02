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

#include "fs_api.h"

namespace hermes::adapter::mpiio {

size_t IoSizeFromCount(int count, MPI_Datatype datatype, IoOptions &opts) {
  int datatype_size;
  opts.mpi_type_ = datatype;
  opts.count_ = count;
  MPI_Type_size(datatype, &datatype_size);
  return static_cast<size_t>(count * datatype_size);
}

int MpiioFS::Read(File &f, AdapterStat &stat,
                     void *ptr, size_t offset,
                     int count, MPI_Datatype datatype,
                     MPI_Status *status, IoOptions opts) {
  opts.mpi_type_ = datatype;
  if (offset  + count >= static_cast<size_t>(stat.st_size)) {
    status->count_hi_and_cancelled = 0;
    status->count_lo = 0;
    return 0;
  }
  IoStatus io_status;
  io_status.mpi_status_ptr_ = status;
  size_t total_size = IoSizeFromCount(count, datatype, opts);
  Filesystem::Read(f, stat, ptr, offset,
                   total_size, io_status, opts);
  return io_status.mpi_ret_;
}

int MpiioFS::ARead(File &f, AdapterStat &stat,
                   void *ptr, size_t offset,
                   int count, MPI_Datatype datatype,
                   MPI_Request *request, IoOptions opts) {
  opts.mpi_type_ = datatype;
  IoStatus io_status;
  size_t total_size = IoSizeFromCount(count, datatype, opts);
  Filesystem::ARead(f, stat, ptr, offset, total_size,
                    reinterpret_cast<size_t>(request),
                    io_status, opts);
  return io_status.mpi_ret_;
}

int MpiioFS::ReadAll(File &f, AdapterStat &stat,
                        void *ptr, size_t offset,
                        int count, MPI_Datatype datatype,
                        MPI_Status *status, IoOptions opts) {
  opts.mpi_type_ = datatype;
  MPI_Barrier(stat.comm);
  size_t ret = Read(f, stat, ptr, offset, count, datatype, status, opts);
  MPI_Barrier(stat.comm);
  return ret;
}

int MpiioFS::ReadOrdered(File &f, AdapterStat &stat,
                            void *ptr, int count,
                            MPI_Datatype datatype,
                            MPI_Status *status, IoOptions opts) {
  opts.mpi_type_ = datatype;

  int total;
  MPI_Scan(&count, &total, 1, MPI_INT, MPI_SUM, stat.comm);
  MPI_Offset my_offset = total - count;
  size_t ret = ReadAll(f, stat, ptr, my_offset, count, datatype, status, opts);
  return ret;
}

int MpiioFS::Write(File &f, AdapterStat &stat,
                      const void *ptr, size_t offset,
                      int count, MPI_Datatype datatype,
                      MPI_Status *status, IoOptions opts) {
  opts.mpi_type_ = datatype;
  IoStatus io_status;
  io_status.mpi_status_ptr_ = status;
  size_t total_size = IoSizeFromCount(count, datatype, opts);
  Filesystem::Write(f, stat, ptr, offset, total_size,
                    io_status, opts);
  return io_status.mpi_ret_;
}

int MpiioFS::AWrite(File &f, AdapterStat &stat,
                    const void *ptr, size_t offset,
                    int count, MPI_Datatype datatype,
                    MPI_Request *request, IoOptions opts) {
  opts.mpi_type_ = datatype;
  IoStatus io_status;
  size_t total_size = IoSizeFromCount(count, datatype, opts);
  Filesystem::AWrite(f, stat, ptr, offset, total_size,
                     reinterpret_cast<size_t>(request),
                     io_status, opts);
  return io_status.mpi_ret_;
}

int MpiioFS::WriteAll(File &f, AdapterStat &stat,
                         const void *ptr, size_t offset,
                         int count, MPI_Datatype datatype,
                         MPI_Status *status, IoOptions opts) {
  opts.mpi_type_ = datatype;
  MPI_Barrier(stat.comm);
  int ret = Write(f, stat, ptr, offset, count, datatype, status, opts);
  MPI_Barrier(stat.comm);
  return ret;
}

int MpiioFS::WriteOrdered(File &f, AdapterStat &stat,
                            const void *ptr, int count,
                            MPI_Datatype datatype,
                            MPI_Status *status, IoOptions opts) {
  opts.mpi_type_ = datatype;
  int total;
  MPI_Scan(&count, &total, 1, MPI_INT, MPI_SUM, stat.comm);
  MPI_Offset my_offset = total - count;
  size_t ret = WriteAll(f, stat, ptr, my_offset, count, datatype, status, opts);
  return ret;
}

int MpiioFS::AWriteOrdered(File &f, AdapterStat &stat,
                  const void *ptr, int count,
                  MPI_Datatype datatype,
                  MPI_Request *request, IoOptions opts) {
  LOG(INFO) << "Starting an asynchronous write" << std::endl;
  auto pool =
      Singleton<ThreadPool>::GetInstance();
  HermesRequest *hreq = new HermesRequest();
  auto lambda =
      [](MpiioFS *fs, File &f, AdapterStat &stat,
         const void *ptr, int count,
         MPI_Datatype datatype, MPI_Status *status,
         IoOptions opts) {
        int ret = fs->WriteOrdered(f, stat, ptr,
                                count, datatype, status, opts);
        return static_cast<size_t>(ret);
      };
  auto func = std::bind(lambda, this, f, stat, ptr,
                        count, datatype, &hreq->io_status.mpi_status_,
                        opts);
  hreq->return_future = pool->run(func);
  auto mdm = Singleton<MetadataManager>::GetInstance();
  mdm->request_map.emplace(reinterpret_cast<size_t>(request), hreq);
  return MPI_SUCCESS;
}

int MpiioFS::Wait(MPI_Request *req, MPI_Status *status) {
  auto mdm = Singleton<MetadataManager>::GetInstance();
  auto iter = mdm->request_map.find(reinterpret_cast<size_t>(req));
  if (iter != mdm->request_map.end()) {
    hermes::adapter::fs::HermesRequest *hreq = iter->second;
    hreq->return_future.get();
    memcpy(status,
           hreq->io_status.mpi_status_ptr_,
           sizeof(MPI_Status));
    mdm->request_map.erase(iter);
    delete (hreq);
    return MPI_SUCCESS;
  }
  return real_api->MPI_Wait(req, status);
}

int MpiioFS::WaitAll(int count, MPI_Request *req, MPI_Status *status) {
  int ret = 0;
  for (int i = 0; i < count; i++) {
    auto sub_ret = Wait(&req[i], &status[i]);
    if (sub_ret != MPI_SUCCESS) {
      ret = sub_ret;
    }
  }
  return ret;
}

int MpiioFS::Seek(File &f, AdapterStat &stat,
                  MPI_Offset offset, int whence) {
  Filesystem::Seek(f, stat,
                   MpiioSeekModeConv::Normalize(whence),
                   offset);
  return MPI_SUCCESS;
}

int MpiioFS::SeekShared(File &f, AdapterStat &stat,
                        MPI_Offset offset, int whence) {
  MPI_Offset sum_offset;
  int sum_whence;
  int comm_participators;
  MPI_Comm_size(stat.comm, &comm_participators);
  MPI_Allreduce(&offset, &sum_offset, 1, MPI_LONG_LONG_INT, MPI_SUM,
                stat.comm);
  MPI_Allreduce(&whence, &sum_whence, 1, MPI_INT, MPI_SUM,
                stat.comm);
  if (sum_offset / comm_participators != offset) {
    LOG(ERROR)
        << "Same offset should be passed across the opened file communicator."
        << std::endl;
  }
  if (sum_whence / comm_participators != whence) {
    LOG(ERROR)
        << "Same whence should be passed across the opened file communicator."
        << std::endl;
  }
  Seek(f, stat, offset, whence);
  return 0;
}

/**
 * Variants which internally find the correct offset
* */

int MpiioFS::Read(File &f, AdapterStat &stat,
            void *ptr, int count, MPI_Datatype datatype,
            MPI_Status *status) {
  IoOptions opts = IoOptions::DataType(datatype, true);
  return Read(f, stat, ptr, Tell(f, stat), count, datatype, status, opts);
}

int MpiioFS::ARead(File &f, AdapterStat &stat,
          void *ptr, int count, MPI_Datatype datatype, MPI_Request *request) {
  IoOptions opts = IoOptions::DataType(datatype, true);
  return ARead(f, stat, ptr, Tell(f, stat), count, datatype, request, opts);
}

int MpiioFS::ReadAll(File &f, AdapterStat &stat,
                        void *ptr, int count, MPI_Datatype datatype,
                        MPI_Status *status) {
  IoOptions opts = IoOptions::DataType(datatype, true);
  return ReadAll(f, stat, ptr, Tell(f, stat), count, datatype, status, opts);
}

int MpiioFS::Write(File &f, AdapterStat &stat,
                      const void *ptr, int count, MPI_Datatype datatype,
                      MPI_Status *status) {
  IoOptions opts = IoOptions::DataType(datatype, true);
  return Write(f, stat, ptr, Tell(f, stat), count, datatype, status, opts);
}

int MpiioFS::AWrite(File &f, AdapterStat &stat,
                    const void *ptr, int count, MPI_Datatype datatype,
                    MPI_Request *request) {
  IoOptions opts = IoOptions::DataType(datatype, true);
  return AWrite(f, stat, ptr, Tell(f, stat), count, datatype, request, opts);
}

int MpiioFS::WriteAll(File &f, AdapterStat &stat,
                         const void *ptr, int count, MPI_Datatype datatype,
                         MPI_Status *status) {
  IoOptions opts = IoOptions::DataType(datatype, true);
  return WriteAll(f, stat, ptr, Tell(f, stat), count, datatype, status, opts);
}


/**
 * Variants which retrieve the stat data structure internally
 * */

int MpiioFS::Read(File &f, bool &stat_exists,
                     void *ptr, size_t offset,
                     int count, MPI_Datatype datatype,
                     MPI_Status *status) {
  auto mdm = Singleton<MetadataManager>::GetInstance();
  auto [stat, exists] = mdm->Find(f);
  if (!exists) {
    stat_exists = false;
    return -1;
  }
  stat_exists = true;
  IoOptions opts = IoOptions::DataType(datatype, false);
  return Read(f, stat, ptr, offset, count, datatype, status, opts);
}

int MpiioFS::ARead(File &f, bool &stat_exists,
                   void *ptr, size_t offset,
                   int count, MPI_Datatype datatype, MPI_Request *request) {
  auto mdm = Singleton<MetadataManager>::GetInstance();
  auto [stat, exists] = mdm->Find(f);
  if (!exists) {
    stat_exists = false;
    return -1;
  }
  stat_exists = true;
  IoOptions opts = IoOptions::DataType(datatype, false);
  return ARead(f, stat, ptr, offset, count, datatype, request, opts);
}

int MpiioFS::ReadAll(File &f, bool &stat_exists,
                        void *ptr, size_t offset,
                        int count, MPI_Datatype datatype,
                        MPI_Status *status) {
  auto mdm = Singleton<MetadataManager>::GetInstance();
  auto [stat, exists] = mdm->Find(f);
  if (!exists) {
    stat_exists = false;
    return -1;
  }
  stat_exists = true;
  IoOptions opts = IoOptions::DataType(datatype, false);
  return ReadAll(f, stat, ptr, offset, count, datatype, status, opts);
}

int MpiioFS::ReadOrdered(File &f, bool &stat_exists,
                            void *ptr, int count,
                            MPI_Datatype datatype,
                            MPI_Status *status) {
  auto mdm = Singleton<MetadataManager>::GetInstance();
  auto [stat, exists] = mdm->Find(f);
  if (!exists) {
    stat_exists = false;
    return -1;
  }
  stat_exists = true;
  IoOptions opts = IoOptions::DataType(datatype, false);
  return ReadOrdered(f, stat, ptr, count, datatype, status, opts);
}

int MpiioFS::Write(File &f, bool &stat_exists,
                      const void *ptr, size_t offset,
                      int count, MPI_Datatype datatype,
                      MPI_Status *status) {
  auto mdm = Singleton<MetadataManager>::GetInstance();
  auto [stat, exists] = mdm->Find(f);
  if (!exists) {
    stat_exists = false;
    return -1;
  }
  stat_exists = true;
  IoOptions opts = IoOptions::DataType(datatype, false);
  return Write(f, stat, ptr, offset, count, datatype, status, opts);
}

int MpiioFS::AWrite(File &f, bool &stat_exists,
                    const void *ptr, size_t offset,
                    int count, MPI_Datatype datatype, MPI_Request *request) {
  auto mdm = Singleton<MetadataManager>::GetInstance();
  auto [stat, exists] = mdm->Find(f);
  if (!exists) {
    stat_exists = false;
    return -1;
  }
  stat_exists = true;
  IoOptions opts = IoOptions::DataType(datatype, false);
  return AWrite(f, stat, ptr, offset, count, datatype, request, opts);
}

int MpiioFS::WriteAll(File &f, bool &stat_exists,
                         const void *ptr, size_t offset,
                         int count, MPI_Datatype datatype,
                         MPI_Status *status) {
  auto mdm = Singleton<MetadataManager>::GetInstance();
  auto [stat, exists] = mdm->Find(f);
  if (!exists) {
    stat_exists = false;
    return -1;
  }
  stat_exists = true;
  IoOptions opts = IoOptions::DataType(datatype, false);
  return WriteAll(f, stat, ptr, offset, count, datatype, status, opts);
}

int MpiioFS::WriteOrdered(File &f, bool &stat_exists,
                             const void *ptr, int count,
                             MPI_Datatype datatype,
                             MPI_Status *status) {
  auto mdm = Singleton<MetadataManager>::GetInstance();
  auto [stat, exists] = mdm->Find(f);
  if (!exists) {
    stat_exists = false;
    return -1;
  }
  stat_exists = true;
  IoOptions opts = IoOptions::DataType(datatype, false);
  return WriteOrdered(f, stat, ptr, count, datatype, status, opts);
}

int MpiioFS::AWriteOrdered(File &f, bool &stat_exists,
                           const void *ptr, int count,
                           MPI_Datatype datatype,
                           MPI_Request *request) {
  auto mdm = Singleton<MetadataManager>::GetInstance();
  auto [stat, exists] = mdm->Find(f);
  if (!exists) {
    stat_exists = false;
    return -1;
  }
  stat_exists = true;
  IoOptions opts = IoOptions::DataType(datatype, false);
  return AWriteOrdered(f, stat, ptr, count, datatype, request, opts);
}

int MpiioFS::Read(File &f, bool &stat_exists,
                     void *ptr, int count, MPI_Datatype datatype,
                     MPI_Status *status) {
  auto mdm = Singleton<MetadataManager>::GetInstance();
  auto [stat, exists] = mdm->Find(f);
  if (!exists) {
    stat_exists = false;
    return -1;
  }
  stat_exists = true;
  return Read(f, stat, ptr, count, datatype, status);
}

int MpiioFS::ARead(File &f, bool &stat_exists,
                   void *ptr, int count, MPI_Datatype datatype,
                   MPI_Request *request) {
  auto mdm = Singleton<MetadataManager>::GetInstance();
  auto [stat, exists] = mdm->Find(f);
  if (!exists) {
    stat_exists = false;
    return -1;
  }
  stat_exists = true;
  return ARead(f, stat, ptr, count, datatype, request);
}

int MpiioFS::ReadAll(File &f, bool &stat_exists,
                        void *ptr, int count, MPI_Datatype datatype,
                        MPI_Status *status) {
  auto mdm = Singleton<MetadataManager>::GetInstance();
  auto [stat, exists] = mdm->Find(f);
  if (!exists) {
    stat_exists = false;
    return -1;
  }
  stat_exists = true;
  return ReadAll(f, stat, ptr, count, datatype, status);
}

int MpiioFS::Write(File &f, bool &stat_exists,
                      const void *ptr, int count, MPI_Datatype datatype,
                      MPI_Status *status) {
  auto mdm = Singleton<MetadataManager>::GetInstance();
  auto [stat, exists] = mdm->Find(f);
  if (!exists) {
    stat_exists = false;
    return -1;
  }
  stat_exists = true;
  return Write(f, stat, ptr, count, datatype, status);
}

int MpiioFS::AWrite(File &f, bool &stat_exists,
                    const void *ptr, int count, MPI_Datatype datatype,
                    MPI_Request *request) {
  auto mdm = Singleton<MetadataManager>::GetInstance();
  auto [stat, exists] = mdm->Find(f);
  if (!exists) {
    stat_exists = false;
    return -1;
  }
  stat_exists = true;
  return AWrite(f, stat, ptr, count, datatype, request);
}

int MpiioFS::WriteAll(File &f, bool &stat_exists,
                         const void *ptr, int count, MPI_Datatype datatype,
                         MPI_Status *status) {
  auto mdm = Singleton<MetadataManager>::GetInstance();
  auto [stat, exists] = mdm->Find(f);
  if (!exists) {
    stat_exists = false;
    return -1;
  }
  stat_exists = true;
  return WriteAll(f, stat, ptr, count, datatype, status);
}

int MpiioFS::Seek(File &f, bool &stat_exists,
                  MPI_Offset offset, int whence) {
  auto mdm = Singleton<MetadataManager>::GetInstance();
  auto [stat, exists] = mdm->Find(f);
  if (!exists) {
    stat_exists = false;
    return -1;
  }
  stat_exists = true;
  return Seek(f, stat, offset, whence);
}

int MpiioFS::SeekShared(File &f, bool &stat_exists,
                        MPI_Offset offset, int whence) {
  auto mdm = Singleton<MetadataManager>::GetInstance();
  auto [stat, exists] = mdm->Find(f);
  if (!exists) {
    stat_exists = false;
    return -1;
  }
  stat_exists = true;
  return SeekShared(f, stat, offset, whence);
}

/**
 * Internal overrides
 * */

File MpiioFS::RealOpen(AdapterStat &stat, const std::string &path) {
  File f;
  f.mpi_status_ = real_api->MPI_File_open(stat.comm, path.c_str(), stat.amode,
                                          stat.info, &f.mpi_fh_);
  if (f.mpi_status_ != MPI_SUCCESS) {
    f.status_ = false;
  }
  // NOTE(llogan): MPI_Info_get does not behave well, so removing
  /*
  MPI_Info info;
  MPI_File_get_info(f.mpi_fh_, &info);
  MPI_Info_set(info, "path", path.c_str());
  MPI_File_set_info(f.mpi_fh_, info);*/
  return f;
}

void MpiioFS::InitFile(File &f) {
  // NOTE(llogan): MPI_Info_get does not behave well, so removing
  (void) f;
  /*struct stat st;
  std::string filename = GetFilenameFromFP(&f.mpi_fh_);
  int fd = posix_api->open(filename.c_str(), O_RDONLY);
  posix_api->__fxstat(_STAT_VER, fd, &st);
  f.st_dev = st.st_dev;
  f.st_ino = st.st_ino;
  posix_api->close(fd);*/
}

void MpiioFS::OpenInitStat(File &f, AdapterStat &stat) {
  MPI_Offset size = static_cast<MPI_Offset>(stat.st_size);
  MPI_File_get_size(f.mpi_fh_, &size);
  stat.st_size = size;
  if (stat.amode & MPI_MODE_APPEND) {
    stat.is_append = true;
  }
}

size_t MpiioFS::_RealWrite(const std::string &filename, off_t offset,
                           size_t size, const u8 *data_ptr,
                           IoStatus &io_status, IoOptions &opts) {
  LOG(INFO) << "Writing to file: " << filename << " offset: " << offset
            << " size:" << size << "."
            << " offset:" << offset << "."
            << " file_size:" << stdfs::file_size(filename)
            << " pid: " << getpid() << std::endl;
  MPI_File fh;
  int write_count = 0;
  io_status.mpi_ret_ = real_api->MPI_File_open(MPI_COMM_SELF, filename.c_str(),
                                       MPI_MODE_RDWR, MPI_INFO_NULL, &fh);
  if (io_status.mpi_ret_ != MPI_SUCCESS) {
    return 0;
  }

  io_status.mpi_ret_ = real_api->MPI_File_seek(fh, offset, MPI_SEEK_SET);
  if (io_status.mpi_ret_ != MPI_SUCCESS) {
    goto ERROR;
  }
  io_status.mpi_ret_ = real_api->MPI_File_write(fh, data_ptr,
                                                opts.count_, opts.mpi_type_,
                                                io_status.mpi_status_ptr_);
  MPI_Get_count(io_status.mpi_status_ptr_, opts.mpi_type_, &write_count);
  if (opts.count_ != write_count) {
    LOG(ERROR) << "writing failed: write " << write_count << " of "
               << opts.count_ << "." << std::endl;
  }

ERROR:
  real_api->MPI_File_close(&fh);
  return size;
}

size_t MpiioFS::_RealRead(const std::string &filename, off_t offset,
                          size_t size, u8 *data_ptr,
                          IoStatus &io_status, IoOptions &opts) {
  LOG(INFO) << "Read called for filename from destination: " << filename
            << " on offset: " << offset << " and size: " << size << "."
            << " file_size:" << stdfs::file_size(filename)
            << " pid: " << getpid() << std::endl;
  MPI_File fh;
  int read_count = 0;
  io_status.mpi_ret_ = real_api->MPI_File_open(MPI_COMM_SELF, filename.c_str(),
                                       MPI_MODE_RDONLY, MPI_INFO_NULL, &fh);
  if (io_status.mpi_ret_ != MPI_SUCCESS) {
    return 0;
  }

  io_status.mpi_ret_ = real_api->MPI_File_seek(fh, offset, MPI_SEEK_SET);
  if (io_status.mpi_ret_ != MPI_SUCCESS) {
    goto ERROR;
  }
  io_status.mpi_ret_ = real_api->MPI_File_read(fh, data_ptr,
                                               opts.count_, opts.mpi_type_,
                                               io_status.mpi_status_ptr_);
  MPI_Get_count(io_status.mpi_status_ptr_,
                opts.mpi_type_, &read_count);
  if (read_count != opts.count_) {
    LOG(ERROR) << "reading failed: read " << read_count << " of " << opts.count_
               << "." << std::endl;
  }

ERROR:
  real_api->MPI_File_close(&fh);
  return size;
}

void MpiioFS::_IoStats(size_t count, IoStatus &io_status, IoOptions &opts) {
  (void) opts;
  io_status.mpi_status_ptr_->count_hi_and_cancelled = 0;
  io_status.mpi_status_ptr_->count_lo = count;
}

int MpiioFS::RealSync(File &f) {
  return real_api->MPI_File_sync(f.mpi_fh_);
}

int MpiioFS::RealClose(File &f) {
  return real_api->MPI_File_close(&f.mpi_fh_);
}

}  // namespace hermes::adapter::mpiio
