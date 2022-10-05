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

size_t MpiioFS::Read(File &f, AdapterStat &stat,
                     void *ptr, size_t offset,
                     int count, MPI_Datatype datatype,
                     MPI_Status *status, IoOptions opts) {
  opts.mpi_type_ = datatype;
  if (offset >= static_cast<MPI_Offset>(stat.st_size)) {
    status->count_hi_and_cancelled = 0;
    status->count_lo = 0;
    return 0;
  }
  size_t total_read_size = Filesystem::Read(f, stat, ptr, offset,
                                static_cast<size_t>(count), opts);
  status->count_hi_and_cancelled = 0;
  status->count_lo = total_read_size;
  return total_read_size;
}

int MpiioFS::ARead(File &f, AdapterStat &stat,
                   void *ptr, size_t offset,
                   int count, MPI_Datatype datatype,
                   MPI_Request *request, IoOptions opts) {
  opts.mpi_type_ = datatype;
  int ret = Filesystem::ARead(f, stat, ptr, offset,
                              static_cast<size_t>(count),
                              reinterpret_cast<size_t>(request), opts);
  return ret;
}

size_t MpiioFS::ReadAll(File &f, AdapterStat &stat,
                        void *ptr, size_t offset,
                        int count, MPI_Datatype datatype,
                        MPI_Status *status, IoOptions opts) {
  opts.mpi_type_ = datatype;
  MPI_Barrier(stat.comm);
  size_t ret = Read(f, stat, ptr, offset, count, datatype, status, opts);
  MPI_Barrier(stat.comm);
  return ret;
}

size_t MpiioFS::ReadOrdered(File &f, AdapterStat &stat,
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

size_t MpiioFS::Write(File &f, AdapterStat &stat,
                      const void *ptr, size_t offset,
                      int count, MPI_Datatype datatype,
                      MPI_Status *status, IoOptions opts) {
  opts.mpi_type_ = datatype;
  if (offset >= static_cast<MPI_Offset>(stat.st_size)) {
    status->count_hi_and_cancelled = 0;
    status->count_lo = 0;
    return 0;
  }
  size_t total_write_size = Filesystem::Write(f, stat, ptr, offset,
                                              static_cast<size_t>(count), opts);
  status->count_hi_and_cancelled = 0;
  status->count_lo = total_write_size;
  return total_write_size;
}

int MpiioFS::AWrite(File &f, AdapterStat &stat,
                    const void *ptr, size_t offset,
                    int count, MPI_Datatype datatype,
                    MPI_Request *request, IoOptions opts) {
  opts.mpi_type_ = datatype;
  int ret = Filesystem::AWrite(f, stat, ptr, offset,
                               static_cast<size_t>(count),
                               reinterpret_cast<size_t>(request), opts);
  return ret;
}

size_t MpiioFS::WriteAll(File &f, AdapterStat &stat,
                         const void *ptr, size_t offset,
                         int count, MPI_Datatype datatype,
                         MPI_Status *status, IoOptions opts) {
  opts.mpi_type_ = datatype;
  MPI_Barrier(stat.comm);
  size_t ret = Write(f, stat, ptr, offset, count, datatype, status, opts);
  MPI_Barrier(stat.comm);
  return ret;
}

size_t MpiioFS::WriteOrdered(File &f, AdapterStat &stat,
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

int MpiioFS::Wait(MPI_Request *req, MPI_Status *status) {
  auto mdm = Singleton<MetadataManager>::GetInstance();
  auto iter = mdm->request_map.find(reinterpret_cast<size_t>(req));
  if (iter != mdm->request_map.end()) {
    hermes::adapter::fs::HermesRequest *req = iter->second;
    req->return_future.get();
    memcpy(status, &iter->second->status, sizeof(MPI_Status));
    auto h_req = iter->second;
    mdm->request_map.erase(iter);
    delete (h_req);
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

size_t MpiioFS::Read(File &f, AdapterStat &stat,
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

size_t MpiioFS::ReadAll(File &f, AdapterStat &stat,
                        void *ptr, int count, MPI_Datatype datatype,
                        MPI_Status *status) {
  IoOptions opts = IoOptions::DataType(datatype, true);
  return ReadAll(f, stat, ptr, Tell(f, stat), count, datatype, status, opts);
}

size_t MpiioFS::Write(File &f, AdapterStat &stat,
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

size_t MpiioFS::WriteAll(File &f, AdapterStat &stat,
                         const void *ptr, int count, MPI_Datatype datatype,
                         MPI_Status *status) {
  IoOptions opts = IoOptions::DataType(datatype, true);
  return WriteAll(f, stat, ptr, Tell(f, stat), count, datatype, status, opts);
}


/**
 * Variants which retrieve the stat data structure internally
 * */

size_t MpiioFS::Read(File &f, bool &stat_exists,
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

size_t MpiioFS::ReadAll(File &f, bool &stat_exists,
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

size_t MpiioFS::ReadOrdered(File &f, bool &stat_exists,
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

size_t MpiioFS::Write(File &f, bool &stat_exists,
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

size_t MpiioFS::WriteAll(File &f, bool &stat_exists,
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

size_t MpiioFS::WriteOrdered(File &f, bool &stat_exists,
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

size_t MpiioFS::Read(File &f, bool &stat_exists,
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

size_t MpiioFS::ReadAll(File &f, bool &stat_exists,
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

size_t MpiioFS::Write(File &f, bool &stat_exists,
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

size_t MpiioFS::WriteAll(File &f, bool &stat_exists,
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

File MpiioFS::_RealOpen(AdapterStat &stat, const std::string &path) {
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

void MpiioFS::_InitFile(File &f) {
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

void MpiioFS::_OpenInitStats(File &f, AdapterStat &stat, bool bucket_exists) {
  (void) bucket_exists;
  MPI_Offset size = static_cast<MPI_Offset>(stat.st_size);
  MPI_File_get_size(f.mpi_fh_, &size);
  stat.st_size = size;
  if (stat.amode & MPI_MODE_APPEND) {
    stat.st_ptr = stat.st_size;
    stat.is_append = true;
  }
}

size_t MpiioFS::_RealWrite(const std::string &filename, off_t offset,
                           size_t count, const u8 *data_ptr, IoOptions &opts) {
  LOG(INFO) << "Writing to file: " << filename << " offset: " << offset
            << " size:" << count << "."
            << " file_size:" << stdfs::file_size(filename) << std::endl;
  MPI_File fh;
  int write_size = 0;
  MPI_Status write_status;
  int status = real_api->MPI_File_open(MPI_COMM_SELF, filename.c_str(),
                                       MPI_MODE_RDONLY, MPI_INFO_NULL, &fh);
  if (status == MPI_SUCCESS) {
    return 0;
  }

  status = real_api->MPI_File_seek(fh, offset, MPI_SEEK_SET);
  if (status != MPI_SUCCESS) {
    goto ERROR;
  }
  status = real_api->MPI_File_write(fh, data_ptr, count, opts.mpi_type_,
                                    &write_status);
  MPI_Get_count(&write_status, opts.mpi_type_, &write_size);
  if (static_cast<int>(count)) {
    LOG(ERROR) << "writing failed: write " << write_size << " of " << count
               << "." << std::endl;
  }

ERROR:
  status = real_api->MPI_File_close(&fh);
  return write_size;
}

size_t MpiioFS::_RealRead(const std::string &filename, off_t offset,
                          size_t count, u8 *data_ptr, IoOptions &opts) {
  LOG(INFO) << "Read called for filename from destination: " << filename
            << " on offset: " << offset << " and count: " << count << "."
            << " file_size:" << stdfs::file_size(filename) << std::endl;
  MPI_File fh;
  int read_size = 0;
  MPI_Status read_status;
  int status = real_api->MPI_File_open(MPI_COMM_SELF, filename.c_str(),
                                       MPI_MODE_RDONLY, MPI_INFO_NULL, &fh);
  if (status == MPI_SUCCESS) {
    return 0;
  }

  status = real_api->MPI_File_seek(fh, offset, MPI_SEEK_SET);
  if (status != MPI_SUCCESS) {
    goto ERROR;
  }
  status = real_api->MPI_File_read(fh, data_ptr, count, opts.mpi_type_,
                                   &read_status);
  MPI_Get_count(&read_status, opts.mpi_type_, &read_size);
  if (read_size != static_cast<int>(count)) {
    LOG(ERROR) << "reading failed: read " << read_size << " of " << count
               << "." << std::endl;
  }

ERROR:
  status = real_api->MPI_File_close(&fh);
  return read_size;
}

int MpiioFS::_RealSync(File &f) {
  return real_api->MPI_File_sync(f.mpi_fh_);
}

int MpiioFS::_RealClose(File &f) {
  return real_api->MPI_File_close(&f.mpi_fh_);
}

}  // namespace hermes::adapter::mpiio
