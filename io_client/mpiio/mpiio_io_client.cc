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

#include "mpiio_io_client.h"

namespace hermes::adapter::fs {

/** Allocate an fd for the file f */
void MpiioIoClient::RealOpen(File &f,
                             AdapterStat &stat,
                             const std::string &path) {
  f.mpi_status_ = real_api->MPI_File_open(stat.comm_,
                                          path.c_str(),
                                          stat.amode_,
                                          stat.info_,
                                          &stat.mpi_fh_);
  if (f.mpi_status_ != MPI_SUCCESS) {
    f.status_ = false;
  }
  if (stat.amode_ & MPI_MODE_APPEND) {
    stat.is_append_ = true;
  }
}

/**
 * Called after real open. Allocates the Hermes representation of
 * identifying file information, such as a hermes file descriptor
 * and hermes file handler. These are not the same as POSIX file
 * descriptor and STDIO file handler.
 * */
void MpiioIoClient::HermesOpen(File &f,
                               const AdapterStat &stat,
                               FilesystemIoClientState &fs_mdm) {
  f.hermes_mpi_fh_ = (MPI_File)fs_mdm.stat_;
}

/** Synchronize \a file FILE f */
int MpiioIoClient::RealSync(const File &f,
                            const AdapterStat &stat) {
  return real_api->MPI_File_sync(stat.mpi_fh_);
}

/** Close \a file FILE f */
int MpiioIoClient::RealClose(const File &f,
                             AdapterStat &stat) {
  return real_api->MPI_File_close(&stat.mpi_fh_);
}

/**
 * Called before RealClose. Releases information provisioned during
 * the allocation phase.
 * */
void MpiioIoClient::HermesClose(File &f,
                                const AdapterStat &stat,
                                FilesystemIoClientState &fs_mdm) {
  (void) f; (void) stat; (void) fs_mdm;
}

/** Remove \a file FILE f */
int MpiioIoClient::RealRemove(const std::string &path) {
  return remove(path.c_str());
}

/** Get initial statistics from the backend */
size_t MpiioIoClient::GetSize(const hipc::charbuf &bkt_name) {
  size_t true_size = 0;
  std::string filename = bkt_name.str();
  int fd = open(filename.c_str(), O_RDONLY);
  if (fd < 0) { return 0; }
  struct stat buf;
  fstat(fd, &buf);
  true_size = buf.st_size;
  close(fd);

  VLOG(kDebug) << "The size of the file "
            << filename << " on disk is " << true_size << std::endl;
  return true_size;
}

/** Initialize I/O context using count + datatype */
size_t MpiioIoClient::IoSizeFromCount(int count,
                                      MPI_Datatype datatype,
                                      FsIoOptions &opts) {
  int datatype_size;
  opts.mpi_type_ = datatype;
  opts.mpi_count_ = count;
  MPI_Type_size(datatype, &datatype_size);
  return static_cast<size_t>(count * datatype_size);
}

/** Write blob to backend */
void MpiioIoClient::WriteBlob(const hipc::charbuf &bkt_name,
                              const Blob &full_blob,
                              const FsIoOptions &opts,
                              IoStatus &status) {
  std::string filename = bkt_name.str();
  status.success_ = true;
  VLOG(kDebug) << "Write called for filename to destination: " << filename
            << " on offset: " << opts.backend_off_
            << " and size: " << full_blob.size() << "."
            << " file_size:" << stdfs::file_size(filename)
            << " pid: " << getpid() << std::endl;
  MPI_File fh;
  int write_count = 0;
  status.mpi_ret_ = real_api->MPI_File_open(MPI_COMM_SELF, filename.c_str(),
                                            MPI_MODE_RDONLY,
                                            MPI_INFO_NULL, &fh);
  if (status.mpi_ret_ != MPI_SUCCESS) {
    status.success_ = false;
    return;
  }

  status.mpi_ret_ = real_api->MPI_File_seek(fh, opts.backend_off_,
                                            MPI_SEEK_SET);
  if (status.mpi_ret_ != MPI_SUCCESS) {
    status.success_ = false;
    goto ERROR;
  }
  status.mpi_ret_ = real_api->MPI_File_write(fh,
                                             full_blob.data(),
                                             opts.mpi_count_,
                                             opts.mpi_type_,
                                             status.mpi_status_ptr_);
  MPI_Get_count(status.mpi_status_ptr_,
                opts.mpi_type_, &write_count);
  if (write_count != opts.mpi_count_) {
    status.success_ = false;
    LOG(ERROR) << "writing failed: wrote " << write_count
               << " / " << opts.mpi_count_
               << "." << std::endl;
  }

ERROR:
  real_api->MPI_File_close(&fh);
  status.size_ = full_blob.size();
  UpdateIoStatus(opts.mpi_count_, status);
}

/** Read blob from the backend */
void MpiioIoClient::ReadBlob(const hipc::charbuf &bkt_name,
                             Blob &full_blob,
                             const FsIoOptions &opts,
                             IoStatus &status) {
  std::string filename = bkt_name.str();
  status.success_ = true;
  VLOG(kDebug) << "Reading from: " << filename
            << " on offset: " << opts.backend_off_
            << " and size: " << full_blob.size() << "."
            << " file_size:" << stdfs::file_size(filename)
            << " pid: " << getpid() << std::endl;
  MPI_File fh;
  int read_count = 0;
  status.mpi_ret_ = real_api->MPI_File_open(MPI_COMM_SELF, filename.c_str(),
                                            MPI_MODE_RDONLY, MPI_INFO_NULL,
                                            &fh);
  if (status.mpi_ret_ != MPI_SUCCESS) {
    status.success_ = false;
    return;
  }

  status.mpi_ret_ = real_api->MPI_File_seek(fh, opts.backend_off_,
                                            MPI_SEEK_SET);
  if (status.mpi_ret_ != MPI_SUCCESS) {
    status.success_ = false;
    goto ERROR;
  }
  status.mpi_ret_ = real_api->MPI_File_read(fh,
                                            full_blob.data(),
                                            opts.mpi_count_,
                                            opts.mpi_type_,
                                            status.mpi_status_ptr_);
  MPI_Get_count(status.mpi_status_ptr_,
                opts.mpi_type_, &read_count);
  if (read_count != opts.mpi_count_) {
    status.success_ = false;
    LOG(ERROR) << "reading failed: read " << read_count
               << " / " << opts.mpi_count_
               << "." << std::endl;
  }

ERROR:
  real_api->MPI_File_close(&fh);
  status.size_ = full_blob.size();
  UpdateIoStatus(opts.mpi_count_, status);
}

/** Update the I/O status after a ReadBlob or WriteBlob */
void MpiioIoClient::UpdateIoStatus(size_t count, IoStatus &status) {
  status.mpi_status_ptr_->count_hi_and_cancelled = 0;
  status.mpi_status_ptr_->count_lo = count;
}

}  // namespace hermes::adapter::fs
