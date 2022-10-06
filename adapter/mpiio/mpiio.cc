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

bool mpiio_intercepted = true;

#include <hermes.h>
#include <bucket.h>
#include <vbucket.h>

#include "real_api.h"
#include "fs_api.h"

#include "constants.h"
#include "singleton.h"
#include "interceptor.h"
#include "interceptor.cc"

#include "thread_pool.h"

/**
 * Namespace declarations
 */
using hermes::ThreadPool;
using hermes::adapter::fs::MetadataManager;
using hermes::adapter::fs::File;
using hermes::adapter::mpiio::API;
using hermes::adapter::mpiio::MpiioFS;
using hermes::adapter::mpiio::MpiioSeekModeConv;
using hermes::adapter::Singleton;

namespace hapi = hermes::api;
namespace stdfs = std::experimental::filesystem;
using hermes::adapter::WeaklyCanonical;

/**
 * Internal Functions.
 */


inline bool IsTracked(MPI_File *fh) {
  if (hermes::adapter::exit) return false;
  auto mdm = Singleton<MetadataManager>::GetInstance();
  auto fs_api = Singleton<MpiioFS>::GetInstance();
  File f; f.mpi_fh_ = (*fh); fs_api->_InitFile(f);
  auto [stat, exists] = mdm->Find(f);
  return exists;
}

/**
 * MPI
 */
int HERMES_DECL(MPI_Init)(int *argc, char ***argv) {
  auto real_api = Singleton<API>::GetInstance();
  int status = real_api->MPI_Init(argc, argv);
  if (status == 0) {
    LOG(INFO) << "MPI Init intercepted." << std::endl;
    auto mdm = Singleton<MetadataManager>::GetInstance();
    mdm->InitializeHermes(true);
    Singleton<ThreadPool>::GetInstance(hermes::adapter::fs::kNumThreads);
  }
  return status;
}

int HERMES_DECL(MPI_Finalize)(void) {
  LOG(INFO) << "MPI Finalize intercepted." << std::endl;
  auto real_api = Singleton<API>::GetInstance();
  auto mdm = Singleton<MetadataManager>::GetInstance();
  mdm->FinalizeHermes();
  int status = real_api->MPI_Finalize();
  return status;
}

int HERMES_DECL(MPI_Wait)(MPI_Request *req, MPI_Status *status) {
  auto fs_api = Singleton<MpiioFS>::GetInstance();
  return fs_api->Wait(req, status);
}

int HERMES_DECL(MPI_Waitall)(int count, MPI_Request *req, MPI_Status *status) {
  auto fs_api = Singleton<MpiioFS>::GetInstance();
  return fs_api->WaitAll(count, req, status);
}

/**
 * Metadata functions
 */
int HERMES_DECL(MPI_File_open)(MPI_Comm comm, const char *filename, int amode,
                               MPI_Info info, MPI_File *fh) {
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<MpiioFS>::GetInstance();
  if (hermes::adapter::IsTracked(filename)) {
    LOG(INFO) << "Intercept MPI_File_open for filename: " << filename
              << " and mode: " << amode << " is tracked." << std::endl;
    AdapterStat stat;
    stat.comm = comm;
    stat.amode = amode;
    stat.info = info;
    File f = fs_api->Open(stat, filename);
    (*fh) = f.mpi_fh_;
    return f.mpi_status_;
  }
  return real_api->MPI_File_open(comm, filename, amode, info, fh);
}

int HERMES_DECL(MPI_File_close)(MPI_File *fh) {
  bool stat_exists;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<MpiioFS>::GetInstance();
  if (IsTracked(fh)) {
    File f; f.mpi_fh_ = *fh; fs_api->_InitFile(f);
    return fs_api->Close(f, stat_exists);
  }
  return real_api->MPI_File_close(fh);
}

int HERMES_DECL(MPI_File_seek)(MPI_File fh, MPI_Offset offset, int whence) {
  bool stat_exists;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<MpiioFS>::GetInstance();
  if (IsTracked(&fh)) {
    File f; f.mpi_fh_ = fh; fs_api->_InitFile(f);
    return fs_api->Seek(f, stat_exists, offset, whence);
  }
  return real_api->MPI_File_seek(fh, offset, whence);
}

int HERMES_DECL(MPI_File_seek_shared)(MPI_File fh, MPI_Offset offset,
                                      int whence) {
  bool stat_exists;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<MpiioFS>::GetInstance();
  if (IsTracked(&fh)) {
    LOG(INFO) << "Intercept MPI_File_seek_shared offset:" << offset
              << " whence:" << whence << "." << std::endl;
    File f; f.mpi_fh_ = fh; fs_api->_InitFile(f);
    return fs_api->SeekShared(f, stat_exists, offset, whence);
  }
  return real_api->MPI_File_seek_shared(fh, offset, whence);
}

int HERMES_DECL(MPI_File_get_position)(MPI_File fh, MPI_Offset *offset) {
  bool stat_exists;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<MpiioFS>::GetInstance();
  if (IsTracked(&fh)) {
    File f; f.mpi_fh_ = fh; fs_api->_InitFile(f);
    return fs_api->Tell(f, stat_exists);
  }
  return real_api->MPI_File_get_position(fh, offset);
}

int HERMES_DECL(MPI_File_read_all)(MPI_File fh, void *buf, int count,
                                   MPI_Datatype datatype, MPI_Status *status) {
  bool stat_exists;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<MpiioFS>::GetInstance();
  if (IsTracked(&fh)) {
    LOG(INFO) << "Intercept MPI_File_read_all." << std::endl;
    File f; f.mpi_fh_ = fh; fs_api->_InitFile(f);
    return fs_api->ReadAll(f, stat_exists, buf, count, datatype, status);
  }
  return real_api->MPI_File_read_all(fh, buf, count, datatype, status);
}
int HERMES_DECL(MPI_File_read_at_all)(MPI_File fh, MPI_Offset offset, void *buf,
                                      int count, MPI_Datatype datatype,
                                      MPI_Status *status) {
  bool stat_exists;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<MpiioFS>::GetInstance();
  if (IsTracked(&fh)) {
    File f; f.mpi_fh_ = fh; fs_api->_InitFile(f);
    return fs_api->ReadAll(f, stat_exists, buf,
                           offset, count, datatype, status);
  }
  return real_api->MPI_File_read_at_all(fh, offset, buf,
                                         count, datatype, status);
}
int HERMES_DECL(MPI_File_read_at)(MPI_File fh, MPI_Offset offset, void *buf,
                                  int count, MPI_Datatype datatype,
                                  MPI_Status *status) {
  bool stat_exists;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<MpiioFS>::GetInstance();
  if (IsTracked(&fh)) {
    File f; f.mpi_fh_ = fh; fs_api->_InitFile(f);
    return fs_api->Read(f, stat_exists, buf, offset, count, datatype, status);
  }
  return real_api->MPI_File_read_at(fh, offset, buf, count, datatype, status);
}
int HERMES_DECL(MPI_File_read)(MPI_File fh, void *buf, int count,
                               MPI_Datatype datatype, MPI_Status *status) {
  bool stat_exists;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<MpiioFS>::GetInstance();
  if (IsTracked(&fh)) {
    File f; f.mpi_fh_ = fh; fs_api->_InitFile(f);
    int ret = fs_api->Read(f, stat_exists, buf, count, datatype, status);
    if (stat_exists) return ret;
  }
  return real_api->MPI_File_read(fh, buf, count, datatype, status);
}
int HERMES_DECL(MPI_File_read_ordered)(MPI_File fh, void *buf, int count,
                                       MPI_Datatype datatype,
                                       MPI_Status *status) {
  bool stat_exists;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<MpiioFS>::GetInstance();
  if (IsTracked(&fh)) {
    File f; f.mpi_fh_ = fh; fs_api->_InitFile(f);
    return fs_api->ReadOrdered(f, stat_exists, buf, count, datatype, status);
  }
  return real_api->MPI_File_read_ordered(fh, buf, count, datatype, status);
}
int HERMES_DECL(MPI_File_read_shared)(MPI_File fh, void *buf, int count,
                                      MPI_Datatype datatype,
                                      MPI_Status *status) {
  bool stat_exists;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<MpiioFS>::GetInstance();
  if (IsTracked(&fh)) {
    LOG(INFO) << "Intercept MPI_File_read_shared." << std::endl;
    File f; f.mpi_fh_ = fh; fs_api->_InitFile(f);
    return fs_api->Read(f, stat_exists, buf, count, datatype, status);
  }
  return real_api->MPI_File_read_shared(fh, buf, count, datatype, status);
}
int HERMES_DECL(MPI_File_write_all)(MPI_File fh, const void *buf, int count,
                                    MPI_Datatype datatype, MPI_Status *status) {
  bool stat_exists;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<MpiioFS>::GetInstance();
  if (IsTracked(&fh)) {
      LOG(INFO) << "Intercept MPI_File_write_all." << std::endl;
      File f; f.mpi_fh_ = fh; fs_api->_InitFile(f);
      int ret = fs_api->WriteAll(f, stat_exists, buf, count, datatype, status);
      if (stat_exists) return ret;
  }
  return real_api->MPI_File_write_all(fh, buf, count, datatype, status);
}
int HERMES_DECL(MPI_File_write_at_all)(MPI_File fh, MPI_Offset offset,
                                       const void *buf, int count,
                                       MPI_Datatype datatype,
                                       MPI_Status *status) {
  bool stat_exists;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<MpiioFS>::GetInstance();
  if (IsTracked(&fh)) {
    File f; f.mpi_fh_ = fh; fs_api->_InitFile(f);
    int ret = fs_api->WriteAll(f, stat_exists, buf,
                               offset, count, datatype, status);
    if (stat_exists) return ret;
  }
  return real_api->MPI_File_write_at_all(fh, offset, buf, count,
                                          datatype, status);
}
int HERMES_DECL(MPI_File_write_at)(MPI_File fh, MPI_Offset offset,
                                   const void *buf, int count,
                                   MPI_Datatype datatype, MPI_Status *status) {
  bool stat_exists;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<MpiioFS>::GetInstance();
  LOG(INFO) << "In MPI_File_write_at" << std::endl;
  if (IsTracked(&fh)) {
    File f; f.mpi_fh_ = fh; fs_api->_InitFile(f);
    return fs_api->Write(f, stat_exists, buf, offset, count, datatype, status);
  }
  return real_api->MPI_File_write_at(fh, offset, buf, count, datatype, status);
}
int HERMES_DECL(MPI_File_write)(MPI_File fh, const void *buf, int count,
                                MPI_Datatype datatype, MPI_Status *status) {
  bool stat_exists;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<MpiioFS>::GetInstance();
  LOG(INFO) << "In MPI_File_write" << std::endl;
  if (IsTracked(&fh)) {
    File f; f.mpi_fh_ = fh; fs_api->_InitFile(f);
    int ret = fs_api->Write(f, stat_exists, buf, count, datatype, status);
    if (stat_exists) return ret;
  }
  return real_api->MPI_File_write(fh, buf, count, datatype, status);
}
int HERMES_DECL(MPI_File_write_ordered)(MPI_File fh, const void *buf, int count,
                                        MPI_Datatype datatype,
                                        MPI_Status *status) {
  bool stat_exists;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<MpiioFS>::GetInstance();
  if (IsTracked(&fh)) {
    File f; f.mpi_fh_ = fh; fs_api->_InitFile(f);
    return fs_api->WriteOrdered(f, stat_exists, buf, count, datatype, status);
  }
  return real_api->MPI_File_write_ordered(fh, buf, count, datatype, status);
}
int HERMES_DECL(MPI_File_write_shared)(MPI_File fh, const void *buf, int count,
                                       MPI_Datatype datatype,
                                       MPI_Status *status) {
  bool stat_exists;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<MpiioFS>::GetInstance();
  if (IsTracked(&fh)) {
    // NOTE(llogan): originally WriteOrdered
    File f; f.mpi_fh_ = fh; fs_api->_InitFile(f);
    return fs_api->Write(f, stat_exists, buf, count, datatype, status);
  }
  return real_api->MPI_File_write_shared(fh, buf, count, datatype, status);
}

/**
 * Async Read/Write
 */
int HERMES_DECL(MPI_File_iread_at)(MPI_File fh, MPI_Offset offset, void *buf,
                                   int count, MPI_Datatype datatype,
                                   MPI_Request *request) {
  bool stat_exists;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<MpiioFS>::GetInstance();
  if (IsTracked(&fh)) {
    File f; f.mpi_fh_ = fh; fs_api->_InitFile(f);
    fs_api->ARead(f, stat_exists, buf, offset, count, datatype, request);
    return MPI_SUCCESS;
  }
  return real_api->MPI_File_iread_at(fh, offset, buf,
                                     count, datatype, request);
}
int HERMES_DECL(MPI_File_iread)(MPI_File fh, void *buf, int count,
                                MPI_Datatype datatype, MPI_Request *request) {
  bool stat_exists;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<MpiioFS>::GetInstance();
  if (IsTracked(&fh)) {
    File f; f.mpi_fh_ = fh; fs_api->_InitFile(f);
    fs_api->ARead(f, stat_exists, buf, count, datatype, request);
  }
  return real_api->MPI_File_iread(fh, buf, count, datatype, request);
}
int HERMES_DECL(MPI_File_iread_shared)(MPI_File fh, void *buf, int count,
                                       MPI_Datatype datatype,
                                       MPI_Request *request) {
  bool stat_exists;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<MpiioFS>::GetInstance();
  if (IsTracked(&fh)) {
    File f; f.mpi_fh_ = fh; fs_api->_InitFile(f);
    fs_api->ARead(f, stat_exists, buf, count, datatype, request);
    return MPI_SUCCESS;
  }
  return real_api->MPI_File_iread_shared(fh, buf, count, datatype, request);
}
int HERMES_DECL(MPI_File_iwrite_at)(MPI_File fh, MPI_Offset offset,
                                    const void *buf, int count,
                                    MPI_Datatype datatype,
                                    MPI_Request *request) {
  bool stat_exists;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<MpiioFS>::GetInstance();
  if (IsTracked(&fh)) {
    File f; f.mpi_fh_ = fh; fs_api->_InitFile(f);
    fs_api->AWrite(f, stat_exists, buf, offset, count, datatype, request);
    return MPI_SUCCESS;
  }
  return real_api->MPI_File_iwrite_at(fh, offset, buf,
                                      count, datatype, request);
}

int HERMES_DECL(MPI_File_iwrite)(MPI_File fh, const void *buf, int count,
                                 MPI_Datatype datatype, MPI_Request *request) {
  bool stat_exists;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<MpiioFS>::GetInstance();
  if (IsTracked(&fh)) {
    File f; f.mpi_fh_ = fh; fs_api->_InitFile(f);
    fs_api->AWrite(f, stat_exists, buf, count, datatype, request);
    return MPI_SUCCESS;
  }
  return real_api->MPI_File_iwrite(fh, buf, count, datatype, request);
}
int HERMES_DECL(MPI_File_iwrite_shared)(MPI_File fh, const void *buf, int count,
                                        MPI_Datatype datatype,
                                        MPI_Request *request) {
  bool stat_exists;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<MpiioFS>::GetInstance();
  if (IsTracked(&fh)) {
    File f; f.mpi_fh_ = fh; fs_api->_InitFile(f);
    fs_api->AWrite(f, stat_exists, buf, count, datatype, request);
    return MPI_SUCCESS;
  }
  return real_api->MPI_File_iwrite_shared(fh, buf, count,
                                          datatype, request);
}

/**
 * Other functions
 */
int HERMES_DECL(MPI_File_sync)(MPI_File fh) {
  bool stat_exists;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<MpiioFS>::GetInstance();
  if (IsTracked(&fh)) {
    File f; f.mpi_fh_ = fh; fs_api->_InitFile(f);
    fs_api->Sync(f, stat_exists);
    return 0;
  }
  return real_api->MPI_File_sync(fh);
}
