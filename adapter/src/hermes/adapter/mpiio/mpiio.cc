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

#include <hermes/adapter/mpiio.h>
#include <hermes/adapter/mpiio/mapper/mapper_factory.h>

#include <hermes/adapter/interceptor.cc>
#include <hermes/adapter/mpiio/metadata_manager.cc>

#include "mpio.h"
/**
 * Namespace declarations
 */
using hermes::adapter::mpiio::AdapterStat;
using hermes::adapter::mpiio::FileStruct;
using hermes::adapter::mpiio::MapperFactory;
using hermes::adapter::mpiio::MetadataManager;

namespace hapi = hermes::api;
namespace fs = std::experimental::filesystem;
/**
 * Internal Functions.
 */
int simple_open(MPI_Comm &comm, const char *path, int &amode, MPI_Info &info,
                MPI_File *fh) {
  LOG(INFO) << "Open file for filename " << path << " in mode " << amode
            << std::endl;
  int ret = MPI_SUCCESS;
  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
  auto existing = mdm->Find(fh);
  if (!existing.second) {
    LOG(INFO) << "File not opened before by adapter" << std::endl;
    AdapterStat stat;
    stat.ref_count = 1;
    stat.a_mode = amode;
    if (amode & MPI_MODE_APPEND) {
      MPI_File_get_size(*fh, &stat.size);
      stat.ptr = stat.size;
    }
    stat.info = info;
    stat.comm = comm;
    mdm->InitializeHermes();
    hapi::Context ctx;
    stat.st_bkid = std::make_shared<hapi::Bucket>(path, mdm->GetHermes(), ctx);
    mdm->Create(fh, stat);
  } else {
    LOG(INFO) << "File opened before by adapter" << std::endl;
    existing.first.ref_count++;
    mdm->Update(fh, existing.first);
  }
  return ret;
}

int open_internal(MPI_Comm &comm, const char *path, int &amode, MPI_Info &info,
                  MPI_File *fh) {
  int ret;
  MAP_OR_FAIL(MPI_File_open);
  ret = real_MPI_File_open_(comm, path, amode, info, fh);
  if (ret == MPI_SUCCESS) {
    ret = simple_open(comm, path, amode, info, fh);
  }
  return ret;
}
/**
 * MPI
 */
int HERMES_DECL(MPI_Init)(int *argc, char ***argv) {
  MAP_OR_FAIL(MPI_Init);
  int status = real_MPI_Init_(argc, argv);
  if (status == 0) {
    LOG(INFO) << "MPI Init intercepted." << std::endl;
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    mdm->InitializeHermes();
  }
  return status;
}

int HERMES_DECL(MPI_Finalize)(void) {
  LOG(INFO) << "MPI Finalize intercepted." << std::endl;
  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
  mdm->FinalizeHermes();
  MAP_OR_FAIL(MPI_Finalize);
  int status = real_MPI_Finalize_();
  return status;
}
/**
 * Metadata functions
 */
int HERMES_DECL(MPI_File_open)(MPI_Comm comm, const char *filename, int amode,
                               MPI_Info info, MPI_File *fh) {
  int status;
  if (hermes::adapter::IsTracked(filename)) {
    LOG(INFO) << "Intercept fopen for filename: " << filename
              << " and mode: " << amode << " is tracked." << std::endl;
    status = open_internal(comm, filename, amode, info, fh);
  } else {
    MAP_OR_FAIL(MPI_File_open);
    status = real_MPI_File_open_(comm, filename, amode, info, fh);
  }
  return (status);
}

int HERMES_DECL(MPI_File_close)(MPI_File *fh) {
  int ret;

  LOG(INFO) << "Intercept fclose." << std::endl;
  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
  auto existing = mdm->Find(fh);
  if (existing.second) {
    LOG(INFO) << "File handler is opened by adapter." << std::endl;
    hapi::Context ctx;
    if (existing.first.ref_count == 1) {
      auto filename = existing.first.st_bkid->GetName();
      auto persist = INTERCEPTOR_LIST->Persists(filename);
      mdm->Delete(fh);
      const auto &blob_names = existing.first.st_blobs;
      if (!blob_names.empty() && persist) {
        LOG(INFO) << "Adapter flushes " << blob_names.size()
                  << " blobs to filename:" << filename << "." << std::endl;
        INTERCEPTOR_LIST->hermes_flush_exclusion.insert(filename);
        hermes::api::VBucket file_vbucket(filename, mdm->GetHermes(), true,
                                          ctx);
        auto offset_map = std::unordered_map<std::string, hermes::u64>();

        for (const auto &blob_name : blob_names) {
          auto status = file_vbucket.Link(blob_name, filename, ctx);
          if (!status.Failed()) {
            auto page_index = std::stol(blob_name) - 1;
            offset_map.emplace(blob_name, page_index * kPageSize);
          }
        }
        auto trait = hermes::api::FileMappingTrait(filename, offset_map,
                                                   nullptr, NULL, NULL);
        file_vbucket.Attach(&trait, ctx);
        file_vbucket.Delete(ctx);
        existing.first.st_blobs.clear();
        INTERCEPTOR_LIST->hermes_flush_exclusion.erase(filename);
      }
      existing.first.st_bkid->Destroy(ctx);
      mdm->FinalizeHermes();
      if (existing.first.a_mode & MPI_MODE_DELETE_ON_CLOSE) {
        fs::remove(filename);
      }
    } else {
      LOG(INFO) << "File handler is opened by more than one fopen."
                << std::endl;
      existing.first.ref_count--;
      mdm->Update(fh, existing.first);
      existing.first.st_bkid->Release(ctx);
    }
    ret = MPI_SUCCESS;
  } else {
    MAP_OR_FAIL(MPI_File_close);
    ret = real_MPI_File_close_(fh);
  }
  return (ret);
}
int HERMES_DECL(MPI_File_set_view)(MPI_File fh, MPI_Offset disp,
                                   MPI_Datatype etype, MPI_Datatype filetype,
                                   const char *datarep, MPI_Info info) {
  (void)fh;
  (void)disp;
  (void)etype;
  (void)filetype;
  (void)datarep;
  (void)info;
  return 0;
}

/**
 * Sync Read/Write
 */
int HERMES_DECL(MPI_File_read_all_begin)(MPI_File fh, void *buf, int count,
                                         MPI_Datatype datatype) {
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  return 0;
}
int HERMES_DECL(MPI_File_read_all)(MPI_File fh, void *buf, int count,
                                   MPI_Datatype datatype, MPI_Status *status) {
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  (void)status;
  return 0;
}
int HERMES_DECL(MPI_File_read_at_all)(MPI_File fh, MPI_Offset offset, void *buf,
                                      int count, MPI_Datatype datatype,
                                      MPI_Status *status) {
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  (void)status;
  (void)offset;
  return 0;
}
int HERMES_DECL(MPI_File_read_at_all_begin)(MPI_File fh, MPI_Offset offset,
                                            void *buf, int count,
                                            MPI_Datatype datatype) {
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  (void)offset;
  return 0;
}
int HERMES_DECL(MPI_File_read_at)(MPI_File fh, MPI_Offset offset, void *buf,
                                  int count, MPI_Datatype datatype,
                                  MPI_Status *status) {
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  (void)status;
  (void)offset;
  return 0;
}
int HERMES_DECL(MPI_File_read)(MPI_File fh, void *buf, int count,
                               MPI_Datatype datatype, MPI_Status *status) {
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  (void)status;
  return 0;
}
int HERMES_DECL(MPI_File_read_ordered_begin)(MPI_File fh, void *buf, int count,
                                             MPI_Datatype datatype) {
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  return 0;
}
int HERMES_DECL(MPI_File_read_ordered)(MPI_File fh, void *buf, int count,
                                       MPI_Datatype datatype,
                                       MPI_Status *status) {
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  (void)status;
  return 0;
}
int HERMES_DECL(MPI_File_read_shared)(MPI_File fh, void *buf, int count,
                                      MPI_Datatype datatype,
                                      MPI_Status *status) {
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  (void)status;
  return 0;
}
int HERMES_DECL(MPI_File_write_all_begin)(MPI_File fh, const void *buf,
                                          int count, MPI_Datatype datatype) {
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  return 0;
}
int HERMES_DECL(MPI_File_write_all)(MPI_File fh, const void *buf, int count,
                                    MPI_Datatype datatype, MPI_Status *status) {
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  (void)status;
  return 0;
}
int HERMES_DECL(MPI_File_write_at_all_begin)(MPI_File fh, MPI_Offset offset,
                                             const void *buf, int count,
                                             MPI_Datatype datatype) {
  (void)fh;
  (void)offset;
  (void)buf;
  (void)count;
  (void)datatype;
  return 0;
}
int HERMES_DECL(MPI_File_write_at_all)(MPI_File fh, MPI_Offset offset,
                                       const void *buf, int count,
                                       MPI_Datatype datatype,
                                       MPI_Status *status) {
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  (void)status;
  (void)offset;
  return 0;
}
int HERMES_DECL(MPI_File_write_at)(MPI_File fh, MPI_Offset offset,
                                   const void *buf, int count,
                                   MPI_Datatype datatype, MPI_Status *status) {
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  (void)status;
  (void)offset;
  return 0;
}
int HERMES_DECL(MPI_File_write)(MPI_File fh, const void *buf, int count,
                                MPI_Datatype datatype, MPI_Status *status) {
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  (void)status;
  return 0;
}
int HERMES_DECL(MPI_File_write_ordered_begin)(MPI_File fh, const void *buf,
                                              int count,
                                              MPI_Datatype datatype) {
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  return 0;
}
int HERMES_DECL(MPI_File_write_ordered)(MPI_File fh, const void *buf, int count,
                                        MPI_Datatype datatype,
                                        MPI_Status *status) {
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  (void)status;
  return 0;
}
int HERMES_DECL(MPI_File_write_shared)(MPI_File fh, const void *buf, int count,
                                       MPI_Datatype datatype,
                                       MPI_Status *status) {
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  (void)status;
  return 0;
}
/**
 * Async Read/Write
 */
int HERMES_DECL(MPI_File_iread_at)(MPI_File fh, MPI_Offset offset, void *buf,
                                   int count, MPI_Datatype datatype,
                                   MPI_Request *request) {
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  (void)request;
  (void)offset;
  return 0;
}
int HERMES_DECL(MPI_File_iread)(MPI_File fh, void *buf, int count,
                                MPI_Datatype datatype, MPI_Request *request) {
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  (void)request;
  return 0;
}
int HERMES_DECL(MPI_File_iread_shared)(MPI_File fh, void *buf, int count,
                                       MPI_Datatype datatype,
                                       MPI_Request *request) {
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  (void)request;
  return 0;
}
int HERMES_DECL(MPI_File_iwrite_at)(MPI_File fh, MPI_Offset offset,
                                    const void *buf, int count,
                                    MPI_Datatype datatype,
                                    MPI_Request *request) {
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  (void)request;
  (void)offset;
  return 0;
}
int HERMES_DECL(MPI_File_iwrite)(MPI_File fh, const void *buf, int count,
                                 MPI_Datatype datatype, MPI_Request *request) {
  return 0;
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  (void)request;
}
int HERMES_DECL(MPI_File_iwrite_shared)(MPI_File fh, const void *buf, int count,
                                        MPI_Datatype datatype,
                                        MPI_Request *request) {
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  (void)request;
  return 0;
}

/**
 * Other functions
 */
int HERMES_DECL(MPI_File_sync)(MPI_File fh) {
  (void)fh;
  return 0;
}
