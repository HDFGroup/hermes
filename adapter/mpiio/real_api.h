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

#ifndef HERMES_ADAPTER_MPIIO_H
#define HERMES_ADAPTER_MPIIO_H
#include <dlfcn.h>
#include <glog/logging.h>
#include <mpi.h>
#include <mpio.h>

#include <iostream>
#include <string>

#include "filesystem/filesystem.h"
#include "filesystem/metadata_manager.h"
#include "interceptor.h"

#define REQUIRE_API(api_name)                                       \
  if (real_api->api_name == nullptr) {                              \
    LOG(FATAL) << "HERMES Adapter failed to map symbol: " #api_name \
               << std::endl;                                        \
    exit(1);

extern "C" {
typedef int (*MPI_Init_t)(int *argc, char ***argv);
typedef int (*MPI_Finalize_t)(void);
typedef int (*MPI_Wait_t)(MPI_Request *req, MPI_Status *status);
typedef int (*MPI_Waitall_t)(int count, MPI_Request *req, MPI_Status *status);
typedef int (*MPI_File_open_t)(MPI_Comm comm, const char *filename, int amode,
                               MPI_Info info, MPI_File *fh);
typedef int (*MPI_File_close_t)(MPI_File *fh);
typedef int (*MPI_File_seek_shared_t)(MPI_File fh, MPI_Offset offset,
                                      int whence);
typedef int (*MPI_File_seek_t)(MPI_File fh, MPI_Offset offset, int whence);
typedef int (*MPI_File_get_position_t)(MPI_File fh, MPI_Offset *offset);
typedef int (*MPI_File_read_all_t)(MPI_File fh, void *buf, int count,
                                   MPI_Datatype datatype, MPI_Status *status);
typedef int (*MPI_File_read_at_all_t)(MPI_File fh, MPI_Offset offset, void *buf,
                                      int count, MPI_Datatype datatype,
                                      MPI_Status *status);
typedef int (*MPI_File_read_at_t)(MPI_File fh, MPI_Offset offset, void *buf,
                                  int count, MPI_Datatype datatype,
                                  MPI_Status *status);
typedef int (*MPI_File_read_t)(MPI_File fh, void *buf, int count,
                               MPI_Datatype datatype, MPI_Status *status);
typedef int (*MPI_File_read_ordered_t)(MPI_File fh, void *buf, int count,
                                       MPI_Datatype datatype,
                                       MPI_Status *status);
typedef int (*MPI_File_read_shared_t)(MPI_File fh, void *buf, int count,
                                      MPI_Datatype datatype,
                                      MPI_Status *status);
typedef int (*MPI_File_write_all_t)(MPI_File fh, const void *buf, int count,
                                    MPI_Datatype datatype, MPI_Status *status);
typedef int (*MPI_File_write_at_all_t)(MPI_File fh, MPI_Offset offset,
                                       const void *buf, int count,
                                       MPI_Datatype datatype,
                                       MPI_Status *status);
typedef int (*MPI_File_write_at_t)(MPI_File fh, MPI_Offset offset,
                                   const void *buf, int count,
                                   MPI_Datatype datatype, MPI_Status *status);
typedef int (*MPI_File_write_t)(MPI_File fh, const void *buf, int count,
                                MPI_Datatype datatype, MPI_Status *status);
typedef int (*MPI_File_write_ordered_t)(MPI_File fh, const void *buf, int count,
                                        MPI_Datatype datatype,
                                        MPI_Status *status);
typedef int (*MPI_File_write_shared_t)(MPI_File fh, const void *buf, int count,
                                       MPI_Datatype datatype,
                                       MPI_Status *status);
typedef int (*MPI_File_iread_at_t)(MPI_File fh, MPI_Offset offset, void *buf,
                                   int count, MPI_Datatype datatype,
                                   MPI_Request *request);
typedef int (*MPI_File_iread_t)(MPI_File fh, void *buf, int count,
                                MPI_Datatype datatype, MPI_Request *request);
typedef int (*MPI_File_iread_shared_t)(MPI_File fh, void *buf, int count,
                                       MPI_Datatype datatype,
                                       MPI_Request *request);
typedef int (*MPI_File_iwrite_at_t)(MPI_File fh, MPI_Offset offset,
                                    const void *buf, int count,
                                    MPI_Datatype datatype,
                                    MPI_Request *request);
typedef int (*MPI_File_iwrite_t)(MPI_File fh, const void *buf, int count,
                                 MPI_Datatype datatype, MPI_Request *request);
typedef int (*MPI_File_iwrite_shared_t)(MPI_File fh, const void *buf, int count,
                                        MPI_Datatype datatype,
                                        MPI_Request *request);
typedef int (*MPI_File_sync_t)(MPI_File fh);
}

namespace hermes::adapter::mpiio {
/**
   A class to represent MPIIO API
*/
class API {
 public:
  /** MPI_Init */
  int (*MPI_Init)(int *argc, char ***argv) = nullptr;
  /** MPI_Finalize */
  int (*MPI_Finalize)(void) = nullptr;
  /** MPI_Wait */
  int (*MPI_Wait)(MPI_Request *req, MPI_Status *status) = nullptr;
  /** MPI_Waitall */
  int (*MPI_Waitall)(int count, MPI_Request *req, MPI_Status *status) = nullptr;
  /** MPI_File_open */
  int (*MPI_File_open)(MPI_Comm comm, const char *filename, int amode,
                       MPI_Info info, MPI_File *fh) = nullptr;
  /** MPI_File_close */
  int (*MPI_File_close)(MPI_File *fh) = nullptr;
  /** MPI_File_seek_shared */
  int (*MPI_File_seek_shared)(MPI_File fh, MPI_Offset offset,
                              int whence) = nullptr;
  /** MPI_File_seek */
  int (*MPI_File_seek)(MPI_File fh, MPI_Offset offset, int whence) = nullptr;
  /** MPI_File_get_position */
  int (*MPI_File_get_position)(MPI_File fh, MPI_Offset *offset) = nullptr;
  /** MPI_File_read_all */
  int (*MPI_File_read_all)(MPI_File fh, void *buf, int count,
                           MPI_Datatype datatype, MPI_Status *status) = nullptr;
  /** MPI_File_read_at_all */
  int (*MPI_File_read_at_all)(MPI_File fh, MPI_Offset offset, void *buf,
                              int count, MPI_Datatype datatype,
                              MPI_Status *status) = nullptr;
  /** MPI_File_read_at */
  int (*MPI_File_read_at)(MPI_File fh, MPI_Offset offset, void *buf, int count,
                          MPI_Datatype datatype, MPI_Status *status) = nullptr;
  /** MPI_File_read */
  int (*MPI_File_read)(MPI_File fh, void *buf, int count, MPI_Datatype datatype,
                       MPI_Status *status) = nullptr;
  /** MPI_File_read_ordered */
  int (*MPI_File_read_ordered)(MPI_File fh, void *buf, int count,
                               MPI_Datatype datatype,
                               MPI_Status *status) = nullptr;
  /** MPI_File_read_shared */
  int (*MPI_File_read_shared)(MPI_File fh, void *buf, int count,
                              MPI_Datatype datatype,
                              MPI_Status *status) = nullptr;
  /** MPI_File_write_all */
  int (*MPI_File_write_all)(MPI_File fh, const void *buf, int count,
                            MPI_Datatype datatype,
                            MPI_Status *status) = nullptr;
  /** MPI_File_write_at_all */
  int (*MPI_File_write_at_all)(MPI_File fh, MPI_Offset offset, const void *buf,
                               int count, MPI_Datatype datatype,
                               MPI_Status *status) = nullptr;
  /** MPI_File_write_at */
  int (*MPI_File_write_at)(MPI_File fh, MPI_Offset offset, const void *buf,
                           int count, MPI_Datatype datatype,
                           MPI_Status *status) = nullptr;
  /** MPI_File_write */
  int (*MPI_File_write)(MPI_File fh, const void *buf, int count,
                        MPI_Datatype datatype, MPI_Status *status) = nullptr;
  /** MPI_File_write_ordered */
  int (*MPI_File_write_ordered)(MPI_File fh, const void *buf, int count,
                                MPI_Datatype datatype,
                                MPI_Status *status) = nullptr;
  /** MPI_File_write_shared */
  int (*MPI_File_write_shared)(MPI_File fh, const void *buf, int count,
                               MPI_Datatype datatype,
                               MPI_Status *status) = nullptr;
  /** MPI_File_iread_at */
  int (*MPI_File_iread_at)(MPI_File fh, MPI_Offset offset, void *buf, int count,
                           MPI_Datatype datatype,
                           MPI_Request *request) = nullptr;
  /** MPI_File_iread */
  int (*MPI_File_iread)(MPI_File fh, void *buf, int count,
                        MPI_Datatype datatype, MPI_Request *request) = nullptr;
  /** MPI_File_iread_shared */
  int (*MPI_File_iread_shared)(MPI_File fh, void *buf, int count,
                               MPI_Datatype datatype,
                               MPI_Request *request) = nullptr;
  /** MPI_File_iwrite_at */
  int (*MPI_File_iwrite_at)(MPI_File fh, MPI_Offset offset, const void *buf,
                            int count, MPI_Datatype datatype,
                            MPI_Request *request) = nullptr;
  /** MPI_File_iwrite */
  int (*MPI_File_iwrite)(MPI_File fh, const void *buf, int count,
                         MPI_Datatype datatype, MPI_Request *request) = nullptr;
  /** MPI_File_iwrite_shared */
  int (*MPI_File_iwrite_shared)(MPI_File fh, const void *buf, int count,
                                MPI_Datatype datatype,
                                MPI_Request *request) = nullptr;
  /** MPI_File_sync */
  int (*MPI_File_sync)(MPI_File fh) = nullptr;

  /** API constructor that intercepts MPI API calls */
  API() {
    void *is_intercepted = (void *)dlsym(RTLD_DEFAULT, "mpiio_intercepted");
    if (is_intercepted) {
      MPI_Init = (MPI_Init_t)dlsym(RTLD_NEXT, "MPI_Init");
    } else {
      MPI_Init = (MPI_Init_t)dlsym(RTLD_DEFAULT, "MPI_Init");
    }
    if (is_intercepted) {
      MPI_Finalize = (MPI_Finalize_t)dlsym(RTLD_NEXT, "MPI_Finalize");
    } else {
      MPI_Finalize = (MPI_Finalize_t)dlsym(RTLD_DEFAULT, "MPI_Finalize");
    }
    if (is_intercepted) {
      MPI_Wait = (MPI_Wait_t)dlsym(RTLD_NEXT, "MPI_Wait");
    } else {
      MPI_Wait = (MPI_Wait_t)dlsym(RTLD_DEFAULT, "MPI_Wait");
    }
    if (is_intercepted) {
      MPI_Waitall = (MPI_Waitall_t)dlsym(RTLD_NEXT, "MPI_Waitall");
    } else {
      MPI_Waitall = (MPI_Waitall_t)dlsym(RTLD_DEFAULT, "MPI_Waitall");
    }
    if (is_intercepted) {
      MPI_File_open = (MPI_File_open_t)dlsym(RTLD_NEXT, "MPI_File_open");
    } else {
      MPI_File_open = (MPI_File_open_t)dlsym(RTLD_DEFAULT, "MPI_File_open");
    }
    if (is_intercepted) {
      MPI_File_close = (MPI_File_close_t)dlsym(RTLD_NEXT, "MPI_File_close");
    } else {
      MPI_File_close = (MPI_File_close_t)dlsym(RTLD_DEFAULT, "MPI_File_close");
    }
    if (is_intercepted) {
      MPI_File_seek_shared =
          (MPI_File_seek_shared_t)dlsym(RTLD_NEXT, "MPI_File_seek_shared");
    } else {
      MPI_File_seek_shared =
          (MPI_File_seek_shared_t)dlsym(RTLD_DEFAULT, "MPI_File_seek_shared");
    }
    if (is_intercepted) {
      MPI_File_seek = (MPI_File_seek_t)dlsym(RTLD_NEXT, "MPI_File_seek");
    } else {
      MPI_File_seek = (MPI_File_seek_t)dlsym(RTLD_DEFAULT, "MPI_File_seek");
    }
    if (is_intercepted) {
      MPI_File_get_position =
          (MPI_File_get_position_t)dlsym(RTLD_NEXT, "MPI_File_get_position");
    } else {
      MPI_File_get_position =
          (MPI_File_get_position_t)dlsym(RTLD_DEFAULT, "MPI_File_get_position");
    }
    if (is_intercepted) {
      MPI_File_read_all =
          (MPI_File_read_all_t)dlsym(RTLD_NEXT, "MPI_File_read_all");
    } else {
      MPI_File_read_all =
          (MPI_File_read_all_t)dlsym(RTLD_DEFAULT, "MPI_File_read_all");
    }
    if (is_intercepted) {
      MPI_File_read_at_all =
          (MPI_File_read_at_all_t)dlsym(RTLD_NEXT, "MPI_File_read_at_all");
    } else {
      MPI_File_read_at_all =
          (MPI_File_read_at_all_t)dlsym(RTLD_DEFAULT, "MPI_File_read_at_all");
    }
    if (is_intercepted) {
      MPI_File_read_at =
          (MPI_File_read_at_t)dlsym(RTLD_NEXT, "MPI_File_read_at");
    } else {
      MPI_File_read_at =
          (MPI_File_read_at_t)dlsym(RTLD_DEFAULT, "MPI_File_read_at");
    }
    if (is_intercepted) {
      MPI_File_read = (MPI_File_read_t)dlsym(RTLD_NEXT, "MPI_File_read");
    } else {
      MPI_File_read = (MPI_File_read_t)dlsym(RTLD_DEFAULT, "MPI_File_read");
    }
    if (is_intercepted) {
      MPI_File_read_ordered =
          (MPI_File_read_ordered_t)dlsym(RTLD_NEXT, "MPI_File_read_ordered");
    } else {
      MPI_File_read_ordered =
          (MPI_File_read_ordered_t)dlsym(RTLD_DEFAULT, "MPI_File_read_ordered");
    }
    if (is_intercepted) {
      MPI_File_read_shared =
          (MPI_File_read_shared_t)dlsym(RTLD_NEXT, "MPI_File_read_shared");
    } else {
      MPI_File_read_shared =
          (MPI_File_read_shared_t)dlsym(RTLD_DEFAULT, "MPI_File_read_shared");
    }
    if (is_intercepted) {
      MPI_File_write_all =
          (MPI_File_write_all_t)dlsym(RTLD_NEXT, "MPI_File_write_all");
    } else {
      MPI_File_write_all =
          (MPI_File_write_all_t)dlsym(RTLD_DEFAULT, "MPI_File_write_all");
    }
    if (is_intercepted) {
      MPI_File_write_at_all =
          (MPI_File_write_at_all_t)dlsym(RTLD_NEXT, "MPI_File_write_at_all");
    } else {
      MPI_File_write_at_all =
          (MPI_File_write_at_all_t)dlsym(RTLD_DEFAULT, "MPI_File_write_at_all");
    }
    if (is_intercepted) {
      MPI_File_write_at =
          (MPI_File_write_at_t)dlsym(RTLD_NEXT, "MPI_File_write_at");
    } else {
      MPI_File_write_at =
          (MPI_File_write_at_t)dlsym(RTLD_DEFAULT, "MPI_File_write_at");
    }
    if (is_intercepted) {
      MPI_File_write = (MPI_File_write_t)dlsym(RTLD_NEXT, "MPI_File_write");
    } else {
      MPI_File_write = (MPI_File_write_t)dlsym(RTLD_DEFAULT, "MPI_File_write");
    }
    if (is_intercepted) {
      MPI_File_write_ordered =
          (MPI_File_write_ordered_t)dlsym(RTLD_NEXT, "MPI_File_write_ordered");
    } else {
      MPI_File_write_ordered = (MPI_File_write_ordered_t)dlsym(
          RTLD_DEFAULT, "MPI_File_write_ordered");
    }
    if (is_intercepted) {
      MPI_File_write_shared =
          (MPI_File_write_shared_t)dlsym(RTLD_NEXT, "MPI_File_write_shared");
    } else {
      MPI_File_write_shared =
          (MPI_File_write_shared_t)dlsym(RTLD_DEFAULT, "MPI_File_write_shared");
    }
    if (is_intercepted) {
      MPI_File_iread_at =
          (MPI_File_iread_at_t)dlsym(RTLD_NEXT, "MPI_File_iread_at");
    } else {
      MPI_File_iread_at =
          (MPI_File_iread_at_t)dlsym(RTLD_DEFAULT, "MPI_File_iread_at");
    }
    if (is_intercepted) {
      MPI_File_iread = (MPI_File_iread_t)dlsym(RTLD_NEXT, "MPI_File_iread");
    } else {
      MPI_File_iread = (MPI_File_iread_t)dlsym(RTLD_DEFAULT, "MPI_File_iread");
    }
    if (is_intercepted) {
      MPI_File_iread_shared =
          (MPI_File_iread_shared_t)dlsym(RTLD_NEXT, "MPI_File_iread_shared");
    } else {
      MPI_File_iread_shared =
          (MPI_File_iread_shared_t)dlsym(RTLD_DEFAULT, "MPI_File_iread_shared");
    }
    if (is_intercepted) {
      MPI_File_iwrite_at =
          (MPI_File_iwrite_at_t)dlsym(RTLD_NEXT, "MPI_File_iwrite_at");
    } else {
      MPI_File_iwrite_at =
          (MPI_File_iwrite_at_t)dlsym(RTLD_DEFAULT, "MPI_File_iwrite_at");
    }
    if (is_intercepted) {
      MPI_File_iwrite = (MPI_File_iwrite_t)dlsym(RTLD_NEXT, "MPI_File_iwrite");
    } else {
      MPI_File_iwrite =
          (MPI_File_iwrite_t)dlsym(RTLD_DEFAULT, "MPI_File_iwrite");
    }
    if (is_intercepted) {
      MPI_File_iwrite_shared =
          (MPI_File_iwrite_shared_t)dlsym(RTLD_NEXT, "MPI_File_iwrite_shared");
    } else {
      MPI_File_iwrite_shared = (MPI_File_iwrite_shared_t)dlsym(
          RTLD_DEFAULT, "MPI_File_iwrite_shared");
    }
    if (is_intercepted) {
      MPI_File_sync = (MPI_File_sync_t)dlsym(RTLD_NEXT, "MPI_File_sync");
    } else {
      MPI_File_sync = (MPI_File_sync_t)dlsym(RTLD_DEFAULT, "MPI_File_sync");
    }
  }
};
}  // namespace hermes::adapter::mpiio

#endif  // HERMES_ADAPTER_MPIIO_H
