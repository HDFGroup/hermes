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
#include <string>
#include <dlfcn.h>
#include <iostream>
#include <glog/logging.h>
#include <mpi.h>
#include <mpio.h>

#ifndef MPI_MODE_TRUNCATE
#define MPI_MODE_TRUNCATE 0
#endif

#define REQUIRE_API(api_name) \
  if (api_name == nullptr) { \
    LOG(FATAL) << "HERMES Adapter failed to map symbol: " \
    #api_name << std::endl; \
    std::exit(1); \
   }

extern "C" {
typedef int (*MPI_Init_t)(int * argc, char *** argv);
typedef int (*MPI_Finalize_t)( void);
typedef int (*MPI_Wait_t)(MPI_Request * req, MPI_Status * status);
typedef int (*MPI_Waitall_t)(int count, MPI_Request * req, MPI_Status * status);
typedef int (*MPI_File_open_t)(MPI_Comm comm, const char * filename, int amode, MPI_Info info, MPI_File * fh);
typedef int (*MPI_File_close_t)(MPI_File * fh);
typedef int (*MPI_File_seek_shared_t)(MPI_File fh, MPI_Offset offset, int whence);
typedef int (*MPI_File_seek_t)(MPI_File fh, MPI_Offset offset, int whence);
typedef int (*MPI_File_get_position_t)(MPI_File fh, MPI_Offset * offset);
typedef int (*MPI_File_read_all_t)(MPI_File fh, void * buf, int count, MPI_Datatype datatype, MPI_Status * status);
typedef int (*MPI_File_read_at_all_t)(MPI_File fh, MPI_Offset offset, void * buf, int count, MPI_Datatype datatype, MPI_Status * status);
typedef int (*MPI_File_read_at_t)(MPI_File fh, MPI_Offset offset, void * buf, int count, MPI_Datatype datatype, MPI_Status * status);
typedef int (*MPI_File_read_t)(MPI_File fh, void * buf, int count, MPI_Datatype datatype, MPI_Status * status);
typedef int (*MPI_File_read_ordered_t)(MPI_File fh, void * buf, int count, MPI_Datatype datatype, MPI_Status * status);
typedef int (*MPI_File_read_shared_t)(MPI_File fh, void * buf, int count, MPI_Datatype datatype, MPI_Status * status);
typedef int (*MPI_File_write_all_t)(MPI_File fh, const void * buf, int count, MPI_Datatype datatype, MPI_Status * status);
typedef int (*MPI_File_write_at_all_t)(MPI_File fh, MPI_Offset offset, const void * buf, int count, MPI_Datatype datatype, MPI_Status * status);
typedef int (*MPI_File_write_at_t)(MPI_File fh, MPI_Offset offset, const void * buf, int count, MPI_Datatype datatype, MPI_Status * status);
typedef int (*MPI_File_write_t)(MPI_File fh, const void * buf, int count, MPI_Datatype datatype, MPI_Status * status);
typedef int (*MPI_File_write_ordered_t)(MPI_File fh, const void * buf, int count, MPI_Datatype datatype, MPI_Status * status);
typedef int (*MPI_File_write_shared_t)(MPI_File fh, const void * buf, int count, MPI_Datatype datatype, MPI_Status * status);
typedef int (*MPI_File_iread_at_t)(MPI_File fh, MPI_Offset offset, void * buf, int count, MPI_Datatype datatype, MPI_Request * request);
typedef int (*MPI_File_iread_t)(MPI_File fh, void * buf, int count, MPI_Datatype datatype, MPI_Request * request);
typedef int (*MPI_File_iread_shared_t)(MPI_File fh, void * buf, int count, MPI_Datatype datatype, MPI_Request * request);
typedef int (*MPI_File_iwrite_at_t)(MPI_File fh, MPI_Offset offset, const void * buf, int count, MPI_Datatype datatype, MPI_Request * request);
typedef int (*MPI_File_iwrite_t)(MPI_File fh, const void * buf, int count, MPI_Datatype datatype, MPI_Request * request);
typedef int (*MPI_File_iwrite_shared_t)(MPI_File fh, const void * buf, int count, MPI_Datatype datatype, MPI_Request * request);
typedef int (*MPI_File_sync_t)(MPI_File fh);
}

namespace hermes::adapter::fs {

/** Pointers to the real mpiio API */
class MpiioApi {
 public:
  /** MPI_Init */
  MPI_Init_t MPI_Init = nullptr;
  /** MPI_Finalize */
  MPI_Finalize_t MPI_Finalize = nullptr;
  /** MPI_Wait */
  MPI_Wait_t MPI_Wait = nullptr;
  /** MPI_Waitall */
  MPI_Waitall_t MPI_Waitall = nullptr;
  /** MPI_File_open */
  MPI_File_open_t MPI_File_open = nullptr;
  /** MPI_File_close */
  MPI_File_close_t MPI_File_close = nullptr;
  /** MPI_File_seek_shared */
  MPI_File_seek_shared_t MPI_File_seek_shared = nullptr;
  /** MPI_File_seek */
  MPI_File_seek_t MPI_File_seek = nullptr;
  /** MPI_File_get_position */
  MPI_File_get_position_t MPI_File_get_position = nullptr;
  /** MPI_File_read_all */
  MPI_File_read_all_t MPI_File_read_all = nullptr;
  /** MPI_File_read_at_all */
  MPI_File_read_at_all_t MPI_File_read_at_all = nullptr;
  /** MPI_File_read_at */
  MPI_File_read_at_t MPI_File_read_at = nullptr;
  /** MPI_File_read */
  MPI_File_read_t MPI_File_read = nullptr;
  /** MPI_File_read_ordered */
  MPI_File_read_ordered_t MPI_File_read_ordered = nullptr;
  /** MPI_File_read_shared */
  MPI_File_read_shared_t MPI_File_read_shared = nullptr;
  /** MPI_File_write_all */
  MPI_File_write_all_t MPI_File_write_all = nullptr;
  /** MPI_File_write_at_all */
  MPI_File_write_at_all_t MPI_File_write_at_all = nullptr;
  /** MPI_File_write_at */
  MPI_File_write_at_t MPI_File_write_at = nullptr;
  /** MPI_File_write */
  MPI_File_write_t MPI_File_write = nullptr;
  /** MPI_File_write_ordered */
  MPI_File_write_ordered_t MPI_File_write_ordered = nullptr;
  /** MPI_File_write_shared */
  MPI_File_write_shared_t MPI_File_write_shared = nullptr;
  /** MPI_File_iread_at */
  MPI_File_iread_at_t MPI_File_iread_at = nullptr;
  /** MPI_File_iread */
  MPI_File_iread_t MPI_File_iread = nullptr;
  /** MPI_File_iread_shared */
  MPI_File_iread_shared_t MPI_File_iread_shared = nullptr;
  /** MPI_File_iwrite_at */
  MPI_File_iwrite_at_t MPI_File_iwrite_at = nullptr;
  /** MPI_File_iwrite */
  MPI_File_iwrite_t MPI_File_iwrite = nullptr;
  /** MPI_File_iwrite_shared */
  MPI_File_iwrite_shared_t MPI_File_iwrite_shared = nullptr;
  /** MPI_File_sync */
  MPI_File_sync_t MPI_File_sync = nullptr;

  MpiioApi() {
    void *is_intercepted = (void*)dlsym(RTLD_DEFAULT, "mpiio_intercepted");
    if (is_intercepted) {
      MPI_Init = (MPI_Init_t)dlsym(RTLD_NEXT, "MPI_Init");
    } else {
      MPI_Init = (MPI_Init_t)dlsym(RTLD_DEFAULT, "MPI_Init");
    }
    REQUIRE_API(MPI_Init)
    if (is_intercepted) {
      MPI_Finalize = (MPI_Finalize_t)dlsym(RTLD_NEXT, "MPI_Finalize");
    } else {
      MPI_Finalize = (MPI_Finalize_t)dlsym(RTLD_DEFAULT, "MPI_Finalize");
    }
    REQUIRE_API(MPI_Finalize)
    if (is_intercepted) {
      MPI_Wait = (MPI_Wait_t)dlsym(RTLD_NEXT, "MPI_Wait");
    } else {
      MPI_Wait = (MPI_Wait_t)dlsym(RTLD_DEFAULT, "MPI_Wait");
    }
    REQUIRE_API(MPI_Wait)
    if (is_intercepted) {
      MPI_Waitall = (MPI_Waitall_t)dlsym(RTLD_NEXT, "MPI_Waitall");
    } else {
      MPI_Waitall = (MPI_Waitall_t)dlsym(RTLD_DEFAULT, "MPI_Waitall");
    }
    REQUIRE_API(MPI_Waitall)
    if (is_intercepted) {
      MPI_File_open = (MPI_File_open_t)dlsym(RTLD_NEXT, "MPI_File_open");
    } else {
      MPI_File_open = (MPI_File_open_t)dlsym(RTLD_DEFAULT, "MPI_File_open");
    }
    REQUIRE_API(MPI_File_open)
    if (is_intercepted) {
      MPI_File_close = (MPI_File_close_t)dlsym(RTLD_NEXT, "MPI_File_close");
    } else {
      MPI_File_close = (MPI_File_close_t)dlsym(RTLD_DEFAULT, "MPI_File_close");
    }
    REQUIRE_API(MPI_File_close)
    if (is_intercepted) {
      MPI_File_seek_shared = (MPI_File_seek_shared_t)dlsym(RTLD_NEXT, "MPI_File_seek_shared");
    } else {
      MPI_File_seek_shared = (MPI_File_seek_shared_t)dlsym(RTLD_DEFAULT, "MPI_File_seek_shared");
    }
    REQUIRE_API(MPI_File_seek_shared)
    if (is_intercepted) {
      MPI_File_seek = (MPI_File_seek_t)dlsym(RTLD_NEXT, "MPI_File_seek");
    } else {
      MPI_File_seek = (MPI_File_seek_t)dlsym(RTLD_DEFAULT, "MPI_File_seek");
    }
    REQUIRE_API(MPI_File_seek)
    if (is_intercepted) {
      MPI_File_get_position = (MPI_File_get_position_t)dlsym(RTLD_NEXT, "MPI_File_get_position");
    } else {
      MPI_File_get_position = (MPI_File_get_position_t)dlsym(RTLD_DEFAULT, "MPI_File_get_position");
    }
    REQUIRE_API(MPI_File_get_position)
    if (is_intercepted) {
      MPI_File_read_all = (MPI_File_read_all_t)dlsym(RTLD_NEXT, "MPI_File_read_all");
    } else {
      MPI_File_read_all = (MPI_File_read_all_t)dlsym(RTLD_DEFAULT, "MPI_File_read_all");
    }
    REQUIRE_API(MPI_File_read_all)
    if (is_intercepted) {
      MPI_File_read_at_all = (MPI_File_read_at_all_t)dlsym(RTLD_NEXT, "MPI_File_read_at_all");
    } else {
      MPI_File_read_at_all = (MPI_File_read_at_all_t)dlsym(RTLD_DEFAULT, "MPI_File_read_at_all");
    }
    REQUIRE_API(MPI_File_read_at_all)
    if (is_intercepted) {
      MPI_File_read_at = (MPI_File_read_at_t)dlsym(RTLD_NEXT, "MPI_File_read_at");
    } else {
      MPI_File_read_at = (MPI_File_read_at_t)dlsym(RTLD_DEFAULT, "MPI_File_read_at");
    }
    REQUIRE_API(MPI_File_read_at)
    if (is_intercepted) {
      MPI_File_read = (MPI_File_read_t)dlsym(RTLD_NEXT, "MPI_File_read");
    } else {
      MPI_File_read = (MPI_File_read_t)dlsym(RTLD_DEFAULT, "MPI_File_read");
    }
    REQUIRE_API(MPI_File_read)
    if (is_intercepted) {
      MPI_File_read_ordered = (MPI_File_read_ordered_t)dlsym(RTLD_NEXT, "MPI_File_read_ordered");
    } else {
      MPI_File_read_ordered = (MPI_File_read_ordered_t)dlsym(RTLD_DEFAULT, "MPI_File_read_ordered");
    }
    REQUIRE_API(MPI_File_read_ordered)
    if (is_intercepted) {
      MPI_File_read_shared = (MPI_File_read_shared_t)dlsym(RTLD_NEXT, "MPI_File_read_shared");
    } else {
      MPI_File_read_shared = (MPI_File_read_shared_t)dlsym(RTLD_DEFAULT, "MPI_File_read_shared");
    }
    REQUIRE_API(MPI_File_read_shared)
    if (is_intercepted) {
      MPI_File_write_all = (MPI_File_write_all_t)dlsym(RTLD_NEXT, "MPI_File_write_all");
    } else {
      MPI_File_write_all = (MPI_File_write_all_t)dlsym(RTLD_DEFAULT, "MPI_File_write_all");
    }
    REQUIRE_API(MPI_File_write_all)
    if (is_intercepted) {
      MPI_File_write_at_all = (MPI_File_write_at_all_t)dlsym(RTLD_NEXT, "MPI_File_write_at_all");
    } else {
      MPI_File_write_at_all = (MPI_File_write_at_all_t)dlsym(RTLD_DEFAULT, "MPI_File_write_at_all");
    }
    REQUIRE_API(MPI_File_write_at_all)
    if (is_intercepted) {
      MPI_File_write_at = (MPI_File_write_at_t)dlsym(RTLD_NEXT, "MPI_File_write_at");
    } else {
      MPI_File_write_at = (MPI_File_write_at_t)dlsym(RTLD_DEFAULT, "MPI_File_write_at");
    }
    REQUIRE_API(MPI_File_write_at)
    if (is_intercepted) {
      MPI_File_write = (MPI_File_write_t)dlsym(RTLD_NEXT, "MPI_File_write");
    } else {
      MPI_File_write = (MPI_File_write_t)dlsym(RTLD_DEFAULT, "MPI_File_write");
    }
    REQUIRE_API(MPI_File_write)
    if (is_intercepted) {
      MPI_File_write_ordered = (MPI_File_write_ordered_t)dlsym(RTLD_NEXT, "MPI_File_write_ordered");
    } else {
      MPI_File_write_ordered = (MPI_File_write_ordered_t)dlsym(RTLD_DEFAULT, "MPI_File_write_ordered");
    }
    REQUIRE_API(MPI_File_write_ordered)
    if (is_intercepted) {
      MPI_File_write_shared = (MPI_File_write_shared_t)dlsym(RTLD_NEXT, "MPI_File_write_shared");
    } else {
      MPI_File_write_shared = (MPI_File_write_shared_t)dlsym(RTLD_DEFAULT, "MPI_File_write_shared");
    }
    REQUIRE_API(MPI_File_write_shared)
    if (is_intercepted) {
      MPI_File_iread_at = (MPI_File_iread_at_t)dlsym(RTLD_NEXT, "MPI_File_iread_at");
    } else {
      MPI_File_iread_at = (MPI_File_iread_at_t)dlsym(RTLD_DEFAULT, "MPI_File_iread_at");
    }
    REQUIRE_API(MPI_File_iread_at)
    if (is_intercepted) {
      MPI_File_iread = (MPI_File_iread_t)dlsym(RTLD_NEXT, "MPI_File_iread");
    } else {
      MPI_File_iread = (MPI_File_iread_t)dlsym(RTLD_DEFAULT, "MPI_File_iread");
    }
    REQUIRE_API(MPI_File_iread)
    if (is_intercepted) {
      MPI_File_iread_shared = (MPI_File_iread_shared_t)dlsym(RTLD_NEXT, "MPI_File_iread_shared");
    } else {
      MPI_File_iread_shared = (MPI_File_iread_shared_t)dlsym(RTLD_DEFAULT, "MPI_File_iread_shared");
    }
    REQUIRE_API(MPI_File_iread_shared)
    if (is_intercepted) {
      MPI_File_iwrite_at = (MPI_File_iwrite_at_t)dlsym(RTLD_NEXT, "MPI_File_iwrite_at");
    } else {
      MPI_File_iwrite_at = (MPI_File_iwrite_at_t)dlsym(RTLD_DEFAULT, "MPI_File_iwrite_at");
    }
    REQUIRE_API(MPI_File_iwrite_at)
    if (is_intercepted) {
      MPI_File_iwrite = (MPI_File_iwrite_t)dlsym(RTLD_NEXT, "MPI_File_iwrite");
    } else {
      MPI_File_iwrite = (MPI_File_iwrite_t)dlsym(RTLD_DEFAULT, "MPI_File_iwrite");
    }
    REQUIRE_API(MPI_File_iwrite)
    if (is_intercepted) {
      MPI_File_iwrite_shared = (MPI_File_iwrite_shared_t)dlsym(RTLD_NEXT, "MPI_File_iwrite_shared");
    } else {
      MPI_File_iwrite_shared = (MPI_File_iwrite_shared_t)dlsym(RTLD_DEFAULT, "MPI_File_iwrite_shared");
    }
    REQUIRE_API(MPI_File_iwrite_shared)
    if (is_intercepted) {
      MPI_File_sync = (MPI_File_sync_t)dlsym(RTLD_NEXT, "MPI_File_sync");
    } else {
      MPI_File_sync = (MPI_File_sync_t)dlsym(RTLD_DEFAULT, "MPI_File_sync");
    }
    REQUIRE_API(MPI_File_sync)
  }
};
}  // namespace hermes::adapter::mpiio

#undef REQUIRE_API

#include "hermes_shm/util/singleton.h"

/** Simplify access to the stateless MpiioFs Singleton */
#define HERMES_MPIIO_API \
  hermes_shm::EasySingleton<hermes::adapter::fs::MpiioApi>::GetInstance()
#define HERMES_MPIIO_API_T hermes::adapter::fs::MpiioApi*

#endif  // HERMES_ADAPTER_MPIIO_H
