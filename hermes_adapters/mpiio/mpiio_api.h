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
#include "hermes_shm/util/logging.h"
#include <mpi.h>
#ifdef HERMES_MPICH
#include <mpio.h>
#endif
#include "hermes_adapters/real_api.h"

#ifndef MPI_MODE_TRUNCATE
#define MPI_MODE_TRUNCATE 0
#endif

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

namespace hermes::adapter {

/** Pointers to the real mpiio API */
class MpiioApi : public RealApi {
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

  MpiioApi() : RealApi("MPI_Init", "mpiio_intercepted") {
    MPI_Init = (MPI_Init_t)dlsym(real_lib_, "MPI_Init");
    REQUIRE_API(MPI_Init)
    MPI_Finalize = (MPI_Finalize_t)dlsym(real_lib_, "MPI_Finalize");
    REQUIRE_API(MPI_Finalize)
    MPI_Wait = (MPI_Wait_t)dlsym(real_lib_, "MPI_Wait");
    REQUIRE_API(MPI_Wait)
    MPI_Waitall = (MPI_Waitall_t)dlsym(real_lib_, "MPI_Waitall");
    REQUIRE_API(MPI_Waitall)
    MPI_File_open = (MPI_File_open_t)dlsym(real_lib_, "MPI_File_open");
    REQUIRE_API(MPI_File_open)
    MPI_File_close = (MPI_File_close_t)dlsym(real_lib_, "MPI_File_close");
    REQUIRE_API(MPI_File_close)
    MPI_File_seek_shared = (MPI_File_seek_shared_t)dlsym(real_lib_, "MPI_File_seek_shared");
    REQUIRE_API(MPI_File_seek_shared)
    MPI_File_seek = (MPI_File_seek_t)dlsym(real_lib_, "MPI_File_seek");
    REQUIRE_API(MPI_File_seek)
    MPI_File_get_position = (MPI_File_get_position_t)dlsym(real_lib_, "MPI_File_get_position");
    REQUIRE_API(MPI_File_get_position)
    MPI_File_read_all = (MPI_File_read_all_t)dlsym(real_lib_, "MPI_File_read_all");
    REQUIRE_API(MPI_File_read_all)
    MPI_File_read_at_all = (MPI_File_read_at_all_t)dlsym(real_lib_, "MPI_File_read_at_all");
    REQUIRE_API(MPI_File_read_at_all)
    MPI_File_read_at = (MPI_File_read_at_t)dlsym(real_lib_, "MPI_File_read_at");
    REQUIRE_API(MPI_File_read_at)
    MPI_File_read = (MPI_File_read_t)dlsym(real_lib_, "MPI_File_read");
    REQUIRE_API(MPI_File_read)
    MPI_File_read_ordered = (MPI_File_read_ordered_t)dlsym(real_lib_, "MPI_File_read_ordered");
    REQUIRE_API(MPI_File_read_ordered)
    MPI_File_read_shared = (MPI_File_read_shared_t)dlsym(real_lib_, "MPI_File_read_shared");
    REQUIRE_API(MPI_File_read_shared)
    MPI_File_write_all = (MPI_File_write_all_t)dlsym(real_lib_, "MPI_File_write_all");
    REQUIRE_API(MPI_File_write_all)
    MPI_File_write_at_all = (MPI_File_write_at_all_t)dlsym(real_lib_, "MPI_File_write_at_all");
    REQUIRE_API(MPI_File_write_at_all)
    MPI_File_write_at = (MPI_File_write_at_t)dlsym(real_lib_, "MPI_File_write_at");
    REQUIRE_API(MPI_File_write_at)
    MPI_File_write = (MPI_File_write_t)dlsym(real_lib_, "MPI_File_write");
    REQUIRE_API(MPI_File_write)
    MPI_File_write_ordered = (MPI_File_write_ordered_t)dlsym(real_lib_, "MPI_File_write_ordered");
    REQUIRE_API(MPI_File_write_ordered)
    MPI_File_write_shared = (MPI_File_write_shared_t)dlsym(real_lib_, "MPI_File_write_shared");
    REQUIRE_API(MPI_File_write_shared)
    MPI_File_iread_at = (MPI_File_iread_at_t)dlsym(real_lib_, "MPI_File_iread_at");
    REQUIRE_API(MPI_File_iread_at)
    MPI_File_iread = (MPI_File_iread_t)dlsym(real_lib_, "MPI_File_iread");
    REQUIRE_API(MPI_File_iread)
    MPI_File_iread_shared = (MPI_File_iread_shared_t)dlsym(real_lib_, "MPI_File_iread_shared");
    REQUIRE_API(MPI_File_iread_shared)
    MPI_File_iwrite_at = (MPI_File_iwrite_at_t)dlsym(real_lib_, "MPI_File_iwrite_at");
    REQUIRE_API(MPI_File_iwrite_at)
    MPI_File_iwrite = (MPI_File_iwrite_t)dlsym(real_lib_, "MPI_File_iwrite");
    REQUIRE_API(MPI_File_iwrite)
    MPI_File_iwrite_shared = (MPI_File_iwrite_shared_t)dlsym(real_lib_, "MPI_File_iwrite_shared");
    REQUIRE_API(MPI_File_iwrite_shared)
    MPI_File_sync = (MPI_File_sync_t)dlsym(real_lib_, "MPI_File_sync");
    REQUIRE_API(MPI_File_sync)
  }
};
}  // namespace hermes::adapter::mpiio

#include "hermes_shm/util/singleton.h"

/** Simplify access to the stateless MpiioFs Singleton */
#define HERMES_MPIIO_API \
  hshm::EasySingleton<::hermes::adapter::MpiioApi>::GetInstance()
#define HERMES_MPIIO_API_T hermes::adapter::MpiioApi*

#endif  // HERMES_ADAPTER_MPIIO_H
