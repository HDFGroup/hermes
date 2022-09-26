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
#include "interceptor.h"
#include "filesystem/filesystem.h"
#include <mpi.h>
#include <mpio.h>

namespace hermes::adapter::mpiio {

class API {
 public:
  typedef int (*MPI_Init_t)(int * argc, char *** argv);
  int (*MPI_Init)(int * argc, char *** argv) = nullptr;
  typedef int (*MPI_Finalize_t)( void);
  int (*MPI_Finalize)( void) = nullptr;
  typedef int (*MPI_Wait_t)(MPI_Request * req, MPI_Status * status);
  int (*MPI_Wait)(MPI_Request * req, MPI_Status * status) = nullptr;
  typedef int (*MPI_Waitall_t)(int count, MPI_Request * req, MPI_Status * status);
  int (*MPI_Waitall)(int count, MPI_Request * req, MPI_Status * status) = nullptr;
  typedef int (*MPI_File_open_t)(MPI_Comm comm, const char * filename, int amode, MPI_Info info, MPI_File * fh);
  int (*MPI_File_open)(MPI_Comm comm, const char * filename, int amode, MPI_Info info, MPI_File * fh) = nullptr;
  typedef int (*MPI_File_close_t)(MPI_File * fh);
  int (*MPI_File_close)(MPI_File * fh) = nullptr;
  typedef int (*MPI_File_seek_shared_t)(MPI_File fh, MPI_Offset offset, int whence);
  int (*MPI_File_seek_shared)(MPI_File fh, MPI_Offset offset, int whence) = nullptr;
  typedef int (*MPI_File_seek_t)(MPI_File fh, MPI_Offset offset, int whence);
  int (*MPI_File_seek)(MPI_File fh, MPI_Offset offset, int whence) = nullptr;
  typedef int (*MPI_File_get_position_t)(MPI_File fh, MPI_Offset * offset);
  int (*MPI_File_get_position)(MPI_File fh, MPI_Offset * offset) = nullptr;
  typedef int (*MPI_File_read_all_t)(MPI_File fh, void * buf, int count, MPI_Datatype datatype, MPI_Status * status);
  int (*MPI_File_read_all)(MPI_File fh, void * buf, int count, MPI_Datatype datatype, MPI_Status * status) = nullptr;
  typedef int (*MPI_File_read_at_all_t)(MPI_File fh, MPI_Offset offset, void * buf, int count, MPI_Datatype datatype, MPI_Status * status);
  int (*MPI_File_read_at_all)(MPI_File fh, MPI_Offset offset, void * buf, int count, MPI_Datatype datatype, MPI_Status * status) = nullptr;
  typedef int (*MPI_File_read_at_t)(MPI_File fh, MPI_Offset offset, void * buf, int count, MPI_Datatype datatype, MPI_Status * status);
  int (*MPI_File_read_at)(MPI_File fh, MPI_Offset offset, void * buf, int count, MPI_Datatype datatype, MPI_Status * status) = nullptr;
  typedef int (*MPI_File_read_t)(MPI_File fh, void * buf, int count, MPI_Datatype datatype, MPI_Status * status);
  int (*MPI_File_read)(MPI_File fh, void * buf, int count, MPI_Datatype datatype, MPI_Status * status) = nullptr;
  typedef int (*MPI_File_read_ordered_t)(MPI_File fh, void * buf, int count, MPI_Datatype datatype, MPI_Status * status);
  int (*MPI_File_read_ordered)(MPI_File fh, void * buf, int count, MPI_Datatype datatype, MPI_Status * status) = nullptr;
  typedef int (*MPI_File_read_shared_t)(MPI_File fh, void * buf, int count, MPI_Datatype datatype, MPI_Status * status);
  int (*MPI_File_read_shared)(MPI_File fh, void * buf, int count, MPI_Datatype datatype, MPI_Status * status) = nullptr;
  typedef int (*MPI_File_write_all_t)(MPI_File fh, const void * buf, int count, MPI_Datatype datatype, MPI_Status * status);
  int (*MPI_File_write_all)(MPI_File fh, const void * buf, int count, MPI_Datatype datatype, MPI_Status * status) = nullptr;
  typedef int (*MPI_File_write_at_all_t)(MPI_File fh, MPI_Offset offset, const void * buf, int count, MPI_Datatype datatype, MPI_Status * status);
  int (*MPI_File_write_at_all)(MPI_File fh, MPI_Offset offset, const void * buf, int count, MPI_Datatype datatype, MPI_Status * status) = nullptr;
  typedef int (*MPI_File_write_at_t)(MPI_File fh, MPI_Offset offset, const void * buf, int count, MPI_Datatype datatype, MPI_Status * status);
  int (*MPI_File_write_at)(MPI_File fh, MPI_Offset offset, const void * buf, int count, MPI_Datatype datatype, MPI_Status * status) = nullptr;
  typedef int (*MPI_File_write_t)(MPI_File fh, const void * buf, int count, MPI_Datatype datatype, MPI_Status * status);
  int (*MPI_File_write)(MPI_File fh, const void * buf, int count, MPI_Datatype datatype, MPI_Status * status) = nullptr;
  typedef int (*MPI_File_write_ordered_t)(MPI_File fh, const void * buf, int count, MPI_Datatype datatype, MPI_Status * status);
  int (*MPI_File_write_ordered)(MPI_File fh, const void * buf, int count, MPI_Datatype datatype, MPI_Status * status) = nullptr;
  typedef int (*MPI_File_write_shared_t)(MPI_File fh, const void * buf, int count, MPI_Datatype datatype, MPI_Status * status);
  int (*MPI_File_write_shared)(MPI_File fh, const void * buf, int count, MPI_Datatype datatype, MPI_Status * status) = nullptr;
  typedef int (*MPI_File_iread_at_t)(MPI_File fh, MPI_Offset offset, void * buf, int count, MPI_Datatype datatype, MPI_Request * request);
  int (*MPI_File_iread_at)(MPI_File fh, MPI_Offset offset, void * buf, int count, MPI_Datatype datatype, MPI_Request * request) = nullptr;
  typedef int (*MPI_File_iread_t)(MPI_File fh, void * buf, int count, MPI_Datatype datatype, MPI_Request * request);
  int (*MPI_File_iread)(MPI_File fh, void * buf, int count, MPI_Datatype datatype, MPI_Request * request) = nullptr;
  typedef int (*MPI_File_iread_shared_t)(MPI_File fh, void * buf, int count, MPI_Datatype datatype, MPI_Request * request);
  int (*MPI_File_iread_shared)(MPI_File fh, void * buf, int count, MPI_Datatype datatype, MPI_Request * request) = nullptr;
  typedef int (*MPI_File_iwrite_at_t)(MPI_File fh, MPI_Offset offset, const void * buf, int count, MPI_Datatype datatype, MPI_Request * request);
  int (*MPI_File_iwrite_at)(MPI_File fh, MPI_Offset offset, const void * buf, int count, MPI_Datatype datatype, MPI_Request * request) = nullptr;
  typedef int (*MPI_File_iwrite_t)(MPI_File fh, const void * buf, int count, MPI_Datatype datatype, MPI_Request * request);
  int (*MPI_File_iwrite)(MPI_File fh, const void * buf, int count, MPI_Datatype datatype, MPI_Request * request) = nullptr;
  typedef int (*MPI_File_iwrite_shared_t)(MPI_File fh, const void * buf, int count, MPI_Datatype datatype, MPI_Request * request);
  int (*MPI_File_iwrite_shared)(MPI_File fh, const void * buf, int count, MPI_Datatype datatype, MPI_Request * request) = nullptr;
  typedef int (*MPI_File_sync_t)(MPI_File fh);
  int (*MPI_File_sync)(MPI_File fh) = nullptr;
  API() {
    void *is_intercepted = (void*)dlsym(RTLD_DEFAULT, "mpiio_intercepted");
    if (is_intercepted) {
      MPI_Init = (MPI_Init_t)dlsym(RTLD_NEXT, "MPI_Init");
    } else {
      MPI_Init = (MPI_Init_t)dlsym(RTLD_DEFAULT, "MPI_Init");
    }
    if (MPI_Init == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "MPI_Init" << std::endl;
    }
    if (is_intercepted) {
      MPI_Finalize = (MPI_Finalize_t)dlsym(RTLD_NEXT, "MPI_Finalize");
    } else {
      MPI_Finalize = (MPI_Finalize_t)dlsym(RTLD_DEFAULT, "MPI_Finalize");
    }
    if (MPI_Finalize == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "MPI_Finalize" << std::endl;
    }
    if (is_intercepted) {
      MPI_Wait = (MPI_Wait_t)dlsym(RTLD_NEXT, "MPI_Wait");
    } else {
      MPI_Wait = (MPI_Wait_t)dlsym(RTLD_DEFAULT, "MPI_Wait");
    }
    if (MPI_Wait == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "MPI_Wait" << std::endl;
    }
    if (is_intercepted) {
      MPI_Waitall = (MPI_Waitall_t)dlsym(RTLD_NEXT, "MPI_Waitall");
    } else {
      MPI_Waitall = (MPI_Waitall_t)dlsym(RTLD_DEFAULT, "MPI_Waitall");
    }
    if (MPI_Waitall == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "MPI_Waitall" << std::endl;
    }
    if (is_intercepted) {
      MPI_File_open = (MPI_File_open_t)dlsym(RTLD_NEXT, "MPI_File_open");
    } else {
      MPI_File_open = (MPI_File_open_t)dlsym(RTLD_DEFAULT, "MPI_File_open");
    }
    if (MPI_File_open == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "MPI_File_open" << std::endl;
    }
    if (is_intercepted) {
      MPI_File_close = (MPI_File_close_t)dlsym(RTLD_NEXT, "MPI_File_close");
    } else {
      MPI_File_close = (MPI_File_close_t)dlsym(RTLD_DEFAULT, "MPI_File_close");
    }
    if (MPI_File_close == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "MPI_File_close" << std::endl;
    }
    if (is_intercepted) {
      MPI_File_seek_shared = (MPI_File_seek_shared_t)dlsym(RTLD_NEXT, "MPI_File_seek_shared");
    } else {
      MPI_File_seek_shared = (MPI_File_seek_shared_t)dlsym(RTLD_DEFAULT, "MPI_File_seek_shared");
    }
    if (MPI_File_seek_shared == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "MPI_File_seek_shared" << std::endl;
    }
    if (is_intercepted) {
      MPI_File_seek = (MPI_File_seek_t)dlsym(RTLD_NEXT, "MPI_File_seek");
    } else {
      MPI_File_seek = (MPI_File_seek_t)dlsym(RTLD_DEFAULT, "MPI_File_seek");
    }
    if (MPI_File_seek == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "MPI_File_seek" << std::endl;
    }
    if (is_intercepted) {
      MPI_File_get_position = (MPI_File_get_position_t)dlsym(RTLD_NEXT, "MPI_File_get_position");
    } else {
      MPI_File_get_position = (MPI_File_get_position_t)dlsym(RTLD_DEFAULT, "MPI_File_get_position");
    }
    if (MPI_File_get_position == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "MPI_File_get_position" << std::endl;
    }
    if (is_intercepted) {
      MPI_File_read_all = (MPI_File_read_all_t)dlsym(RTLD_NEXT, "MPI_File_read_all");
    } else {
      MPI_File_read_all = (MPI_File_read_all_t)dlsym(RTLD_DEFAULT, "MPI_File_read_all");
    }
    if (MPI_File_read_all == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "MPI_File_read_all" << std::endl;
    }
    if (is_intercepted) {
      MPI_File_read_at_all = (MPI_File_read_at_all_t)dlsym(RTLD_NEXT, "MPI_File_read_at_all");
    } else {
      MPI_File_read_at_all = (MPI_File_read_at_all_t)dlsym(RTLD_DEFAULT, "MPI_File_read_at_all");
    }
    if (MPI_File_read_at_all == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "MPI_File_read_at_all" << std::endl;
    }
    if (is_intercepted) {
      MPI_File_read_at = (MPI_File_read_at_t)dlsym(RTLD_NEXT, "MPI_File_read_at");
    } else {
      MPI_File_read_at = (MPI_File_read_at_t)dlsym(RTLD_DEFAULT, "MPI_File_read_at");
    }
    if (MPI_File_read_at == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "MPI_File_read_at" << std::endl;
    }
    if (is_intercepted) {
      MPI_File_read = (MPI_File_read_t)dlsym(RTLD_NEXT, "MPI_File_read");
    } else {
      MPI_File_read = (MPI_File_read_t)dlsym(RTLD_DEFAULT, "MPI_File_read");
    }
    if (MPI_File_read == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "MPI_File_read" << std::endl;
    }
    if (is_intercepted) {
      MPI_File_read_ordered = (MPI_File_read_ordered_t)dlsym(RTLD_NEXT, "MPI_File_read_ordered");
    } else {
      MPI_File_read_ordered = (MPI_File_read_ordered_t)dlsym(RTLD_DEFAULT, "MPI_File_read_ordered");
    }
    if (MPI_File_read_ordered == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "MPI_File_read_ordered" << std::endl;
    }
    if (is_intercepted) {
      MPI_File_read_shared = (MPI_File_read_shared_t)dlsym(RTLD_NEXT, "MPI_File_read_shared");
    } else {
      MPI_File_read_shared = (MPI_File_read_shared_t)dlsym(RTLD_DEFAULT, "MPI_File_read_shared");
    }
    if (MPI_File_read_shared == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "MPI_File_read_shared" << std::endl;
    }
    if (is_intercepted) {
      MPI_File_write_all = (MPI_File_write_all_t)dlsym(RTLD_NEXT, "MPI_File_write_all");
    } else {
      MPI_File_write_all = (MPI_File_write_all_t)dlsym(RTLD_DEFAULT, "MPI_File_write_all");
    }
    if (MPI_File_write_all == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "MPI_File_write_all" << std::endl;
    }
    if (is_intercepted) {
      MPI_File_write_at_all = (MPI_File_write_at_all_t)dlsym(RTLD_NEXT, "MPI_File_write_at_all");
    } else {
      MPI_File_write_at_all = (MPI_File_write_at_all_t)dlsym(RTLD_DEFAULT, "MPI_File_write_at_all");
    }
    if (MPI_File_write_at_all == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "MPI_File_write_at_all" << std::endl;
    }
    if (is_intercepted) {
      MPI_File_write_at = (MPI_File_write_at_t)dlsym(RTLD_NEXT, "MPI_File_write_at");
    } else {
      MPI_File_write_at = (MPI_File_write_at_t)dlsym(RTLD_DEFAULT, "MPI_File_write_at");
    }
    if (MPI_File_write_at == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "MPI_File_write_at" << std::endl;
    }
    if (is_intercepted) {
      MPI_File_write = (MPI_File_write_t)dlsym(RTLD_NEXT, "MPI_File_write");
    } else {
      MPI_File_write = (MPI_File_write_t)dlsym(RTLD_DEFAULT, "MPI_File_write");
    }
    if (MPI_File_write == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "MPI_File_write" << std::endl;
    }
    if (is_intercepted) {
      MPI_File_write_ordered = (MPI_File_write_ordered_t)dlsym(RTLD_NEXT, "MPI_File_write_ordered");
    } else {
      MPI_File_write_ordered = (MPI_File_write_ordered_t)dlsym(RTLD_DEFAULT, "MPI_File_write_ordered");
    }
    if (MPI_File_write_ordered == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "MPI_File_write_ordered" << std::endl;
    }
    if (is_intercepted) {
      MPI_File_write_shared = (MPI_File_write_shared_t)dlsym(RTLD_NEXT, "MPI_File_write_shared");
    } else {
      MPI_File_write_shared = (MPI_File_write_shared_t)dlsym(RTLD_DEFAULT, "MPI_File_write_shared");
    }
    if (MPI_File_write_shared == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "MPI_File_write_shared" << std::endl;
    }
    if (is_intercepted) {
      MPI_File_iread_at = (MPI_File_iread_at_t)dlsym(RTLD_NEXT, "MPI_File_iread_at");
    } else {
      MPI_File_iread_at = (MPI_File_iread_at_t)dlsym(RTLD_DEFAULT, "MPI_File_iread_at");
    }
    if (MPI_File_iread_at == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "MPI_File_iread_at" << std::endl;
    }
    if (is_intercepted) {
      MPI_File_iread = (MPI_File_iread_t)dlsym(RTLD_NEXT, "MPI_File_iread");
    } else {
      MPI_File_iread = (MPI_File_iread_t)dlsym(RTLD_DEFAULT, "MPI_File_iread");
    }
    if (MPI_File_iread == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "MPI_File_iread" << std::endl;
    }
    if (is_intercepted) {
      MPI_File_iread_shared = (MPI_File_iread_shared_t)dlsym(RTLD_NEXT, "MPI_File_iread_shared");
    } else {
      MPI_File_iread_shared = (MPI_File_iread_shared_t)dlsym(RTLD_DEFAULT, "MPI_File_iread_shared");
    }
    if (MPI_File_iread_shared == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "MPI_File_iread_shared" << std::endl;
    }
    if (is_intercepted) {
      MPI_File_iwrite_at = (MPI_File_iwrite_at_t)dlsym(RTLD_NEXT, "MPI_File_iwrite_at");
    } else {
      MPI_File_iwrite_at = (MPI_File_iwrite_at_t)dlsym(RTLD_DEFAULT, "MPI_File_iwrite_at");
    }
    if (MPI_File_iwrite_at == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "MPI_File_iwrite_at" << std::endl;
    }
    if (is_intercepted) {
      MPI_File_iwrite = (MPI_File_iwrite_t)dlsym(RTLD_NEXT, "MPI_File_iwrite");
    } else {
      MPI_File_iwrite = (MPI_File_iwrite_t)dlsym(RTLD_DEFAULT, "MPI_File_iwrite");
    }
    if (MPI_File_iwrite == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "MPI_File_iwrite" << std::endl;
    }
    if (is_intercepted) {
      MPI_File_iwrite_shared = (MPI_File_iwrite_shared_t)dlsym(RTLD_NEXT, "MPI_File_iwrite_shared");
    } else {
      MPI_File_iwrite_shared = (MPI_File_iwrite_shared_t)dlsym(RTLD_DEFAULT, "MPI_File_iwrite_shared");
    }
    if (MPI_File_iwrite_shared == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "MPI_File_iwrite_shared" << std::endl;
    }
    if (is_intercepted) {
      MPI_File_sync = (MPI_File_sync_t)dlsym(RTLD_NEXT, "MPI_File_sync");
    } else {
      MPI_File_sync = (MPI_File_sync_t)dlsym(RTLD_DEFAULT, "MPI_File_sync");
    }
    if (MPI_File_sync == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "MPI_File_sync" << std::endl;
    }
  }
};
}  // namespace hermes::adapter::mpiio

#endif  // HERMES_ADAPTER_MPIIO_H
