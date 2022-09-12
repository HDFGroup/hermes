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

#ifndef HERMES_ADAPTER_METADATA_MANAGER_H_
#define HERMES_ADAPTER_METADATA_MANAGER_H_

#include <mpi.h>
#include "constants.h"
#include "enumerations.h"
#include "interceptor.h"

#include <bucket.h>
#include <buffer_pool.h>
#include <hermes_types.h>

namespace hapi = hermes::api;

namespace hermes::adapter {

FlushingMode global_flushing_mode;

class MetadataManager {
 protected:
  int rank;
  int comm_size;
  std::atomic<size_t> ref;
  std::shared_ptr<hapi::Hermes> hermes;
  bool is_mpi_;

 public:
  /**
   * Initialize hermes. Get the kHermesConf from environment else get_env
   * returns NULL which is handled internally by hermes. Initialize hermes in
   * daemon mode. Keep a reference of how many times Initialize is called.
   * Within the adapter, Initialize is called from fopen.
   */

  void InitializeHermes(bool is_mpi) {
    if (ref == 0) {
      is_mpi_ = is_mpi;
      char* async_flush_mode = getenv(kHermesAsyncFlush);

      if (async_flush_mode && async_flush_mode[0] == '1') {
        global_flushing_mode = FlushingMode::kAsynchronous;
      } else {
        global_flushing_mode = FlushingMode::kSynchronous;
      }

      char* hermes_config = getenv(kHermesConf);

      if (is_mpi_) {
        MPI_Comm_rank(MPI_COMM_WORLD, &rank);
        MPI_Comm_size(MPI_COMM_WORLD, &comm_size);
        // TODO(chogan): Need a better way to distinguish between client and
        // daemon. https://github.com/HDFGroup/hermes/issues/206
        if (comm_size > 1) {
          hermes = hermes::InitHermesClient(hermes_config);
        } else {
          is_mpi_ = false;
          hermes = hermes::InitHermesDaemon(hermes_config);
        }
      } else {
        hermes = hermes::InitHermesDaemon(hermes_config);
      }
    }
    ref++;
  }


  /**
   * Finalize hermes and close rpc if reference is equal to one. Else just
   * decrement the ref counter.
   */

  void FinalizeHermes() {
    if (ref == 1) {
      hermes->FinalizeClient();
    }
    ref--;
  }

  /**
   * Get the instance of hermes.
   */

  std::shared_ptr<hapi::Hermes>& GetHermes() { return hermes; }
};

}  // namespace hermes::adapter


#endif  // HERMES_ADAPTER_METADATA_MANAGER_H_
