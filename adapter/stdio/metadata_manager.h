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

#ifndef HERMES_ADAPTER_METADATA_MANAGER_H
#define HERMES_ADAPTER_METADATA_MANAGER_H

#include <ftw.h>
#include <cstdio>
#include <unordered_map>

#include <mpi.h>

#include "constants.h"
#include "enumerations.h"
#include "interceptor.h"
#include "stdio/constants.h"
#include "stdio/datastructures.h"

namespace hermes::adapter::stdio {

FlushingMode global_flushing_mode;

/**
 * Metadata manager for STDIO adapter
 */
class MetadataManager {
 private:
  /**
   * Private members
   */
  /**
   * Maintain a local metadata FileID structure mapped to Adapter Stats.
   */
  std::unordered_map<FileID, AdapterStat> metadata;
  /**
   * hermes attribute to initialize Hermes
   */
  std::shared_ptr<hapi::Hermes> hermes;
  /**
   * references of how many times hermes was tried to initialize.
   */
  std::atomic<size_t> ref;
  /**
   * MPI attributes
   */
  bool is_mpi;
  int rank;
  int comm_size;

 public:
  /**
   * Constructor
   */
  MetadataManager()
      : metadata(),
        ref(0),
        is_mpi(false),
        rank(0),
        comm_size(1) {}
  /**
   * Get the instance of hermes.
   */
  std::shared_ptr<hapi::Hermes>& GetHermes() { return hermes; }


  /**
   * Initialize hermes. Get the kHermesConf from environment else get_env
   * returns NULL which is handled internally by hermes. Initialize hermes in
   * daemon mode. Keep a reference of how many times Initialize is called.
   * Within the adapter, Initialize is called from fopen.
   */
  void InitializeHermes(bool is_mpi = false) {
    if (ref == 0) {
      this->is_mpi = is_mpi;
      char* async_flush_mode = getenv(kHermesAsyncFlush);

      if (async_flush_mode && async_flush_mode[0] == '1') {
        global_flushing_mode = FlushingMode::kAsynchronous;
      } else {
        global_flushing_mode = FlushingMode::kSynchronous;
      }

      char* hermes_config = getenv(kHermesConf);

      if (this->is_mpi) {
        MPI_Comm_rank(MPI_COMM_WORLD, &rank);
        MPI_Comm_size(MPI_COMM_WORLD, &comm_size);
        // TODO(chogan): Need a better way to distinguish between client and
        // daemon. https://github.com/HDFGroup/hermes/issues/206
        if (comm_size > 1) {
          hermes = hermes::InitHermesClient(hermes_config);
        } else {
          this->is_mpi = false;
          hermes = hermes::InitHermesDaemon(hermes_config);
        }
      } else {
        hermes = hermes::InitHermesDaemon(hermes_config);
      }
      INTERCEPTOR_LIST->SetupAdapterMode();
    }
    ref++;
  }
  /**
   * Finalize hermes and close rpc if reference is equal to one. Else just
   * decrement the ref counter.
   */
  void FinalizeHermes() {
    if (ref == 1) {
      if (this->is_mpi) {
        MPI_Barrier(MPI_COMM_WORLD);
        char *stop_daemon = getenv(kStopDaemon);
        bool shutdown_daemon = true;

        if (stop_daemon && stop_daemon[0] == '0') {
          shutdown_daemon = false;
        }

        hermes->FinalizeClient(shutdown_daemon);
      } else {
        hermes->Finalize(true);
      }
    }
    ref--;
  }
  /**
   * Convert file handler to FileID using the stat.
   */
  FileID Convert(FILE* fh);

  /**
   * Create a metadata entry for STDIO adapter for a given file handler.
   * @param fh, FILE*, original file handler of the file on the destination
   * filesystem.
   * @param stat, AdapterStat, STDIO Adapter version of Stat data structure.
   * @return    true, if operation was successful.
   *            false, if operation was unsuccessful.
   */
  bool Create(FILE* fh, const AdapterStat& stat);

  /**
   * Update existing metadata entry for STDIO adapter for a given file handler.
   * @param fh, FILE*, original file handler of the file on the destination.
   * @param stat, AdapterStat, STDIO Adapter version of Stat data structure.
   * @return    true, if operation was successful.
   *            false, if operation was unsuccessful or entry doesn't exist.
   */
  bool Update(FILE* fh, const AdapterStat& stat);

  /**
   * Delete existing metadata entry for STDIO adapter for a given file handler.
   * @param fh, FILE*, original file handler of the file on the destination.
   * @return    true, if operation was successful.
   *            false, if operation was unsuccessful.
   */
  bool Delete(FILE* fh);

  /**
   * Find existing metadata entry for STDIO adapter for a given file handler.
   * @param fh, FILE*, original file handler of the file on the destination.
   * @return    The metadata entry if exist.
   *            The bool in pair indicated whether metadata entry exists.
   */
  std::pair<AdapterStat, bool> Find(FILE* fh);
};
}  // namespace hermes::adapter::stdio

#endif  // HERMES_ADAPTER_METADATA_MANAGER_H
