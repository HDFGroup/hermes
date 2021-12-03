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

/**
 * Standard headers
 */
#include <ftw.h>
#include <cstdio>
#include <unordered_map>

/**
 * Internal headers
 */
#include <hermes/adapter/pubsub/common/datastructures.h>
#include <mpi.h>

using hermes::adapter::pubsub::ClientMetadata;

namespace hermes::adapter::pubsub {
/**
 * Metadata manager for STDIO adapter
 */
class MetadataManager {
 private:
  /**
   * Private members
   */
  std::unordered_map<std::string, ClientMetadata> metadata;
  /**
   * hermes attribute to initialize Hermes
   */
  std::shared_ptr<hapi::Hermes> hermes;
  /**
   * references of how many times hermes was tried to initialize.
   */
  std::atomic<size_t> ref;

 public:
  int mpi_rank;
  /**
   * Constructor
   */
  MetadataManager()
      : metadata(),
        ref(0){
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
  }
  /**
   * Get the instance of hermes.
   */
  std::shared_ptr<hapi::Hermes>& GetHermes() { return hermes; }


  /**
   * Initialize hermes.
   */
  void InitializeHermes(const char *config_file) {
    if (ref == 0) {
      hermes = hapi::InitHermes(config_file, false, true);
    }
    ref++;
  }

  /**
   * Finalize hermes if reference is equal to one. Else just
   * decrement the ref counter.
   */
  void FinalizeHermes() {
    if (ref == 1) {
      hermes->Finalize();
    }
    ref--;
  }

  bool isClient(){
    return hermes->IsApplicationCore();
  }

  /**
   * Create a metadata entry for STDIO adapter for a given file handler.
   * @param fh, FILE*, original file handler of the file on the destination
   * filesystem.
   * @param stat, AdapterStat, STDIO Adapter version of Stat data structure.
   * @return    true, if operation was successful.
   *            false, if operation was unsuccessful.
   */
  bool Create(const std::string& topic, const ClientMetadata& stat);

  /**
   * Update existing metadata entry for STDIO adapter for a given file handler.
   * @param fh, FILE*, original file handler of the file on the destination.
   * @param stat, AdapterStat, STDIO Adapter version of Stat data structure.
   * @return    true, if operation was successful.
   *            false, if operation was unsuccessful or entry doesn't exist.
   */
  bool Update(const std::string& topic, const ClientMetadata& stat);

  /**
   * Delete existing metadata entry for STDIO adapter for a given file handler.
   * @param fh, FILE*, original file handler of the file on the destination.
   * @return    true, if operation was successful.
   *            false, if operation was unsuccessful.
   */
  bool Delete(const std::string& topic);

  /**
   * Find existing metadata entry for STDIO adapter for a given file handler.
   * @param fh, FILE*, original file handler of the file on the destination.
   * @return    The metadata entry if exist.
   *            The bool in pair indicated whether metadata entry exists.
   */
  std::pair<ClientMetadata, bool> Find(const std::string& topic);
};
}  // namespace hermes::adapter::stdio

#endif  // HERMES_ADAPTER_METADATA_MANAGER_H
