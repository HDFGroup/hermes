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

#ifndef HERMES_PUBSUB_ADAPTER_METADATA_MANAGER_H
#define HERMES_PUBSUB_ADAPTER_METADATA_MANAGER_H

/**
 * Standard headers
 */
#include <unordered_map>
#include <mpi.h>

/**
 * Internal headers
 */
#include "datastructures.h"

using hermes::adapter::pubsub::ClientMetadata;

namespace hermes::adapter::pubsub {
/**
 * Metadata manager for PubSub adapter
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
  explicit MetadataManager(bool is_mpi = true)
      : metadata(),
        ref(0) {
    if (is_mpi) {
      MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
    } else {
      // branch exists for testing puropses
      mpi_rank = 0;
    }
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

  bool isClient() {
    return hermes->IsApplicationCore();
  }

  /**
   * \brief Create a metadata entry for pubsub adapter for a given topic.
   * \param topic, std::string&, name of the managed topic.
   * \param stat, ClientMetadata&, current metadata of the topic.
   * \return true, if operation was successful.
   *         false, if operation was unsuccessful.
   */
  bool Create(const std::string& topic, const ClientMetadata& stat);

  /**
   * \brief Update existing metadata entry for pubsub adapter for a given file handler.
   * \param topic, std::string&, name of the managed topic.
   * \param stat, ClientMetadata&, current metadata of the topic to replace previous one.
   * \return true, if operation was successful.
   *         false, if operation was unsuccessful or entry doesn't exist.
   * \remark Update call will not degenerate into a create call if topic is not being tracked.
   */
  bool Update(const std::string& topic, const ClientMetadata& stat);

  /**
   * \brief Delete existing metadata entry for pubsub adapter for a given file handler.
   * \param topic, std::string&, name of the managed topic.
   * \return    true, if operation was successful.
   *            false, if operation was unsuccessful.
   */
  bool Delete(const std::string& topic);

  /**
   * \brief Find existing metadata entry for pubsub adapter for a given file handler.
   * \param topic, std::string&, name of the managed topic.
   * \return The metadata entry if exist.
   *         The bool in pair indicated whether metadata entry exists.
   */
  std::pair<ClientMetadata, bool> Find(const std::string& topic);
};

}  // namespace hermes::adapter::pubsub

#endif  // HERMES_PUBSUB_ADAPTER_METADATA_MANAGER_H
