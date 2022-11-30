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

/**
 * \mainpage Welcome to Hermes!
 *
 * \section sec_coordinates Important Coordinates
 *
 * - <a href="https://github.com/HDFGroup/hermes">GitHub</a>
 *
 */

#ifndef HERMES_H_
#define HERMES_H_

#include <glog/logging.h>

#include <cstdint>
#include <string>

#include "buffer_pool.h"
#include "hermes_types.h"
#include "metadata_management.h"
#include "rpc.h"

/** \file hermes.h */

namespace hermes {
namespace api {

/** \brief Return the (semantic versioning compatible) version of Hermes.
 *
 * \return A string in the form MAJOR.MINOR.PATCH
 */
std::string GetVersion();

/** Class representing an instance of a Hermes buffering system. */
class Hermes {
 public:
  /** \bool{Hermes is initialized} */
  bool is_initialized;

  // TODO(chogan): Temporarily public to facilitate iterative development.
  Config *config_;
  hermes::SharedMemoryContext context_; /**< shared memory context */
  std::unique_ptr<hermes::CommunicationContext> comm_; /**< communication context */
  std::unique_ptr<hermes::RpcContext> rpc_;  /**< remote procedure call context */
  /** The name of the shared memory segment in which all Hermes data is
   * stored.
   */
  std::string shmem_name_;
  /** The name of the primary RPC server. */
  std::string rpc_server_name_;

  Hermes() {}

  /**
   * Constructor
   * */

  Hermes(Config *config, bool is_daemon, bool is_adapter);

  /** \brief Return \bool{this rank is an application core}
   *
   * An application core is a core or rank on which user code runs as opposed to
   * the Hermes core (or rank) which only runs Hermes services.
   *
   * \return \bool{this rank is an application core}
   */
  bool IsApplicationCore();

  /** \brief Returns \bool{this is the first MPI rank on this node}
   *
   * Hermes assigns numeric IDs to each rank. The first rank on the node is the
   * lowest ID on that node.
   *
   * \return \bool{this is the first MPI rank on this node}
   */
  bool IsFirstRankOnNode();

  /** \brief A barrier across all application processes.
   *
   * Like MPI_Barrier but only involves application ranks.
   */
  void AppBarrier();

  /** \brief Returns the rank of this process.
   *
   * Hermes assigns each application core a unique rank.
   *
   * \return The rank of this process.
   */
  int GetProcessRank();

  /** \brief Return ID of the node this process is running on.
   *
   * Hermes assigns each node a numeric ID.
   *
   * \return The node's ID.
   */
  int GetNodeId();

  /** \brief Returns the total number of application processes.
   *
   * Does not count Hermes processes.
   *
   * \return The number of application processes.
   */
  int GetNumProcesses();

  /** \brief Get an application communicator handle.
   *
   * The handle can be cast to the appropriate type for the communication
   * backend and used in the backend's API calls. For example, when using the
   * MPI communication backend (the default), this function returns a pointer to
   * an MPI_Comm object, which can then be used in any MPI call.
   *
   * \return A void pointer to a communicator handle.
   */
  void *GetAppCommunicator();

  /** \brief Shutdown Hermes.
   *
   * This should be called by every process (application and Hermes cores)
   * before shutting down the communication backend (e.g., MPI_Finalize).
   *
   * \param force_rpc_shutdown This should be \c true if Hermes was initialized
   * as a daemon.
   */
  void Finalize(bool force_rpc_shutdown = false);

  /** \brief Shutdown application cores.
   *
   * To be called from application cores that were started separately from a
   * Hermes daemon. Normally this is called from adapters.
   *
   * \param stop_daemon By default this function will stop the daemon this
   * client is connected to. Passing \c false here will keep it alive.
   */
  void FinalizeClient(bool stop_daemon = true);

  /** \todo Is this still necessary?
   *
   */
  void RemoteFinalize();

  /** \brief Starts a Hermes daemon.
   *
   * Starts all Hermes services, then waits on the main thread to be finalized.
   *
   * \pre The Hermes instance must be initialized with InitHermesDaemon.
   */
  void RunDaemon();

  /** \brief Check if a given Bucket contains a Blob.
   *
   * \param bucket_name The name of the Bucket to check.
   * \param blob_name The name of the Blob to check.
   *
   * \return \bool{the bucket \p bucket_name contains the Blob \p blob_name}
   */
  bool BucketContainsBlob(const std::string &bucket_name,
                          const std::string &blob_name);

  /** \brief Returns true if \p bucket_name exists in this Hermes instance.
   *
   * \param bucket_name The name of the Bucket to check.
   *
   * \return \bool{\p bucket_name exists in this Hermes instance}
   */
  bool BucketExists(const std::string &bucket_name);
};

class Bucket;

/** Renames a bucket referred to by name only */
Status RenameBucket(const std::string &old_name, const std::string &new_name,
                    Context &ctx);

/** \todo Not implemented yet. */
Status TransferBlob(const Bucket &src_bkt, const std::string &src_blob_name,
                    Bucket &dst_bkt, const std::string &dst_blob_name,
                    Context &ctx);

/** \brief Initialize an instance of Hermes.
 *
 * \param config_file The (relative or absolute) path to a hermes configuration
 * file
 * \param is_daemon \c true if initializing this Hermes instance as a daemon.
 * \param is_adapter \c true if initializing this Hermes instance as an adapter,
 * or client to an existing daemon.
 *
 * \pre Only one of \p is_daemon and \p is_adapter can be \c true.
 *
 * \return An initialized Hermes instance.
 */
std::shared_ptr<api::Hermes> InitHermes(const char *config_file = NULL,
                                        bool is_daemon = false,
                                        bool is_adapter = false);

}  // namespace api

/** \overload
 *
 * Allows programatically generating configurations.
 *
 * \param config a pointer to configuration
 * \param is_daemon a flag to run Hermes as a daemon
 * \param is_adapter a flag to run Hermes in adapter mode
 *
 * \return An initialized Hermes instance.
 */
std::shared_ptr<api::Hermes> InitHermes(Config *config, bool is_daemon = false,
                                        bool is_adapter = false);

/** \brief Initialize a Hermes instance as a daemon.
 *
 * A Hermes daemon is one or more processes (one per node) that handle all
 * Hermes background services. This includes RPC servers, thread pools, buffer
 * organization, and SystemViewState updates. A daemon is necessary in workflows
 * that involve 2 or more applications sharing buffered data. Without a daemon,
 * (i.e., co-deploying Hermes services with an application) the lifetime of
 * Hermes is tied to the app.
 *
 * \param config_file The (relative or absolute) path to a hermes configuration
 * file
 *
 * \return An initialized Hermes instance.
 */
std::shared_ptr<api::Hermes> InitHermesDaemon(char *config_file = NULL);

/** \overload
 *
 * \param config A valid Config.
 */
std::shared_ptr<api::Hermes> InitHermesDaemon(Config *config);

/** \brief  Initialize a Hermes instance as a client or adapter.
 *
 * \param config_file The (relative or absolute) path to a hermes configuration
 * file
 *
 * \pre An existing Hermes daemon must already be running.
 *
 * \return An initialized Hermes instance.
 */
std::shared_ptr<api::Hermes> InitHermesClient(const char *config_file = NULL);

}  // namespace hermes

#endif  // HERMES_H_
