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

#include <cstdint>
#include <string>

#include <glog/logging.h>

#include "hermes_types.h"
#include "buffer_pool.h"
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
  hermes::SharedMemoryContext context_;
  hermes::CommunicationContext comm_;
  hermes::RpcContext rpc_;
  hermes::Arena trans_arena_;
  /** The name of the shared memory segment in which all Hermes data is
   * stored.
   */
  std::string shmem_name_;
  /** The name of the primary RPC server. */
  std::string rpc_server_name_;

  Hermes() {}

  explicit Hermes(SharedMemoryContext context) : context_(context) {}

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
   * 
   *
   * \param stop_daemon By default this function will stop the daemon this
   * client is connected to. Passing \c false here will keep it alive.
   */
  void FinalizeClient(bool stop_daemon = true);
  /** \todo Hermes::RemoteFinalize */
  void RemoteFinalize();
  /** \todo Hermes::RunDaemon */
  void RunDaemon();

  /** Check if a given bucket contains a blob. */
  bool BucketContainsBlob(const std::string &bucket_name,
                          const std::string &blob_name);
  /** Returns true if \p bucket_name exists in this Hermes instance. */
  bool BucketExists(const std::string &bucket_name);
};

class VBucket;

class Bucket;

/** Renames a bucket referred to by name only */
Status RenameBucket(const std::string &old_name,
                    const std::string &new_name,
                    Context &ctx);

/** Transfers a blob between buckets */
Status TransferBlob(const Bucket &src_bkt,
                    const std::string &src_blob_name,
                    Bucket &dst_bkt,
                    const std::string &dst_blob_name,
                    Context &ctx);

/** \todo InitHermes */
std::shared_ptr<api::Hermes> InitHermes(const char *config_file = NULL,
                                        bool is_daemon = false,
                                        bool is_adapter = false);

}  // namespace api

/** \todo InitHermes */
std::shared_ptr<api::Hermes> InitHermes(Config *config, bool is_daemon = false,
                                        bool is_adapter = false);
/** \todo InitHermesDaemon */
std::shared_ptr<api::Hermes> InitHermesDaemon(char *config_file = NULL);
/** \todo InitHermesDaemon */
std::shared_ptr<api::Hermes> InitHermesDaemon(Config *config);
/** \todo InitHermesClient */
std::shared_ptr<api::Hermes> InitHermesClient(const char *config_file = NULL);

}  // namespace hermes

#endif  // HERMES_H_
