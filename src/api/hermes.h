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
#include <set>
#include <iostream>
#include <vector>

#include <glog/logging.h>

#include "hermes_types.h"
#include "buffer_pool.h"
#include "metadata_management.h"
#include "rpc.h"
#include "id.h"

namespace hermes {
namespace api {

/** Return the (semantic versioning compatible) version of Hermes in the form
 *  MAJOR.MINOR.PATCH
 */
std::string GetVersion();

/** Hermes node state */
class Hermes {
 public:
  std::set<std::string> bucket_list_;
  std::set<std::string> vbucket_list_;

  // TODO(chogan): Temporarily public to facilitate iterative development.
  hermes::SharedMemoryContext context_;
  hermes::CommunicationContext comm_;
  hermes::RpcContext rpc_;
  hermes::Arena trans_arena_;
  std::string shmem_name_;
  std::string rpc_server_name_;
  bool is_initialized;

  /** if true will do more checks, warnings, expect slower code */
  const bool debug_mode_ = true;

  Hermes() {}

  explicit Hermes(SharedMemoryContext context) : context_(context) {}

  /** Display the list of buckets in this node */
  void Display_bucket() {
    for (auto it = bucket_list_.begin(); it != bucket_list_.end(); ++it)
      std::cout << *it << '\t';
    std::cout << '\n';
  }

  /** Display the list of vbuckets in this node */
  void Display_vbucket() {
    for (auto it = vbucket_list_.begin(); it != vbucket_list_.end(); ++it)
      std::cout << *it << '\t';
    std::cout << '\n';
  }

  /** Returns whether we are running on an application core. */
  bool IsApplicationCore();
  /** Returns whether we are the first MPI rank on a given node */
  bool IsFirstRankOnNode();
  /** A barrier across all application processes. */
  void AppBarrier();
  /** Returns the rank of this process */
  int GetProcessRank();
  /** Return the Node ID of this process */
  int GetNodeId();
  /** Returns the total number of application processes */
  int GetNumProcesses();
  /** Get an application communicator handle */
  void *GetAppCommunicator();
  /** \todo Hermes::Finalize */
  void Finalize(bool force_rpc_shutdown = false);
  /** \todo Hermes::FinalizeClient */
  void FinalizeClient(bool stop_daemon = true);
  /** \todo Hermes::RemoteFinalize */
  void RemoteFinalize();
  /** \todo Hermes::RunDaemon */
  void RunDaemon();

  /** Check if a given bucket contains a blob. */
  bool BucketContainsBlob(const std::string &bucket_name,
                          const std::string &blob_name);
  /** Returns true if @p bucket_name exists in this Hermes instance. */
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
