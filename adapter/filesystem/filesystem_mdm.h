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

#include <cstdio>
#include <unordered_map>
#include "file.h"
#include "filesystem_io_client.h"
#include "filesystem.h"
#include "thread_pool.h"

namespace hermes::adapter::fs {

/**
 * Metadata manager for POSIX adapter
 */
class MetadataManager {
 private:
  std::unordered_map<File, std::shared_ptr<AdapterStat>>
      metadata; /**< Map for metadata*/

 public:
  /** map for Hermes request */
  std::unordered_map<uint64_t, HermesRequest*> request_map;
  std::mutex lock_; /**< Lock for metadata updates */
  FsIoClientMetadata fs_mdm_; /**< Context needed for I/O clients */

  /** Constructor */
  MetadataManager() = default;

  /** Get the current adapter mode */
  AdapterMode GetBaseAdapterMode() {
    return HERMES->client_config_.GetBaseAdapterMode();
  }

  /** Get the adapter mode for a particular file */
  AdapterMode GetAdapterMode(const std::string &path) {
    return HERMES->client_config_.GetAdapterConfig(path).mode_;
  }

  /** Get the adapter page size for a particular file */
  size_t GetAdapterPageSize(const std::string &path) {
    return HERMES->client_config_.GetAdapterConfig(path).page_size_;
  }

  /**
   * Create a metadata entry for filesystem adapters given File handler.
   * @param f original file handler of the file on the destination
   * filesystem.
   * @param stat POSIX Adapter version of Stat data structure.
   * @return    true, if operation was successful.
   *            false, if operation was unsuccessful.
   */
  bool Create(const File& f, std::shared_ptr<AdapterStat> &stat);

  /**
   * Update existing metadata entry for filesystem adapters.
   * @param f original file handler of the file on the destination.
   * @param stat POSIX Adapter version of Stat data structure.
   * @return    true, if operation was successful.
   *            false, if operation was unsuccessful or entry doesn't exist.
   */
  bool Update(const File& f, const AdapterStat& stat);

  /**
   * Delete existing metadata entry for for filesystem adapters.
   * @param f original file handler of the file on the destination.
   * @return    true, if operation was successful.
   *            false, if operation was unsuccessful.
   */
  bool Delete(const File& f);

  /**
   * Find existing metadata entry for filesystem adapters.
   * @param f original file handler of the file on the destination.
   * @return    The metadata entry if exist.
   *            The bool in pair indicated whether metadata entry exists.
   */
  std::shared_ptr<AdapterStat> Find(const File& f);
};
}  // namespace hermes::adapter::fs

// Singleton macros
#include "hermes_shm/util/singleton.h"

#define HERMES_FS_METADATA_MANAGER \
  hermes_shm::Singleton<hermes::adapter::fs::MetadataManager>::GetInstance()
#define HERMES_FS_METADATA_MANAGER_T hermes::adapter::fs::MetadataManager*

#define HERMES_FS_THREAD_POOL \
  hermes_shm::EasySingleton<hermes::ThreadPool>::GetInstance()

#endif  // HERMES_ADAPTER_METADATA_MANAGER_H
