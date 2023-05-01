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
#include "filesystem_io_client.h"
#include "filesystem.h"
#include "thread_pool.h"

namespace hermes::adapter::fs {

/**
 * Metadata manager for POSIX adapter
 */
class MetadataManager {
 private:
  std::unordered_map<std::string, std::list<File>>
      path_to_hermes_file_; /**< Map to determine if path is buffered. */
  std::unordered_map<File, std::shared_ptr<AdapterStat>>
      hermes_file_to_stat_; /**< Map for metadata */
  RwLock lock_;             /**< Lock to synchronize MD updates*/

 public:
  /** map for Hermes request */
  std::unordered_map<uint64_t, HermesRequest*> request_map;
  FsIoClientMetadata fs_mdm_; /**< Context needed for I/O clients */

  /** Constructor */
  MetadataManager() = default;

  /** Get the current adapter mode */
  AdapterMode GetBaseAdapterMode() {
    ScopedRwReadLock md_lock(lock_, kFS_GetBaseAdapterMode);
    return HERMES->client_config_.GetBaseAdapterMode();
  }

  /** Get the adapter mode for a particular file */
  AdapterMode GetAdapterMode(const std::string &path) {
    ScopedRwReadLock md_lock(lock_, kFS_GetAdapterMode);
    return HERMES->client_config_.GetAdapterConfig(path).mode_;
  }

  /** Get the adapter page size for a particular file */
  size_t GetAdapterPageSize(const std::string &path) {
    ScopedRwReadLock md_lock(lock_, kFS_GetAdapterPageSize);
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
  bool Delete(const std::string &path, const File& f);

  /**
   * Find the hermes file relating to a path.
   * @param path the path being checked
   * @return The hermes file.
   * */
  std::list<File>* Find(const std::string &path);

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
  hshm::Singleton<hermes::adapter::fs::MetadataManager>::GetInstance()
#define HERMES_FS_METADATA_MANAGER_T hermes::adapter::fs::MetadataManager*

#define HERMES_FS_THREAD_POOL \
  hshm::EasySingleton<hermes::ThreadPool>::GetInstance()

#endif  // HERMES_ADAPTER_METADATA_MANAGER_H
