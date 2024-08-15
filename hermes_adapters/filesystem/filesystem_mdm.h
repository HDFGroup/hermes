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

namespace hermes::adapter {

/**
 * Metadata manager for POSIX adapter
 */
class MetadataManager {
 private:
  std::unordered_map<std::string, std::list<File>>
      path_to_hermes_file_;  /**< Map to determine if path is buffered. */
  std::unordered_map<File, std::shared_ptr<AdapterStat>>
      hermes_file_to_stat_;  /**< Map for metadata */
  RwLock lock_;              /**< Lock to synchronize MD updates*/

 public:
  std::unordered_map<uint64_t, FsAsyncTask*>
      request_map_;  /**< Map for async FS requests */
  FsIoClientMetadata fs_mdm_;  /**< Context needed for I/O clients */

  /** Constructor */
  MetadataManager() = default;

  /** Get the current adapter mode */
  AdapterMode GetBaseAdapterMode() {
    ScopedRwReadLock md_lock(lock_, 1);
    return HERMES_CLIENT_CONF.GetBaseAdapterMode();
  }

  /** Get the adapter mode for a particular file */
  AdapterMode GetAdapterMode(const std::string &path) {
    ScopedRwReadLock md_lock(lock_, 2);
    return HERMES_CLIENT_CONF.GetAdapterConfig(path).mode_;
  }

  /** Get the adapter page size for a particular file */
  size_t GetAdapterPageSize(const std::string &path) {
    ScopedRwReadLock md_lock(lock_, 3);
    return HERMES_CLIENT_CONF.GetAdapterConfig(path).page_size_;
  }

  /**
   * Create a metadata entry for filesystem adapters given File handler.
   * @param f original file handler of the file on the destination
   * filesystem.
   * @param stat POSIX Adapter version of Stat data structure.
   * @return    true, if operation was successful.
   *            false, if operation was unsuccessful.
   */
  bool Create(const File& f, std::shared_ptr<AdapterStat> &stat) {
    HILOG(kDebug, "Create metadata for file handler")
    ScopedRwWriteLock md_lock(lock_, kMDM_Create);
    if (path_to_hermes_file_.find(stat->path_) == path_to_hermes_file_.end()) {
      path_to_hermes_file_.emplace(stat->path_, std::list<File>());
    }
    path_to_hermes_file_[stat->path_].emplace_back(f);
    auto ret = hermes_file_to_stat_.emplace(f, std::move(stat));
    return ret.second;
  }

  /**
   * Update existing metadata entry for filesystem adapters.
   * @param f original file handler of the file on the destination.
   * @param stat POSIX Adapter version of Stat data structure.
   * @return    true, if operation was successful.
   *            false, if operation was unsuccessful or entry doesn't exist.
   */
  bool Update(const File& f, const AdapterStat& stat) {
    HILOG(kDebug, "Update metadata for file handler")
    ScopedRwWriteLock md_lock(lock_, kMDM_Update);
    auto iter = hermes_file_to_stat_.find(f);
    if (iter != hermes_file_to_stat_.end()) {
      *(*iter).second = stat;
      return true;
    } else {
      return false;
    }
  }

  /**
   * Delete existing metadata entry for for filesystem adapters.
   * @param f original file handler of the file on the destination.
   * @return    true, if operation was successful.
   *            false, if operation was unsuccessful.
   */
  bool Delete(const std::string &path, const File& f) {
    HILOG(kDebug, "Delete metadata for file handler")
    ScopedRwWriteLock md_lock(lock_, kMDM_Delete);
    auto iter = hermes_file_to_stat_.find(f);
    if (iter != hermes_file_to_stat_.end()) {
      hermes_file_to_stat_.erase(iter);
      auto &list = path_to_hermes_file_[path];
      auto f_iter = std::find(list.begin(), list.end(), f);
      path_to_hermes_file_[path].erase(f_iter);
      if (list.size() == 0) {
        path_to_hermes_file_.erase(path);
      }
      return true;
    } else {
      return false;
    }
  }

  /**
   * Find the hermes file relating to a path.
   * @param path the path being checked
   * @return The hermes file.
   * */
  std::list<File>* Find(const std::string &path) {
    try {
      std::string canon_path = stdfs::absolute(path).string();
      ScopedRwReadLock md_lock(lock_, kMDM_Find);
      auto iter = path_to_hermes_file_.find(canon_path);
      if (iter == path_to_hermes_file_.end())
        return nullptr;
      else
        return &iter->second;
    } catch(const std::exception &e) {
      HELOG(kError, "Error finding path: {}", e.what())
      return nullptr;
    }
  }

  /**
   * Find existing metadata entry for filesystem adapters.
   * @param f original file handler of the file on the destination.
   * @return    The metadata entry if exist.
   *            The bool in pair indicated whether metadata entry exists.
   */
  std::shared_ptr<AdapterStat> Find(const File& f) {
    ScopedRwReadLock md_lock(lock_, kMDM_Find2);
    auto iter = hermes_file_to_stat_.find(f);
    if (iter == hermes_file_to_stat_.end())
      return nullptr;
    else
      return iter->second;
  }

  /**
   * Add a request to the request map.
   * */
  void EmplaceTask(uint64_t id, FsAsyncTask* task) {
    ScopedRwWriteLock md_lock(lock_, 0);
    request_map_.emplace(id, task);
  }

  /**
   * Find a request in the request map.
   * */
  FsAsyncTask* FindTask(uint64_t id) {
    ScopedRwReadLock md_lock(lock_, 0);
    auto iter = request_map_.find(id);
    if (iter == request_map_.end()) {
      return nullptr;
    } else {
      return iter->second;
    }
  }

  /**
   * Delete a request in the request map.
   * */
  void DeleteTask(uint64_t id) {
    ScopedRwWriteLock md_lock(lock_, 0);
    auto iter = request_map_.find(id);
    if (iter != request_map_.end()) {
      request_map_.erase(iter);
    }
  }
};
}  // namespace hermes::adapter

// Singleton macros
#include "hermes_shm/util/singleton.h"

#define HERMES_FS_METADATA_MANAGER \
  hshm::Singleton<::hermes::adapter::MetadataManager>::GetInstance()
#define HERMES_FS_METADATA_MANAGER_T hermes::adapter::MetadataManager*


#endif  // HERMES_ADAPTER_METADATA_MANAGER_H
