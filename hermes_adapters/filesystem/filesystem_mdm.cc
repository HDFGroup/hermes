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

#include "filesystem_mdm.h"
#include "hermes/hermes.h"
#include <filesystem>

namespace hermes::adapter::fs {

bool MetadataManager::Create(const File &f,
                             std::shared_ptr<AdapterStat> &stat) {
  HILOG(kDebug, "Create metadata for file handler")
  ScopedRwWriteLock md_lock(lock_, kMDM_Create);
  if (path_to_hermes_file_.find(stat->path_) == path_to_hermes_file_.end()) {
    path_to_hermes_file_.emplace(stat->path_, std::list<File>());
  }
  path_to_hermes_file_[stat->path_].emplace_back(f);
  auto ret = hermes_file_to_stat_.emplace(f, std::move(stat));
  return ret.second;
}

bool MetadataManager::Update(const File &f, const AdapterStat &stat) {
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

std::list<File>* MetadataManager::Find(const std::string &path) {
  std::string canon_path = stdfs::absolute(path).string();
  ScopedRwReadLock md_lock(lock_, kMDM_Find);
  auto iter = path_to_hermes_file_.find(canon_path);
  if (iter == path_to_hermes_file_.end())
    return nullptr;
  else
    return &iter->second;
}

std::shared_ptr<AdapterStat> MetadataManager::Find(const File &f) {
  ScopedRwReadLock md_lock(lock_, kMDM_Find2);
  auto iter = hermes_file_to_stat_.find(f);
  if (iter == hermes_file_to_stat_.end())
    return nullptr;
  else
    return iter->second;
}

bool MetadataManager::Delete(const std::string &path, const File &f) {
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

}  // namespace hermes::adapter::fs
