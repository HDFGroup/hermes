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
#include "hermes.h"
#include <filesystem>

namespace hermes::adapter::fs {

bool MetadataManager::Create(const File &f,
                             std::shared_ptr<AdapterStat> &stat) {
  VLOG(1) << "Create metadata for file handler." << std::endl;
  path_to_hermes_file_.emplace(stat->path_, f);
  auto ret = hermes_file_to_stat_.emplace(f, std::move(stat));
  return ret.second;
}

bool MetadataManager::Update(const File &f, const AdapterStat &stat) {
  VLOG(1) << "Update metadata for file handler." << std::endl;
  auto iter = hermes_file_to_stat_.find(f);
  if (iter != hermes_file_to_stat_.end()) {
    *(*iter).second = stat;
    return true;
  } else {
    return false;
  }
}

File MetadataManager::Find(const std::string &path) {
  std::string canon_path = stdfs::weakly_canonical(path).string();
  auto iter = path_to_hermes_file_.find(canon_path);
  if (iter == path_to_hermes_file_.end())
    return File();
  else
    return iter->second;
}

std::shared_ptr<AdapterStat> MetadataManager::Find(const File &f) {
  auto iter = hermes_file_to_stat_.find(f);
  if (iter == hermes_file_to_stat_.end())
    return nullptr;
  else
    return iter->second;
}

bool MetadataManager::Delete(const std::string &path, const File &f) {
  VLOG(1) << "Delete metadata for file handler." << std::endl;
  auto iter = hermes_file_to_stat_.find(f);
  if (iter != hermes_file_to_stat_.end()) {
    hermes_file_to_stat_.erase(iter);
    return true;
  } else {
    return false;
  }
  path_to_hermes_file_.erase(path);
}

}  // namespace hermes::adapter::fs
