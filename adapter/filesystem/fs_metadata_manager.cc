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

#include "fs_metadata_manager.h"
#include "hermes.h"

/**
 * Namespace declarations for cleaner code.
 */
using hermes::adapter::fs::AdapterStat;
using hermes::adapter::fs::MetadataManager;

void MetadataManager::InitializeHermes() {
  if (!is_init_) {
    lock_.lock();
    if (!is_init_) {
      HERMES->Init(HermesType::kClient);
      is_init_ = true;
    }
    lock_.unlock();
  }

  // TODO(llogan): Recycle old fds
  hermes_fd_min_ = 8192;  // TODO(llogan): don't assume 8192
  hermes_fd_max_ = INT_MAX;
}

bool MetadataManager::Create(const File &f, const AdapterStat &stat) {
  VLOG(1) << "Create metadata for file handler." << std::endl;
  auto ret = metadata.emplace(f, stat);
  return ret.second;
}

bool MetadataManager::Update(const File &f, const AdapterStat &stat) {
  VLOG(1) << "Update metadata for file handler." << std::endl;
  auto iter = metadata.find(f);
  if (iter != metadata.end()) {
    metadata.erase(iter);
    auto ret = metadata.emplace(f, stat);
    return ret.second;
  } else {
    return false;
  }
}

std::pair<AdapterStat, bool> MetadataManager::Find(const File &f) {
  typedef std::pair<AdapterStat, bool> MetadataReturn;
  auto iter = metadata.find(f);
  if (iter == metadata.end())
    return MetadataReturn(AdapterStat(), false);
  else
    return MetadataReturn(iter->second, true);
}

bool MetadataManager::Delete(const File &f) {
  VLOG(1) << "Delete metadata for file handler." << std::endl;
  auto iter = metadata.find(f);
  if (iter != metadata.end()) {
    metadata.erase(iter);
    return true;
  } else {
    return false;
  }
}
