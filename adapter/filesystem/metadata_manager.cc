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

#include "metadata_manager.h"

/**
 * Namespace declarations for cleaner code.
 */
using hermes::adapter::fs::AdapterStat;
using hermes::adapter::fs::MetadataManager;

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
