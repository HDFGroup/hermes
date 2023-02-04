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

/**
 * Namespace declarations for cleaner code.
 */
using hermes::adapter::fs::AdapterStat;
using hermes::adapter::fs::MetadataManager;

bool MetadataManager::Create(const File &f,
                             std::shared_ptr<AdapterStat> &stat) {
  VLOG(1) << "Create metadata for file handler." << std::endl;
  auto ret = metadata.emplace(f, std::move(stat));
  return ret.second;
}

bool MetadataManager::Update(const File &f, const AdapterStat &stat) {
  VLOG(1) << "Update metadata for file handler." << std::endl;
  auto iter = metadata.find(f);
  if (iter != metadata.end()) {
    *(*iter).second = stat;
    return true;
  } else {
    return false;
  }
}

std::shared_ptr<AdapterStat> MetadataManager::Find(const File &f) {
  auto iter = metadata.find(f);
  if (iter == metadata.end())
    return nullptr;
  else
    return iter->second;
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
