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
using hermes::adapter::posix::AdapterStat;
using hermes::adapter::posix::FileID;
using hermes::adapter::posix::MetadataManager;

bool MetadataManager::Create(int fh, const AdapterStat &stat) {
  LOG(INFO) << "Create metadata for file handler." << std::endl;
  auto ret = metadata.emplace(Convert(fh), stat);
  return ret.second;
}

bool MetadataManager::Update(int fh, const AdapterStat &stat) {
  LOG(INFO) << "Update metadata for file handler." << std::endl;
  auto fileId = Convert(fh);
  auto iter = metadata.find(fileId);
  if (iter != metadata.end()) {
    metadata.erase(iter);
    auto ret = metadata.emplace(fileId, stat);
    return ret.second;
  } else {
    return false;
  }
}

std::pair<AdapterStat, bool> MetadataManager::Find(int fh) {
  auto fileId = Convert(fh);
  typedef std::pair<AdapterStat, bool> MetadataReturn;
  auto iter = metadata.find(fileId);
  if (iter == metadata.end())
    return MetadataReturn(AdapterStat(), false);
  else
    return MetadataReturn(iter->second, true);
}

FileID MetadataManager::Convert(int fd) {
  struct stat st;
  MAP_OR_FAIL(__fxstat);
  int status = real___fxstat_(_STAT_VER, fd, &st);
  if (status == 0) {
    return FileID(st.st_dev, st.st_ino);
  } else {
    // TODO(hari) @error_handling fstat failed invalid fh.
    return FileID();
  }
}

bool MetadataManager::Delete(int fh) {
  LOG(INFO) << "Delete metadata for file handler." << std::endl;
  auto fileId = Convert(fh);
  auto iter = metadata.find(fileId);
  if (iter != metadata.end()) {
    metadata.erase(iter);
    return true;
  } else {
    return false;
  }
}
