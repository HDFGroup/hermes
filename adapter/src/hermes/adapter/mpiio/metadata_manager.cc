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
using hermes::adapter::mpiio::AdapterStat;
using hermes::adapter::mpiio::FileID;
using hermes::adapter::mpiio::MetadataManager;

bool MetadataManager::Create(FILE *fh, const AdapterStat &stat) {
  LOG(INFO) << "Create metadata for file handler." << std::endl;
  auto ret = metadata.emplace(Convert(fh), stat);
  return ret.second;
}

bool MetadataManager::Update(FILE *fh, const AdapterStat &stat) {
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

std::pair<AdapterStat, bool> MetadataManager::Find(FILE *fh) {
  auto fileId = Convert(fh);
  typedef std::pair<AdapterStat, bool> MetadataReturn;
  auto iter = metadata.find(fileId);
  if (iter == metadata.end())
    return MetadataReturn(AdapterStat(), false);
  else
    return MetadataReturn(iter->second, true);
}

FileID MetadataManager::Convert(FILE *fh) {
  struct stat st;
  int fd = fileno(fh);
  int status = fstat(fd, &st);
  if (status == 0) {
    return FileID(st.st_dev, st.st_ino);
  } else {
    // TODO(hari) @error_handling fstat failed invalid fh.
    return FileID();
  }
}

bool MetadataManager::Delete(FILE *fh) {
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
