#include "metadata_manager.h"

/**
 * Namespace declarations for cleaner code.
 */
using hermes::adapter::stdio::AdapterStat;
using hermes::adapter::stdio::FileID;
using hermes::adapter::stdio::MetadataManager;

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
  fstat(fd, &st);
  return FileID(st.st_dev, st.st_ino);
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
