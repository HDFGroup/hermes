//
// Created by manihariharan on 12/23/20.
//

#include "metadata_manager.h"

using hermes::adapter::stdio::AdapterStat;
using hermes::adapter::stdio::FileID;
using hermes::adapter::stdio::MetadataManager;

bool MetadataManager::IsTracked(FILE *fh) {
  auto iter = metadata.find(convert(fh));
  if (iter != metadata.end())
    return true;
  else
    return false;
}

bool MetadataManager::Create(FILE *fh, const AdapterStat &stat) {
  auto ret = metadata.emplace(convert(fh), stat);
  return ret.second;
}

bool MetadataManager::Update(FILE *fh, const AdapterStat &stat) {
  auto fileId = convert(fh);
  auto iter = metadata.find(fileId);
  if (iter != metadata.end()) {
    metadata.erase(iter);
    auto ret = metadata.emplace(fileId, stat);
    return true;
  } else {
    return false;
  }
}

std::pair<AdapterStat, bool> MetadataManager::Find(FILE *fh) {
  auto fileId = convert(fh);
  typedef std::pair<AdapterStat, bool> MetadataReturn;
  auto iter = metadata.find(fileId);
  if (iter == metadata.end())
    return MetadataReturn(AdapterStat(), false);
  else
    return MetadataReturn(iter->second, true);
}
FileID MetadataManager::convert(FILE *fh) {
  struct stat st;
  int fd = fileno(fh);
  fstat(fd, &st);
  return FileID(st.st_dev, st.st_ino);
}

bool MetadataManager::Delete(FILE *fh) {
  auto fileId = convert(fh);
  auto iter = metadata.find(fileId);
  if (iter != metadata.end()) {
    metadata.erase(iter);
    return true;
  } else
    return false;
}
