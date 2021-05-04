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
using hermes::adapter::mpiio::HermesStruct;
using hermes::adapter::mpiio::MetadataManager;

bool MetadataManager::Create(MPI_File *fh, const AdapterStat &stat) {
  LOG(INFO) << "Create metadata for file handler." << std::endl;
  auto ret = metadata.emplace(*fh, stat);
  return ret.second;
}

bool MetadataManager::Update(MPI_File *fh, const AdapterStat &stat) {
  LOG(INFO) << "Update metadata for file handler." << std::endl;
  auto iter = metadata.find(*fh);
  if (iter != metadata.end()) {
    metadata.erase(iter);
    auto ret = metadata.emplace(*fh, stat);
    return ret.second;
  } else {
    return false;
  }
}

std::pair<AdapterStat, bool> MetadataManager::Find(MPI_File *fh) {
  typedef std::pair<AdapterStat, bool> MetadataReturn;
  auto iter = metadata.find(*fh);
  if (iter == metadata.end())
    return MetadataReturn(AdapterStat(), false);
  else
    return MetadataReturn(iter->second, true);
}

bool MetadataManager::Delete(MPI_File *fh) {
  LOG(INFO) << "Delete metadata for file handler." << std::endl;
  auto iter = metadata.find(*fh);
  if (iter != metadata.end()) {
    metadata.erase(iter);
    return true;
  } else {
    return false;
  }
}

std::string MetadataManager::EncodeBlobNameLocal(HermesStruct hermes_struct) {
  LOG(INFO) << "Encode Blob:" << hermes_struct.blob_name_
            << " for hermes blobs." << std::endl;
  return hermes_struct.blob_name_ + kStringDelimiter +
         std::to_string(hermes_struct.offset_) + kStringDelimiter +
         std::to_string(hermes_struct.size_) + kStringDelimiter +
         std::to_string(rank);
}

std::pair<int, HermesStruct> MetadataManager::DecodeBlobNameLocal(
    std::string &encoded_blob_name) {
  HermesStruct hermes_struct;
  auto str_split =
      hermes::adapter::StringSplit(encoded_blob_name.data(), kStringDelimiter);
  hermes_struct.blob_name_ = encoded_blob_name;
  hermes_struct.offset_ = std::stoi(str_split[1]);
  hermes_struct.size_ = std::stoi(str_split[2]);
  int rank = std::stoi(str_split[3]);
  return std::pair<int, HermesStruct>(rank, hermes_struct);
}
