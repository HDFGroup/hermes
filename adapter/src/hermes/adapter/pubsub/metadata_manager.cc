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
using hermes::adapter::pubsub::MetadataManager;
using hermes::adapter::pubsub::MetadataManager;

bool MetadataManager::Create(const std::string& topic, const TopicMetadata&stat) {
  LOG(INFO) << "Create metadata for topic: " << topic << std::endl;
  auto ret = metadata.emplace(topic, stat);
  return ret.second;
}

bool MetadataManager::Update(const std::string& topic, const TopicMetadata&stat) {
  LOG(INFO) << "Update metadata for topic: " << topic << std::endl;
  auto iter = metadata.find(topic);
  if (iter != metadata.end()) {
    metadata.erase(iter);
    auto ret = metadata.emplace(topic, stat);
    return ret.second;
  } else {
    return false;
  }
}

std::pair<TopicMetadata, bool> MetadataManager::Find(const std::string& topic) {
  typedef std::pair<TopicMetadata, bool> MetadataReturn;
  auto iter = metadata.find(topic);
  if (iter == metadata.end())
    return MetadataReturn(TopicMetadata(), false);
  else
    return MetadataReturn(iter->second, true);
}

bool MetadataManager::Delete(const std::string& topic) {
  LOG(INFO) << "Delete metadata for topic: " << topic << std::endl;
  auto iter = metadata.find(topic);
  if (iter != metadata.end()) {
    metadata.erase(iter);
    return true;
  } else {
    return false;
  }
}
