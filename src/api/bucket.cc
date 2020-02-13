#include "bucket.h"

#include <iostream>

namespace hermes {

namespace api {

Status Bucket::Rename(const std::string& new_name, Context& ctx) {
  Status ret = 0;
    
  LOG(INFO) << "Rename a bucket to" << new_name << std::endl;
    
  return ret;
}


Status Bucket::Release(Context& ctx) {
  Status ret = 0;
    
  LOG(INFO) << "Release bucket " << std::endl;
    
  return ret;
}

Status Bucket::Put(const std::string& name, const Blob& data, Context& ctx) {
  Status ret = 0;
    
  LOG(INFO) << "Attach blol " << name << "to Bucket " << std::endl;
    
  return ret;
}

const Blob& Bucket::Get(const std::string& name, Context& ctx) {
  Blob& ret = blobs_[0];
    
  LOG(INFO) << "Get Blob " << name << std::endl;
    
  return ret;
}

Status Bucket::DeleteBlob(const std::string& name, Context& ctx) {
  Status ret = 0;
    
  LOG(INFO) << "Delete Blob " << name << std::endl;
    
  return ret;
}

Status Bucket::RenameBlob(const std::string& old_name,
                          const std::string& new_name,
                          Context& ctx) {
  Status ret = 0;
    
  LOG(INFO) << "Rename Blob " << old_name << " to " << new_name << std::endl;
    
  return ret;
}
    
} // api namespace
} // hermes namespace
