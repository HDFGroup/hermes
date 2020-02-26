#include "bucket.h"

#include <iostream>

namespace hermes {

namespace api {

Status Bucket::Rename(const std::string& new_name, Context& ctx) {
  Status ret = 0;
    
  LOG(INFO) << "Rename a bucket to" << new_name << '\n';
    
  return ret;
}


Status Bucket::Release(Context& ctx) {
  Status ret = 0;
    
  LOG(INFO) << "Release bucket " << '\n';
    
  return ret;
}

Status Bucket::Put(const std::string& name, const Blob& data, Context& ctx) {
  Status ret = 0;
    
  LOG(INFO) << "Attach blob " << name << "to Bucket " << '\n';
    
  return ret;
}

const Blob& Bucket::Get(const std::string& name, Context& ctx) {
  Blob& ret = blobs_[0];
    
  LOG(INFO) << "Get Blob " << name << " from bucket " << name_ << '\n';
    
  return ret;
}

Status Bucket::DeleteBlob(const std::string& name, Context& ctx) {
  Status ret = 0;
    
  LOG(INFO) << "Delete Blob " << name << " from bucket " << name_ << '\n';
    
  return ret;
}

Status Bucket::RenameBlob(const std::string& old_name,
                          const std::string& new_name,
                          Context& ctx) {
  Status ret = 0;
    
  LOG(INFO) << "Rename Blob " << old_name << " to " << new_name << '\n';
    
  return ret;
}
    
} // api namespace
} // hermes namespace
