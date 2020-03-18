#include "bucket.h"

#include <iostream>

namespace hermes {

namespace api {

Status Bucket::Contain_blob(const std::string& blob_name) {
	Status ret = 0;
	
	LOG(INFO) << "Checking if blob " << blob_name << " exists in Bucket "
	          << name_ << '\n';
	
	if (blobs_.find(blob_name) != blobs_.end())
		ret = 1;
			
  return ret;
}

Status Bucket::Rename(const std::string &new_name, Context &ctx) {
  (void)ctx;
  Status ret = 0;
    
  LOG(INFO) << "Renaming a bucket to" << new_name << '\n';
    
  return ret;
}


Status Bucket::Release(Context &ctx) {
  Status ret = 0;
    
  LOG(INFO) << "Releasing bucket " << '\n';
    
  return ret;
}

Status Bucket::Put(const std::string &name, const Blob &data, Context &ctx) {
  (void)ctx;
  Status ret = 0;
	// get blob buffer ID
	uint64_t blob_id = 0;
	
	// Inserting blob[name, id] pair
	blobs_[name] = blob_id;
    
  LOG(INFO) << "Attaching blob " << name << "to Bucket " << '\n';
    
  return ret;
}

size_t Bucket::Get(const std::string &name, Blob& user_blob, Context &ctx) {
  (void)ctx;
    
  LOG(INFO) << "Getting Blob " << name << " from bucket " << name_ << '\n';
    
}

Status Bucket::DeleteBlob(const std::string &name, Context &ctx) {
  (void)ctx;
  Status ret = 0;
    
  LOG(INFO) << "Deleting Blob " << name << " from bucket " << name_ << '\n';
    
  return ret;
}

Status Bucket::RenameBlob(const std::string &old_name,
                          const std::string &new_name,
                          Context &ctx) {
  (void)ctx;
  Status ret = 0;
    
  LOG(INFO) << "Renaming Blob " << old_name << " to " << new_name << '\n';
    
  return ret;
}
    
} // api namespace
} // hermes namespace
