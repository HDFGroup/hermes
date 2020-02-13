#include "glog/logging.h"

#include "bucket.h"

namespace hermes {

namespace api {

Bucket Acquire(const std::string& name, Context& ctx) {
  Bucket ret;
    
  LOG(INFO) << "Acquire Bucket " << name << std::endl;
    
  return ret;
}

Status RenameBucket(const std::string& old_name,
                    const std::string& new_name,
                    Context& ctx) {
  Status ret = 0;
    
  LOG(INFO) << "Rename Bucket from " << old_name << " to " << new_name << std::endl;
    
  return ret;
}

Status TransferBlob(const Bucket& src_bkt,
                    const std::string& src_blob_name,
                    Bucket& dst_bkt,
                    const std::string& dst_blob_name,
                    Context& ctx) {
  Status ret = 0;
    
  LOG(INFO) << "Transfer Blob from " << src_blob_name << " to " << dst_blob_name << std::endl;
    
  return ret;
}

} // api namepsace
} // hermes namespace
