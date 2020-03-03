#include "glog/logging.h"

#include "hermes.h"
#include "bucket.h"
#include "trait.h"

namespace hermes {

namespace api {

Bucket Acquire(const std::string &name, Context &ctx) {
  Bucket ret;
    
  LOG(INFO) << "Acquiring Bucket " << name << '\n';
    
  return ret;
}

Status RenameBucket(const std::string &old_name,
                    const std::string &new_name,
                    Context &ctx) {
  Status ret = 0;
    
  LOG(INFO) << "Renaming Bucket from " << old_name << " to " << new_name << '\n';
    
  return ret;
}

Status TransferBlob(const Bucket &src_bkt,
                    const std::string &src_blob_name,
                    Bucket &dst_bkt,
                    const std::string &dst_blob_name,
                    Context &ctx) {
  Status ret = 0;
    
  LOG(INFO) << "Transferring Blob from " << src_blob_name << " to " << dst_blob_name << '\n';
    
  return ret;
}

} // api namepsace
} // hermes namespace
