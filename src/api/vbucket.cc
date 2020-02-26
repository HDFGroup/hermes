#include "vbucket.h"
#include "trait.h"

namespace hermes {
  
namespace api {
  
Status VBucket::Attach(const Trait& trt, Context& ctx) {
  Status ret = 0;
  
  LOG(INFO) << "Attaching trait " << trt.GetName()
            << " to VBucket " << name_ << '\n';
  
  return ret;
}
  
Status VBucket::Detach(const Trait& trt, Context& ctx) {
  Status ret = 0;
  
  LOG(INFO) << "Detaching trait " << trt.GetName()
            << " from VBucket " << name_ << '\n';
  
  return ret;
}

Status VBucket::Link(std::string blob_name, std::string bucket_name, Context& ctx) {
  Status ret = 0;
    
  LOG(INFO) << "Linking blob "<< blob_name << " in bucket "
            << bucket_name << " to VBucket " << name_ << '\n';
    
  return ret;
}
  
Status VBucket::Unlink(std::string blob_name, std::string bucket_name, Context& ctx) {
  Status ret = 0;
    
  LOG(INFO) << "Unlinking blob "<< blob_name << " in bucket "
            << bucket_name << " from VBucket " << name_ << '\n';
    
  return ret;
}
  
} // api namepsace
} // hermes namespace
