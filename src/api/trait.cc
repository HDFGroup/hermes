#include "trait.h"
#include "bucket.h"

namespace hermes {

namespace api {

Status Trait::Register(std::string& name, const std::string& key,
                const std::string& value, Context& ctx) {
  Status ret = 0;
  
  LOG(INFO) << "Register bucket " << name << '\n';;
  
  return ret;
}
  
Status Trait::EditTrait(const std::string& key,
                        const std::string& value, Context& ctx) {
  Status ret = 0;
    
  LOG(INFO) << "Edit bucket " << name_ << '\n';
    
  return ret;
}

Bucket Trait::Acquire(const std::string& name, Context& ctx) {
  Bucket ret;
    
  LOG(INFO) << "Acquire bucket " << name << '\n';
    
  return ret;
}

Status Trait::Link(const VBucket& vb, Context& ctx) {
  Status ret = 0;
    
  LOG(INFO) << "Link Trait "<< name_ << " to VBucket " << vb.GetName() << '\n';
    
  return ret;
}
  
Status Trait::Unlink(const VBucket& vb, Context& ctx) {
  Status ret = 0;
    
  LOG(INFO) << "Unlink Trait "<< name_ << " from VBucket " << vb.GetName() << '\n';
    
  return ret;
}
  
} // api namepsace
} // hermes namespace
