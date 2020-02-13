#include "trait.h"
#include "bucket.h"

namespace hermes {

namespace api {

Status Trait::EditTrait(const std::string& key, const std::string& value, Context& ctx) {
  Status ret = 0;
    
  LOG(INFO) << "Edit bucket " << std::endl;
    
  return ret;
}

Bucket Trait::Acquire(const std::string& name, Context& ctx) {
  Bucket ret;
    
  LOG(INFO) << "Acquire bucket " << name << std::endl;
    
  return ret;
}

Status Trait::Link(const Bucket& bkt, Context& ctx) {
  Status ret = 0;
    
  LOG(INFO) << "Link " << bkt.GetName() << " to trait " << name_ << std::endl;
    
  return ret;
}

Status Trait::Unlink(const Bucket& bkt, Context& ctx) {
  Status ret = 0;
    
  LOG(INFO) << "Unlink trait " << name_ << std::endl;
    
  return ret;
}

} // api namepsace
} // hermes namespace
