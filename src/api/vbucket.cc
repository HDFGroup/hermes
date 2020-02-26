#include "vbucket.h"
#include "trait.h"

namespace hermes {
  
namespace api {
  
  Status VBucket::Attach(const Trait& trt, Context& ctx) {
  Status ret = 0;
  
  LOG(INFO) << "Attach trait " << trt.GetName() << " to VBucket " << name_ << '\n';
  
  return ret;
}
  
  Status VBucket::Detach(const Trait& trt, Context& ctx) {
  Status ret = 0;
  
  LOG(INFO) << "Unlink trait " << trt.GetName() << "from VBucket " << name_ << '\n';
  
  return ret;
}

} // api namepsace
} // hermes namespace
