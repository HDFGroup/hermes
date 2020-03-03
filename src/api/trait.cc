#include "trait.h"
#include "bucket.h"
#include "vbucket.h"

namespace hermes {

namespace api {
  
Status Trait::EditTrait(const std::string& key,
                        const std::string& value, Context& ctx) {
  Status ret = 0;
    
  LOG(INFO) << "Editing Trait " << name_ << '\n';
    
  return ret;
}

} // api namepsace
} // hermes namespace
