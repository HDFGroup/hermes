#include "trait.h"
#include "bucket.h"
#include "vbucket.h"

namespace hermes {

namespace api {

Status TraitSchema::Register(const std::string &name,
                             std::unordered_map<std::string, std::string>,
                             Context &ctx) {
  Status ret = 0;
  
  LOG(INFO) << "Registering Trait " << name << '\n';
  
  return ret;
}

Trait& Trait::Acquire(const std::string &trtschema_name,
                      std::unordered_map<std::string, std::string>,
                      Context &ctx) {
  LOG(INFO) << "Acquiring trait schema " << trtschema_name
            << " and populate to Trait " << name_ << '\n';
  
  return *this;
}
  
Status Trait::EditTrait(const std::string& key,
                        const std::string& value, Context& ctx) {
  Status ret = 0;
    
  LOG(INFO) << "Editing Trait " << name_ << '\n';
    
  return ret;
}

} // api namepsace
} // hermes namespace
