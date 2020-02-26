#ifndef TRAIT_H_
#define TRAIT_H_

#include <memory>
#include <string>
#include <unordered_map>

#include "glog/logging.h"

#include "hermes.h"

namespace hermes {

namespace api {

class TraitSchema {
 private:
  std::string name_;
  std::unordered_map<std::string, std::string> attributes_;
  
 public:
  /** internal HERMES object owned by trait */
  std::shared_ptr<HERMES> m_HERMES_;
  
  TraitSchema() : name_("default_trtschema") {
    //TODO: initialize kDefault
    LOG(INFO) << "Create default TraitSchema" << std::endl;
  };
  
  TraitSchema(std::string initial_name) : name_(initial_name) {
    //TODO: initialize kDefault
    LOG(INFO) << "Create TraitSchema " << initial_name << std::endl;
  };
  
  ~TraitSchema() {
    name_.clear();
    attributes_.clear();
  }
  
  /** get the name of traitschema */
  std::string GetName() const {
    return this->name_;
  }
  
  /** register a traitschema */
  Status Register(const std::string &name,
                  std::unordered_map<std::string, std::string>,
                  Context &ctx);
};
  
class Trait {
 private:
  std::string name_;
  std::unordered_map<std::string, std::string> attributes_;
      
 public:
  /** internal HERMES object owned by trait */
  std::shared_ptr<HERMES> m_HERMES_;
  
  // TBD
  static const Trait kDefault;
        
  Trait() : name_("default_trait") {
    //TODO: initialize kDefault
    LOG(INFO) << "Create default Trait " << std::endl;
  };
        
  Trait(std::string initial_name) : name_(initial_name) {
    //TODO: initialize kDefault
    LOG(INFO) << "Create Trait " << initial_name << std::endl;
  };
  
  ~Trait() {
    name_.clear();
    attributes_.clear();
  }
      
  /** get the name of trait */
  std::string GetName() const {
    return this->name_;
  }
  
  /** acquire a trait schema and populate to this trait as a side-effect */
  Trait& Acquire(const std::string &trtschema_name,
                std::unordered_map<std::string, std::string>,
                Context &ctx);
  
  /** update a trait property */
  Status EditTrait(const std::string &key,
                   const std::string &value,
                   Context &ctx);
}; // class Trait

}  // api
}  // hermes

#endif  //  TRAIT_H_
