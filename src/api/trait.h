#ifndef TRAIT_H_
#define TRAIT_H_

#include <memory>
#include <string>

#include "glog/logging.h"

#include "hermes.h"
#include "vbucket.h"

namespace hermes {

namespace api {

class Trait {
 private:
  std::string name_;
      
 public:
  /** internal HERMES object owned by trait */
  std::shared_ptr<HERMES> m_HERMES_;

  // TBD
  static const Trait kDefault;
        
  Trait() : name_("default") {
    //TODO: initialize kDefault
    LOG(INFO) << "Create default Trait " << std::endl;
  };
        
  Trait(std::string initial_name) : name_(initial_name) {
    //TODO: initialize kDefault
    LOG(INFO) << "Create Trait " << initial_name << std::endl;
  };
  
  ~Trait() {
    name_.clear();
  }
      
  /** get the name of trait */
  std::string GetName() const {
    return this->name_;
  }

  /** register a trait */
  Status Register(std::string& name, const std::string& key,
                  const std::string& value, Context& ctx);
  
  /** update a trait property */
  Status EditTrait(const std::string& key,
                   const std::string& value,
                   Context& ctx);

  /** acquire a bucket and link to this trait as a side-effect */
  Bucket Acquire(const std::string& name, Context& ctx);
  
  /** link this trait to a vbucket */
  Status Link(const VBucket& vb, Context& ctx);
  
  /** unlink this trait from a vbucket */
  Status Unlink(const VBucket& vb, Context& ctx);
}; // class Trait

}  // api
}  // hermes

#endif  //  TRAIT_H_
