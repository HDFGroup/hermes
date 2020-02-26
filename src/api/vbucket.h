#ifndef VBUCKET_H_
#define VBUCKET_H_

#include <string>

#include "glog/logging.h"

#include "hermes.h"

namespace hermes {
  
namespace api {
    
class VBucket {
 private:
  std::string name_;
      
 public:
  /** internal HERMES object owned by vbucket */
  std::shared_ptr<HERMES> m_HERMES_;
      
  VBucket(std::string initial_name) : name_(initial_name) {
    LOG(INFO) << "Create VBucket " << initial_name << std::endl;
  };
      
  ~VBucket() {
    name_.clear();
  }
      
  /** get the name of vbucket */
  std::string GetName() const {
    return this->name_;
  }
      
  /** create a vbucket */
  VBucket Create(const std::string& name, Context& ctx);
  
  /** attach a trait to this vbucket */
  Status Attach(const Trait& trt, Context& ctx);
  
  /** detach a trait to this vbucket */
  Status Detach(const Trait& trt, Context& ctx);
}; // class VBucket
    
}  // api
}  // hermes

#endif  //  VBUCKET_H_
