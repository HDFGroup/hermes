#ifndef VBUCKET_H_
#define VBUCKET_H_

#include <string>
#include <unordered_map>

#include "glog/logging.h"

#include "hermes.h"
#include "trait.h"

namespace hermes {
  
namespace api {
  
template <class... Args>
class VBucket {
 private:
  std::string name_;
  
  /** store the pair of bucket and blob name that is mapped to this VBucket */
  std::unordered_map<std::string, std::string> mapping_;
      
 public:
  /** internal HERMES object owned by vbucket */
  std::shared_ptr<HERMES> m_HERMES_;
      
  VBucket(std::string initial_name) : name_(initial_name) {
    LOG(INFO) << "Create VBucket " << initial_name << std::endl;
  };
      
  ~VBucket() {
    name_.clear();
    mapping_.clear();
  }
      
  /** get the name of vbucket */
  std::string GetName() const {
    return this->name_;
  }
  
  /** link a blob to this vbucket */
  Status Link(std::string blob_name, std::string bucket_name, Context &ctx);
  
  /** unlink a blob from this vbucket */
  Status Unlink(std::string blob_name, std::string bucket_name, Context &ctx);
  
  /** attach a trait to this vbucket */
  Status Attach(typename TraitSchema<Args...>::Trait &trt, Context &ctx);
  
  /** detach a trait to this vbucket */
  Status Detach(typename TraitSchema<Args...>::Trait &trt, Context &ctx);
}; // class VBucket
  
template <class... Args>
Status VBucket<Args...>::Attach(typename TraitSchema<Args...>::Trait &trt, Context &ctx) {
  Status ret = 0;
    
  LOG(INFO) << "Attaching trait to VBucket " << name_ << '\n';
    
  return ret;
}
  
template <class... Args>
Status VBucket<Args...>::Detach(typename TraitSchema<Args...>::Trait& trt, Context& ctx) {
  Status ret = 0;
    
  LOG(INFO) << "Detaching trait from VBucket " << name_ << '\n';
    
  return ret;
}
  
template <class... Args>
Status VBucket<Args...>::Link(std::string blob_name, std::string bucket_name, Context& ctx) {
  Status ret = 0;
    
  LOG(INFO) << "Linking blob "<< blob_name << " in bucket "
            << bucket_name << " to VBucket " << name_ << '\n';
    
  return ret;
}
  
template <class... Args>
Status VBucket<Args...>::Unlink(std::string blob_name, std::string bucket_name, Context& ctx) {
  Status ret = 0;
    
  LOG(INFO) << "Unlinking blob "<< blob_name << " in bucket "
            << bucket_name << " from VBucket " << name_ << '\n';
    
  return ret;
}

}  // api
}  // hermes

#endif  //  VBUCKET_H_
