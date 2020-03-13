#ifndef VBUCKET_H_
#define VBUCKET_H_

#include <string>
#include <list>

#include "glog/logging.h"

#include "hermes.h"

namespace hermes {
  
namespace api {
  
class VBucket {
 private:
  std::string name_;
	std::list<std::pair<std::string,std::string>> linked_blobs_;
      
 public:
  /** internal HERMES object owned by vbucket */
  std::shared_ptr<HERMES> m_HERMES_;
      
  VBucket(std::string initial_name, std::shared_ptr<HERMES> const &h)
	  : name_(initial_name), m_HERMES_(h) {
    LOG(INFO) << "Create VBucket " << initial_name << std::endl;
		if (m_HERMES_->vbucket_list_.find(initial_name) == m_HERMES_->vbucket_list_.end())
		  m_HERMES_->vbucket_list_.insert(initial_name);
		else
			std::cerr << "VBucket " << initial_name << " exists\n";
  };
      
  ~VBucket() {
    name_.clear();
    linked_blobs_.clear();
  }
      
  /** get the name of vbucket */
  std::string GetName() const {
    return this->name_;
  }
	
	/** check if blob is in this vbucket*/
	Status Contain_blob(std::string blob_name, std::string bucket_name);
	
	/** Get a blob linked to  this vbucket*/
	Blob& Get_blob(std::string blob_name, std::string bucket_name);
	
	typedef int (TraitFunc)(Blob &blob, void *trait);
	
	/** attach a trait to this vbucket */
	Status Attach(void *trait, TraitFunc *func, Context& ctx);
	
	/** detach a trait to this vbucket */
  Status Detach(void *trait, Context& ctx);
  
  /** link a blob to this vbucket */
  Status Link(std::string blob_name, std::string bucket_name, Context &ctx);
  
  /** unlink a blob from this vbucket */
  Status Unlink(std::string blob_name, std::string bucket_name, Context &ctx);
}; // class VBucket

}  // api
}  // hermes

#endif  //  VBUCKET_H_
