#ifndef BUCKET_H_
#define BUCKET_H_

#include <memory>
#include <string>
#include <unordered_map>

#include "glog/logging.h"

#include "hermes.h"

namespace hermes {

namespace api {

class Bucket {
 private:
  std::string name_;
  std::unordered_map<std::string, uint64_t> blobs_;
      
 public:
  /** internal HERMES object owned by Bucket */
  std::shared_ptr<HERMES> m_HERMES_;
        
  // TODO: Think about the Big Three
	Bucket() : name_("") {
    LOG(INFO) << "Create NULL Bucket " << std::endl;
  }
	
  Bucket(std::string initial_name, std::shared_ptr<HERMES> const &h) : name_(initial_name), m_HERMES_(h) {
    LOG(INFO) << "Create Bucket " << initial_name << std::endl;
		if (m_HERMES_->bucket_list_.find(initial_name) == m_HERMES_->bucket_list_.end())
		  m_HERMES_->bucket_list_.insert(initial_name);
		else
			std::cerr << "Bucket " << initial_name << " exists\n";
  }
  
  ~Bucket() {
    name_.clear();
    blobs_.clear();
  }
      
  /** get the name of bucket */
  std::string GetName() const {
    return this->name_;
  }

	/** check if a blob name exists in this bucket*/
	Status Contain_blob(const std::string& blob_name);
	
  /** rename this bucket */
  Status Rename(const std::string& new_name,
                Context &ctx);

  /** release this bucket and free its associated resources */
  Status Release(Context &ctx);

  /** put a blob on this bucket */
  Status Put(const std::string &name, const Blob &data, Context &ctx);

  /** get a blob on this bucket */
	/** - if user_blob.size() == 0 => return the minimum buffer size needed */
	/** - if user_blob.size() > 0 => copy user_blob.size() bytes */
	/** to user_blob and return user_blob.size() */
  size_t Get(const std::string &name, Blob& user_blob, Context &ctx);

  /** delete a blob from this bucket */
  Status DeleteBlob(const std::string &name, Context &ctx);

  /** rename a blob on this bucket */
  Status RenameBlob(const std::string &old_name,
                    const std::string &new_name,
                    Context &ctx);
}; // Bucket class
  
}  // api namespace
}  // hermes namespace

#endif  // BUCKET_H_
