#ifndef BUCKET_H_
#define BUCKET_H_

#include <memory>
#include <string>

#include "glog/logging.h"

#include "hermes.h"

namespace hermes {

namespace api {

class Bucket {
 private:
  std::string name_;
  std::vector<Blob> blobs_;
      
 public:
  /** internal HERMES object owned by Bucket */
  std::shared_ptr<HERMES> m_HERMES_;
        
  // TODO: Think about the Big Three
        
  Bucket() : name_("") {
    LOG(INFO) << "Create NULL Bucket " << std::endl;
  }
        
  Bucket(std::string initial_name) : name_(initial_name) {
    LOG(INFO) << "Create Bucket " << initial_name << std::endl;
  }
      
  /** get the name of bucket */
  std::string GetName() const {
    return this->name_;
  }

  /** rename this bucket */
  Status Rename(const std::string& new_name,
                Context& ctx);

  /** release this bucket and free its associated resources */
  Status Release(Context& ctx);

  /** put a blob on this bucket */
  Status Put(const std::string& name, const Blob& data, Context& ctx);

  /** get a blob on this bucket */
  const Blob& Get(const std::string& name, Context& ctx);

  /** delete a blob from this bucket */
  Status DeleteBlob(const std::string& name, Context& ctx);

  /** rename a blob on this bucket */
  Status RenameBlob(const std::string& old_name,
                    const std::string& new_name,
                    Context& ctx);
}; // Bucket class
  
}  // api namespace
}  // hermes namespace

#endif  // BUCKET_H_
