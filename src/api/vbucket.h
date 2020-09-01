#ifndef VBUCKET_H_
#define VBUCKET_H_

#include <list>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "glog/logging.h"

#include "hermes.h"

namespace hermes {

namespace api {

class VBucket {
 private:
  std::string name_;
  std::list<std::pair<std::string, std::string>> linked_blobs_;

 public:
  /** internal Hermes object owned by vbucket */
  std::shared_ptr<Hermes> hermes_;

  VBucket(std::string initial_name, std::shared_ptr<Hermes> const &h)
    : name_(initial_name), hermes_(h) {
    LOG(INFO) << "Create VBucket " << initial_name << std::endl;
    if (hermes_->vbucket_list_.find(initial_name) ==
        hermes_->vbucket_list_.end())
      hermes_->vbucket_list_.insert(initial_name);
    else
      std::cerr << "VBucket " << initial_name << " exists\n";
  }

  ~VBucket() {
    name_.clear();
    linked_blobs_.clear();
  }

  /** get the name of vbucket */
  std::string GetName() const {
    return this->name_;
  }

  /** link a blob to this vbucket */
  Status Link(std::string blob_name, std::string bucket_name, Context &ctx);

  /** unlink a blob from this vbucket */
  Status Unlink(std::string blob_name, std::string bucket_name, Context &ctx);

  /** check if blob is in this vbucket */
  Status Contain_blob(std::string blob_name, std::string bucket_name);

  /** get a blob linked to this vbucket */
  Blob& Get_blob(std::string blob_name, std::string bucket_name);

  /** retrieves the subset of links satisfying pred */
  /** could return iterator */
  template<class Predicate>
  std::vector<std::string> GetLinks(Predicate pred, Context &ctx);

  typedef int (TraitFunc)(Blob &blob, void *trait);

  /** attach a trait to this vbucket */
  Status Attach(void *trait, TraitFunc *func, Context& ctx);

  /** detach a trait to this vbucket */
  Status Detach(void *trait, Context& ctx);

  /** retrieves the subset of attached traits satisfying pred */
  template<class Predicate>
  std::vector<std::string> GetTraits(Predicate pred, Context& ctx);

  /** delete a vBucket */
  /** decrements the links counts of blobs in buckets */
  Status Delete(Context& ctx);
};  // class VBucket

}  // namespace api
}  // namespace hermes

#endif  //  VBUCKET_H_
