#ifndef BUCKET_H_
#define BUCKET_H_

#include <memory>
#include <string>
#include <unordered_map>

#include "glog/logging.h"

#include "hermes.h"
#include "metadata_management.h"

namespace hermes {

namespace api {

class Bucket {
 private:
  std::string name_;
  hermes::BucketID id_;
  // TODO(chogan): Move to MetadataManager
  std::unordered_map<std::string, std::vector<BufferID>> blobs_;

 public:
  /** internal Hermes object owned by Bucket */
  std::shared_ptr<Hermes> hermes_;

  // TODO: Think about the Big Three
	Bucket() : name_(""), id_{0, 0}, hermes_(nullptr) {
    LOG(INFO) << "Create NULL Bucket " << std::endl;
  }

  Bucket(const std::string &initial_name,
         std::shared_ptr<Hermes> const &h, Context ctx);

  ~Bucket() {
    Context ctx;
    Close(ctx);

    name_.clear();
    blobs_.clear();
    id_.as_int = 0;
  }

  /** get the name of bucket */
  std::string GetName() const {
    return this->name_;
  }

	/** open a bucket and retrieve the bucket handle */
	struct bkt_hdl * Open(const std::string &name, Context &ctx);

  /** */
  bool IsValid() const;

	/** put a blob on this bucket */
  Status Put(const std::string &name, const Blob &data, Context &ctx);

  /** get a blob on this bucket */
	/** - if user_blob.size() == 0 => return the minimum buffer size needed */
	/** - if user_blob.size() > 0 => copy user_blob.size() bytes */
	/** to user_blob and return user_blob.size() */
	/** use provides buffer */
  size_t Get(const std::string &name, Blob& user_blob, Context &ctx);

	/** get blob(s) on this bucket according to predicate */
	/** use provides buffer */
	template<class Predicate>
	Status GetV(void *user_blob, Predicate pred, Context &ctx);

	/** delete a blob from this bucket */
  Status DeleteBlob(const std::string &name, Context &ctx);

  /** rename a blob on this bucket */
  Status RenameBlob(const std::string &old_name,
                    const std::string &new_name,
                    Context &ctx);

	/** get a list of blob names filtered by pred */
	template<class Predicate>
	std::vector<std::string> GetBlobNames(Predicate pred,
	                                      Context &ctx);

	/** get information from the bucket at level-of-detail  */
	struct bkt_info * GetInfo(Context &ctx);

	/** rename this bucket */
  Status Rename(const std::string& new_name,
                Context &ctx);

	/** release this bucket and free its associated resources */
	/** check with Close function */
	Status Release(Context &ctx);

	/** close this bucket and free its associated resources (?) */
	/** Invalidates handle */
  Status Close(Context &ctx);

	/** destroy this bucket */
	/** ctx controls "aggressiveness */
  Status Destroy(Context &ctx);
}; // Bucket class

}  // api namespace
}  // hermes namespace

#endif  // BUCKET_H_
