#ifndef HERMES_H_
#define HERMES_H_

#include <cstdint>
#include <string>
#include <set>
#include <iostream>

#include "glog/logging.h"

#include "id.h"

namespace hermes {

namespace api {
  
typedef int Status;
  
class HERMES {
 public:
	std::set<std::string> bucket_list_;
	std::set<std::string> vbucket_list_;
	
  /** if true will do more checks, warnings, expect slower code */
  const bool m_debug_mode_ = true;
	
	void Display_bucket() {
		for (auto it = bucket_list_.begin(); it != bucket_list_.end(); ++it)
			std::cout << *it << '\t';
		std::cout << '\n';
	}
	
	void Display_vbucket() {
		for (auto it = vbucket_list_.begin(); it != vbucket_list_.end(); ++it)
			std::cout << *it << '\t';
		std::cout << '\n';
	}

  // MPI comms.
  // proxy/reference to Hermes core
};

class TraitSchema;
  
class VBucket;

class Bucket;

typedef std::vector<unsigned char> Blob;

class Context {
};

struct TraitTag{};
typedef ID<TraitTag, int64_t, -1> THnd;

struct BucketTag{};
typedef ID<BucketTag, int64_t, -1> BHnd;

/** acquire a bucket with the default trait */
Bucket Acquire(const std::string &name, Context &ctx);

/** rename a bucket referred to by name only */
Status RenameBucket(const std::string &old_name,
                    const std::string &new_name,
                    Context &ctx);

/** transfer a blob between buckets */
Status TransferBlob(const Bucket &src_bkt,
                    const std::string &src_blob_name,
                    Bucket &dst_bkt,
                    const std::string &dst_blob_name,
                    Context &ctx);
  
}  // api namespace
}  // hermes namespace

#endif  // HERMES_H_
