#include "vbucket.h"

namespace hermes {
  
namespace api {

Status VBucket::Link(std::string blob_name, std::string bucket_name, Context& ctx) {
  (void)ctx;
  Status ret = 0;
    
  LOG(INFO) << "Linking blob "<< blob_name << " in bucket "
            << bucket_name << " to VBucket " << name_ << '\n';
	
	// inserting value by insert function
	linked_blobs_.push_back(make_pair(bucket_name, blob_name));
    
  return ret;
}
  
Status VBucket::Unlink(std::string blob_name, std::string bucket_name, Context& ctx) {
  (void)ctx;
  Status ret = 0;
    
  LOG(INFO) << "Unlinking blob "<< blob_name << " in bucket "
            << bucket_name << " from VBucket " << name_ << '\n';
    
  return ret;
}
  
Status VBucket::Contain_blob(std::string blob_name, std::string bucket_name) {
	Status ret = 0;
	std::string bk_tmp, blob_tmp;
    
  LOG(INFO) << "Checking if blob "<< blob_name << " from bucket "
            << bucket_name << " is in this VBucket " << name_ << '\n';
	
	for (auto ci = linked_blobs_.begin(); ci != linked_blobs_.end(); ++ci) {
		bk_tmp = ci->first;
		blob_tmp = ci->second;
		if (bk_tmp == bucket_name && blob_tmp == blob_name)
			ret = 1;
	}
    
  return ret;
}

Blob& VBucket::Get_blob(std::string blob_name, std::string bucket_name) {
	LOG(INFO) << "Retrieving blob "<< blob_name << " from bucket "
	          << bucket_name << " in VBucket " << name_ << '\n';
	
	// get blob by bucket and blob name;
}
  
template<class Predicate>
std::vector<std::string> VBucket::GetLinks(Predicate pred, Context &ctx) {
	LOG(INFO) << "Getting subset of links satisfying pred in VBucket "
	          << name_ << '\n';
}

Status VBucket::Attach(void *trait, TraitFunc *func, Context& ctx) {
  (void)ctx;
  (void)trait;
  Status ret = 0;
  
  LOG(INFO) << "Attaching trait to VBucket " << name_ << '\n';
	
	for (auto ci = linked_blobs_.begin(); ci != linked_blobs_.end(); ++ci) {
		Blob &blob = Get_blob(ci->second, ci->first);
    (void)blob;
    (void)func;
//		func(blob, trait);
	}
  
  return ret;
}
  
Status VBucket::Detach(void *trait, Context& ctx) {
  (void)ctx;
  (void)trait;
  Status ret = 0;
  
  LOG(INFO) << "Detaching trait from VBucket " << name_ << '\n';
  
  return ret;
}

template<class Predicate>
std::vector<std::string> VBucket::GetTraits(Predicate pred, Context& ctx) {
	(void)ctx;
	
	LOG(INFO) << "Getting the subset of attached traits satisfying pred in VBucket "
						<< name_ << '\n';
}

Status VBucket::Delete(Context& ctx) {
	(void)ctx;
	
	LOG(INFO) << "Deleting VBucket " << name_ << '\n';
}

} // api namepsace
} // hermes namespace
