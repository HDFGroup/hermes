#include "bucket.h"
#include "glog/logging.h"

hermes::api::Bucket hermes::api::Acquire(const std::string& name, Context& ctx)
{
    hermes::api::Bucket ret;
    
    LOG(INFO) << "Acquire Bucket " << name << std::endl;
    
    return ret;
}

hermes::api::Status hermes::api::RenameBucket(const std::string& old_name,
                    const std::string& new_name,
                    Context& ctx)
{
    hermes::api::Status ret = 0;
    
    LOG(INFO) << "Rename Bucket from " << old_name << " to " << new_name << std::endl;
    
    return ret;
}

hermes::api::Status hermes::api::TransferBlob(const Bucket& src_bkt,
                    const std::string& src_blob_name,
                    Bucket& dst_bkt,
                    const std::string& dst_blob_name,
                    Context& ctx)
{
    hermes::api::Status ret = 0;
    
    LOG(INFO) << "Transfer Blob from " << src_blob_name << " to " << dst_blob_name << std::endl;
    
    return ret;
}
