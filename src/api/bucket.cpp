#include "bucket.h"
#include "glog/logging.h"
#include <iostream>

hermes::api::Bucket::Bucket ()
{
    LOG(INFO) << "Create NULL Bucket " << std::endl;
    
    name = "";
}

hermes::api::Bucket::Bucket (std::string initial_name)
{
    LOG(INFO) << "Create Bucket " << initial_name << std::endl;
    
    name = initial_name;
}

hermes::api::Status hermes::api::Bucket::Rename(const std::string& new_name, Context& ctx)
{
    hermes::api::Status ret = 0;
    
    LOG(INFO) << "Rename a bucket to" << new_name << std::endl;
    
    return ret;
}


hermes::api::Status hermes::api::Bucket::Release(Context& ctx)
{
    hermes::api::Status ret = 0;
    
    LOG(INFO) << "Release bucket " << std::endl;
    
    return ret;
}

hermes::api::Status hermes::api::Bucket::Put(const std::string& name, const Blob& data, Context& ctx)
{
    hermes::api::Status ret = 0;
    
    LOG(INFO) << "Attach blol " << name << "to Bucket " << std::endl;
    
    return ret;
}

const hermes::api::Blob& hermes::api::Bucket::Get(const std::string& name, Context& ctx)
{
    hermes::api::Blob& ret = blobs[0];
    
    LOG(INFO) << "Get Blob " << name << std::endl;
    
    return ret;
}

hermes::api::Status hermes::api::Bucket::DeleteBlob(const std::string& name, Context& ctx)
{
    hermes::api::Status ret = 0;
    
    LOG(INFO) << "Delete Blob " << name << std::endl;
    
    return ret;
}

hermes::api::Status hermes::api::Bucket::RenameBlob(const std::string& old_name,
                  const std::string& new_name,
                  Context& ctx)
{
    hermes::api::Status ret = 0;
    
    LOG(INFO) << "Rename Blob " << old_name << " to " << new_name << std::endl;
    
    return ret;
}
