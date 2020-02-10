#include "trait.h"
#include "bucket.h"
#include "glog/logging.h"

hermes::api::Trait::Trait ()
{
    LOG(INFO) << "Create default Trait " << std::endl;
    
    name = "default";
}

hermes::api::Trait::Trait (std::string initial_name)
{
    LOG(INFO) << "Create Trait " << initial_name << std::endl;
    
    name = initial_name;
}

hermes::api::Status hermes::api::Trait::EditTrait(const std::string& key, const std::string& value, Context& ctx)
{
    hermes::api::Status ret = 0;
    
    LOG(INFO) << "Edit bucket " << std::endl;
    
    return ret;
}

hermes::api::Bucket hermes::api::Trait::Acquire(const std::string& name, Context& ctx)
{
    Bucket ret;
    
    LOG(INFO) << "Acquire bucket " << name << std::endl;
    
    return ret;
}

hermes::api::Status hermes::api::Trait::Link(const Bucket& bkt, Context& ctx)
{
    hermes::api::Status ret = 0;
    
    LOG(INFO) << "Link " << bkt.name << " to trait " << name << std::endl;
    
    return ret;
}

hermes::api::Status hermes::api::Trait::Unlink(const Bucket& bkt, Context& ctx)
{
    hermes::api::Status ret = 0;
    
    LOG(INFO) << "Unlink trait " << name << std::endl;
    
    return ret;
}
