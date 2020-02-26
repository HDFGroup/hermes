#include <iostream>
#include <unordered_map>

#include "hermes.h"
#include "trait.h"
#include "bucket.h"
#include "vbucket.h"


int main()
{
  hermes::api::HERMES hermes_app;
  hermes::api::Context ctx;
    
  hermes::api::TraitSchema my_trtschema("compression");
  
  std::unordered_map<std::string, std::string> kvalues;
  kvalues["IIT"] = "app1";
  
  my_trtschema.Register(my_trtschema.GetName(), kvalues, ctx);
  
  hermes::api::Trait my_trt("lable_trt");
  my_trt.Acquire(my_trt.GetName(), kvalues, ctx);
  
  hermes::api::Bucket my_bucket("pressure");
  hermes::api::Blob p1;
  my_bucket.Put("Blob1", p1, ctx);
  
  hermes::api::VBucket my_vb("VB1");
  my_vb.Attach(my_trt, ctx);
  my_vb.Link("Blob1", my_bucket.GetName(), ctx);
  
  my_vb.Unlink("Blob1", my_bucket.GetName(), ctx);
  my_vb.Detach(my_trt, ctx);
  
  return 0;
}
