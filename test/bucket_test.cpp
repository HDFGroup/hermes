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
  
  std::array<std::string,3> names = {"foo", "bar", "com"};
  //  using icb_schema = hermes::api::TraitSchema<int, char, bool>;
  hermes::api::TraitSchema<int, char, bool> ts(names);
  
  hermes_app.Register(ts, "compression", ctx);
  
  hermes::api::Trait trt = ts.CreateInstance(ctx);
  
  hermes::api::Bucket my_bucket("pressure");
  hermes::api::Blob p1;
  my_bucket.Put("Blob1", p1, ctx);
  
  hermes::api::VBucket my_vb("VB1");
  my_vb.Attach(trt, ctx);
  my_vb.Link("Blob1", my_bucket.GetName(), ctx);
  
  my_vb.Unlink("Blob1", my_bucket.GetName(), ctx);
  my_vb.Detach(trt, ctx);
  
  return 0;
}
