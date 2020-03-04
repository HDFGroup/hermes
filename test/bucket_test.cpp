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
  
  std::array<std::string,3> names {{"foo", "bar", "com"}};
  
  hermes::api::TraitSchema<int, char, bool> ts("compression", names);
  
  hermes::api::TraitSchema<int, char, bool>::Trait trt = ts.CreateInstance(ctx, 9, 'a', true);
  hermes::api::VBucket<int, char, bool> my_vb("VB1");
  my_vb.Attach(trt, ctx);

  hermes::api::Bucket my_bucket("pressure");
  hermes::api::Blob p1;
  my_bucket.Put("Blob1", p1, ctx);
  
  my_vb.Link("Blob1", "pressure", ctx);
  
  my_vb.Unlink("Blob1", "pressure", ctx);
  my_vb.Detach(trt, ctx);
 
  return 0;
}
