#include <iostream>

#include "hermes.h"
#include "trait.h"
#include "bucket.h"


int main()
{
  hermes::api::HERMES hermes_app;
    
  hermes::api::Trait my_trait("compression");
    
  hermes::api::Bucket my_bucket("application");
    
  return 0;
}
