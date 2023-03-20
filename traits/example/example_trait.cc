//
// Created by lukemartinlogan on 3/11/23.
//

#include "example_trait.h"

namespace hermes::api {

void ExampleTrait::Run(int method, void *params) {
  (void) method;
  auto cast = reinterpret_cast<ExampleTraitParams*>(params);
  cast->hello_ = GetHeader<ExampleTraitHeader>()->hello_;
}

}  // namespace hermes::api
