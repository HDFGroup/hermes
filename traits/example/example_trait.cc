//
// Created by lukemartinlogan on 3/11/23.
//

#include "example_trait.h"

namespace hermes::api {

void ExampleTrait::Run() {
  std::cout << GetHeader<ExampleTraitHeader>()->hello_ << std::endl;
}

}  // namespace hermes::api
