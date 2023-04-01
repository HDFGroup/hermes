//
// Created by lukemartinlogan on 3/11/23.
//

#ifndef HERMES_TRAITS_EXAMPLE_EXAMPLE_TRAIT_H_
#define HERMES_TRAITS_EXAMPLE_EXAMPLE_TRAIT_H_

#include "hermes.h"

namespace hermes::api {

struct ExampleTraitHeader : public TraitHeader {
  int hello_;
  explicit ExampleTraitHeader(const std::string &trait_uuid, int hello)
      : TraitHeader(trait_uuid, TraitClass::kBucket),
        hello_(hello) {}
};

struct ExampleTraitParams {
  int hello_;
};

class ExampleTrait : public Trait {
 public:
  HERMES_TRAIT_H(ExampleTrait, "ExampleTrait");

 public:
  explicit ExampleTrait(hshm::charbuf &data) : Trait(data) {}

  explicit ExampleTrait(const std::string &trait_uuid, int hello) {
    auto hdr = CreateHeader<ExampleTraitHeader>(trait_uuid, hello);
    hdr->hello_ = hello;
  }

  void Run(int method, void *params) override;
};

}  // namespace hermes::api

#endif  // HERMES_TRAITS_EXAMPLE_EXAMPLE_TRAIT_H_
