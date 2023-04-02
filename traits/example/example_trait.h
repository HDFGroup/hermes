/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
* Distributed under BSD 3-Clause license.                                   *
* Copyright by The HDF Group.                                               *
* Copyright by the Illinois Institute of Technology.                        *
* All rights reserved.                                                      *
*                                                                           *
* This file is part of Hermes. The full Hermes copyright notice, including  *
* terms governing use, modification, and redistribution, is contained in    *
* the COPYING file, which can be found at the top directory. If you do not  *
* have access to the file, you may request a copy from help@hdfgroup.org.   *
* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#ifndef HERMES_TRAITS_EXAMPLE_EXAMPLE_TRAIT_H_
#define HERMES_TRAITS_EXAMPLE_EXAMPLE_TRAIT_H_

#include "hermes.h"

namespace hermes::api {

struct ExampleTraitHeader : public TraitHeader {
  int hello_;
  explicit ExampleTraitHeader(const std::string &trait_uuid, int hello)
      : TraitHeader(trait_uuid, HERMES_TRAIT_PUT_GET),
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
