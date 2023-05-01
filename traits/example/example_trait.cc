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

#include "example_trait.h"

namespace hermes::api {

void ExampleTrait::Run(int method, void *params) {
  (void) method;
  auto cast = reinterpret_cast<ExampleTraitParams*>(params);
  cast->hello_ = GetHeader<ExampleTraitHeader>()->hello_;
}

}  // namespace hermes::api

HERMES_TRAIT_CC(hermes::api::ExampleTrait)