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

#include <cstdlib>
#include <string>

#include <mpi.h>
#include "hermes_shm/util/logging.h"
#include "hermes.h"
#include "bucket.h"
#include "traits/example/example_trait.h"

#include "basic_test.h"

namespace hapi = hermes::api;

using hermes::TagId;

void MainPretest() {
  hapi::Hermes::Create(hermes::HermesType::kClient);
}

void MainPosttest() {
  HERMES->Finalize();
}

void TestTrait() {
  auto bkt = HERMES->GetBucket("hello");
  size_t num_blobs = 100;
  hermes::api::Context ctx;
  std::vector<hermes::BlobId> blob_ids(num_blobs);

  // Create a trait
  HERMES->RegisterTrait<hapi::ExampleTrait>("ex", 5);

  // Get the trait id
  hermes::TraitId trait_id = HERMES->GetTraitId("ex");
  REQUIRE(!trait_id.IsNull());

  // Get the trait itself
  auto *trait = HERMES->GetTrait(trait_id);
  hapi::ExampleTraitParams params;
  trait->Run(0, &params);
  auto hello = params.hello_;
  REQUIRE(hello == 5);
}

TEST_CASE("TestTrait") {
  TestTrait();
}

