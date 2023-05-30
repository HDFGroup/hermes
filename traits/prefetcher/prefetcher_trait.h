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
#include "prefetcher_header.h"

namespace hermes {

/** Prefetcher trait */
class PrefetcherTrait : public Trait {
 public:
  HERMES_TRAIT_H(PrefetcherTrait, "PrefetcherTrait");

 public:
  explicit PrefetcherTrait(hshm::charbuf &data) : Trait(data) {}

  explicit PrefetcherTrait(const std::string &trait_uuid,
                           hermes::PrefetcherType prefetch_type) {
    CreateHeader<PrefetcherTraitHeader>(trait_uuid,
                                        trait_name_,
                                        prefetch_type);
  }

  void Run(int method, void *params) override;
};

}  // namespace hermes

#endif  // HERMES_TRAITS_EXAMPLE_EXAMPLE_TRAIT_H_
