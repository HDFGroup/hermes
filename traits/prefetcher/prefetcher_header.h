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

#ifndef HERMES_TRAITS_PREFETCHER_PREFETCHER_HEADER_H_
#define HERMES_TRAITS_PREFETCHER_PREFETCHER_HEADER_H_

#include "trait_manager.h"

namespace hermes {

/** Types of prefetchers available */
enum class PrefetcherType {
  kApriori
};

/** Header for prefetcher trait */
struct PrefetcherTraitHeader : public TraitHeader {
  hermes::PrefetcherType type_;
  explicit PrefetcherTraitHeader(const std::string &trait_uuid,
                                 const std::string &trait_name,
                                 hermes::PrefetcherType type)
    : TraitHeader(trait_uuid, trait_name, HERMES_TRAIT_PREFETCHER),
      type_(type) {}
};

}  // namespace hermes::api

#endif  // HERMES_TRAITS_PREFETCHER_PREFETCHER_HEADER_H_
