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

#ifndef HERMES_SRC_PREFETCHER_FACTORY_H_
#define HERMES_SRC_PREFETCHER_FACTORY_H_

#include "prefetcher.h"
#include "prefetcher/apriori_prefetcher.h"

namespace hermes {

using hermes::PrefetcherType;

/**
 * A class to represent Data Placement Engine Factory
 * */
class PrefetcherFactory {
 public:
  /**
   * return a pointer to prefetcher policy given a policy type.
   * This uses factory pattern.
   *
   * @param[in] type a prefetcher policy type
   * @return pointer to PrefetcherPolicy
   */
  static PrefetcherPolicy* Get(const PrefetcherType &type) {
    switch (type) {
      case PrefetcherType::kApriori: {
        return hshm::EasySingleton<AprioriPrefetcher>::GetInstance();
      }
      default: {
        HELOG(kFatal, "PlacementPolicy not implemented")
        return NULL;
      }
    }
  }
};

}  // namespace hermes

#endif  // HERMES_SRC_PREFETCHER_FACTORY_H_
