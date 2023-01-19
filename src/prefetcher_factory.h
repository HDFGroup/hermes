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
#include <memory>

#include "prefetchers/sequential.h"
#include "prefetchers/apriori.h"
#include "singleton.h"

namespace hermes {

class PrefetcherFactory {
 public:
  /**
   * Return the instance of mapper given a type. Uses factory pattern.
   *
   * @param type, MapperType, type of mapper to be used by the STDIO adapter.
   * @return Instance of mapper given a type.
   */
  static PrefetchAlgorithm* Get(const PrefetchHint &type) {
    switch (type) {
      case PrefetchHint::kFileSequential: {
        return Singleton<SequentialPrefetcher>::GetInstance();
      }
      case PrefetchHint::kApriori: {
        return Singleton<AprioriPrefetcher>::GetInstance();
      }
      default: return nullptr;
    }
  }
};

}  // namespace hermes

#endif  // HERMES_SRC_PREFETCHER_FACTORY_H_
