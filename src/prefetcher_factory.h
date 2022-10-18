//
// Created by lukemartinlogan on 10/18/22.
//

#ifndef HERMES_SRC_PREFETCHER_FACTORY_H_
#define HERMES_SRC_PREFETCHER_FACTORY_H_

#include "prefetcher.h"
#include <memory>

#include "prefetchers/sequential.h"
#include "prefetchers/apriori.h"

namespace hermes {

class PrefetcherFactory {
 public:
  /**
   * Return the instance of mapper given a type. Uses factory pattern.
   *
   * @param type, MapperType, type of mapper to be used by the STDIO adapter.
   * @return Instance of mapper given a type.
   */
  static std::unique_ptr<PrefetchAlgorithm> Get(const PrefetchHint &type) {
    switch (type) {
      case PrefetchHint::kSequential: {
        return std::make_unique<SequentialPrefetcher>();
      }
      case PrefetchHint::kApriori: {
        return std::make_unique<AprioriPrefetcher>();
      }
      default: return nullptr;
    }
  }
};

}

#endif  // HERMES_SRC_PREFETCHER_FACTORY_H_
