//
// Created by lukemartinlogan on 5/30/23.
//

#ifndef HERMES_SRC_PREFETCHER_APRIORI_PREFETCHER_H_
#define HERMES_SRC_PREFETCHER_APRIORI_PREFETCHER_H_

#include "prefetcher.h"

namespace hermes {

class AprioriPrefetcher : public PrefetcherPolicy {
 public:
  /** Constructor. Parse YAML schema. */
  AprioriPrefetcher();

  /** Destructor. */
  virtual ~AprioriPrefetcher() = default;

  /** Prefetch based on YAML schema */
  void Prefetch(BinaryLog<IoStat> &log);
};

}  // namespace hermes

#endif  // HERMES_SRC_PREFETCHER_APRIORI_PREFETCHER_H_
