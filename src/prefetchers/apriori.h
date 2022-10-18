//
// Created by lukemartinlogan on 10/18/22.
//

#ifndef HERMES_SRC_PREFETCHERS_APRIORI_H_
#define HERMES_SRC_PREFETCHERS_APRIORI_H_

#include "prefetcher.h"

namespace hermes {

class AprioriPrefetcher : public PrefetchAlgorithm {
 public:
  void Process(std::list<IoLogEntry> &log,
               PrefetchSchema &schema) {}
};

}

#endif  // HERMES_SRC_PREFETCHERS_APRIORI_H_
