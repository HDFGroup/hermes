//
// Created by lukemartinlogan on 10/18/22.
//

#ifndef HERMES_SRC_PREFETCHERS_SEQUENTIAL_H_
#define HERMES_SRC_PREFETCHERS_SEQUENTIAL_H_

#include "prefetcher.h"

namespace hermes {

class SequentialPrefetcher : public PrefetchAlgorithm {
 public:
  void Process(std::list<IoLogEntry> &log,
               PrefetchSchema &schema) {}
};

}

#endif  // HERMES_SRC_PREFETCHERS_SEQUENTIAL_H_
