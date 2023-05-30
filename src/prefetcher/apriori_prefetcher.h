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
