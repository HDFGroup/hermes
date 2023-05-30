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
#include <vector>

namespace hermes {

struct AprioriPromoteInstr {
  std::string bkt_name_;
  std::vector<std::string> promote_;
  std::vector<std::string> demote_;
};

struct AprioriPrefetchInstr {
  size_t min_op_count_;
  size_t max_op_count_;
  std::vector<AprioriPromoteInstr> promotes_;
};

class AprioriPrefetcher : public PrefetcherPolicy {
 public:
  std::vector<std::list<AprioriPrefetchInstr>> rank_info_;

 public:
  /** Constructor. Parse YAML schema. */
  AprioriPrefetcher();

  /** Parse YAML config */
  void ParseSchema(YAML::Node &schema);

  /** Destructor. */
  virtual ~AprioriPrefetcher() = default;

  /** Prefetch based on YAML schema */
  void Prefetch(BufferOrganizer *borg, BinaryLog<IoStat> &log);
};

}  // namespace hermes

#endif  // HERMES_SRC_PREFETCHER_APRIORI_PREFETCHER_H_
