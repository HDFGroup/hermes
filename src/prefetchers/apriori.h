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

#ifndef HERMES_SRC_PREFETCHERS_APRIORI_H_
#define HERMES_SRC_PREFETCHERS_APRIORI_H_

#include "prefetcher.h"

namespace hermes {

class AprioriPrefetcher : public PrefetchAlgorithm {
 public:
  void Process(std::list<IoLogEntry> &log,
               PrefetchSchema &schema) {}
};

}  // namespace hermes

#endif  // HERMES_SRC_PREFETCHERS_APRIORI_H_
