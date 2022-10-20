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

#ifndef HERMES_SRC_PREFETCHERS_SEQUENTIAL_H_
#define HERMES_SRC_PREFETCHERS_SEQUENTIAL_H_

#include "prefetcher.h"

namespace hermes {

struct SequentialState {
  struct timespec first_access_;
  u32 count_;

  SequentialState() : count_(0) {}
};

class SequentialPrefetcher : public PrefetchAlgorithm {
 private:
  std::unordered_map<UniqueBucket, SequentialState> state_;

 public:
  void Process(std::list<IoLogEntry> &log,
               PrefetchSchema &schema);

 private:
  void GetNextBucket(IoLogEntry &entry,
                     PrefetchDecision &decision,
                     BlobID &cur_id);
  void GetNextVbucket(IoLogEntry &entry,
                      PrefetchDecision &decision,
                      BlobID &cur_id);
};

}  // namespace hermes

#endif  // HERMES_SRC_PREFETCHERS_SEQUENTIAL_H_
