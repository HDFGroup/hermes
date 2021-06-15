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

#ifndef HERMES_BUFFER_ORGANIZER_H_
#define HERMES_BUFFER_ORGANIZER_H_

namespace hermes {

enum class BoOperation {
  kMove,
  kCopy,
  kDelete,

  kCount
};

enum class BoPriority {
  kLow,
  kHigh,

  kCount
};

union BoArgs {
  struct {} move_args;
  struct {} copy_args;
  struct {} delete_args;
};

struct BoTask {
  BoOperation op;
  BoArgs args;
};

const int kNumPools = 2;
const int kNumXstreams = 2;

struct BufferOrganizer {
  ABT_xstream xstreams[kNumXstreams];
  ABT_sched scheds[kNumXstreams];
  ABT_pool pools[kNumPools];
  ABT_thread threads[kNumXstreams];
};

bool LocalEnqueueBoTask(SharedMemoryContext *context, BoTask task,
                        BoPriority priority);

}  // namespace hermes

#endif  // HERMES_BUFFER_ORGANIZER_H_
