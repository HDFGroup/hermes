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

#include "thread_pool.h"

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
  struct {
    BufferID src;
    TargetID dest;
  } move_args;
  struct {
    BufferID src;
    TargetID dest;
  } copy_args;
  struct {
    BufferID src;
  } delete_args;
};

struct BoTask {
  BoOperation op;
  BoArgs args;
};

const int kNumPools = 2;
const int kNumXstreams = 2;

struct BufferOrganizer {
  ThreadPool pool;
};

bool LocalEnqueueBoTask(SharedMemoryContext *context, BoTask task,
                        BoPriority priority);

void BoMove(BoArgs *args);
void BoCopy(BoArgs *args);
void BoDelete(BoArgs *args);

}  // namespace hermes

#endif  // HERMES_BUFFER_ORGANIZER_H_
