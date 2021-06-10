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
  k1,
  k2,

  kCount
};

enum class BoPriority {
  kHigh,
  kLow,

  kCount
};

struct BoArgs {
};

struct BoTask {
  BoOperation op;
  BoArgs args;
};

struct BufferOrganizer {
};

bool LocalEnqueueBoTask(SharedMemoryContext *context, BoTask task,
                        BoPriority priority);

}  // namespace hermes

#endif  // HERMES_BUFFER_ORGANIZER_H_
