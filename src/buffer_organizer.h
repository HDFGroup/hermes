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

/** move list for buffer organizer */
using BoMoveList = std::vector<std::pair<BufferID, std::vector<BufferID>>>;

/** buffer organizer operations */
enum class BoOperation {
  kMove,
  kCopy,
  kDelete,

  kCount
};

/** buffer organizer priorities */
enum class BoPriority {
  kLow,
  kHigh,

  kCount
};

/**
 A union of structures to represent buffer organizer arguments
*/
union BoArgs {
  /** A structure to represent move arguments */
  struct {
    BufferID src;  /**< source buffer ID */
    TargetID dest; /**< destination target ID */
  } move_args;
  /** A structure to represent copy arguments */
  struct {
    BufferID src;  /**< source buffer ID */
    TargetID dest; /**< destination target ID */
  } copy_args;
  /** A structure to represent delete arguments */
  struct {
    BufferID src; /**< source buffer ID */
  } delete_args;
};

/**
   A structure to represent buffer organizer task
*/
struct BoTask {
  BoOperation op; /**< buffer organizer operation */
  BoArgs args;    /**< buffer organizer arguments */
};

/**
   A structure to represent buffer information
*/
struct BufferInfo {
  BufferID id;        /**< buffer ID */
  f32 bandwidth_mbps; /**< bandwidth in Megabits per second */
  size_t size;        /**< buffer size */
};

/** comparison operator */
bool operator==(const BufferInfo &lhs, const BufferInfo &rhs);

/**
   A structure to represent buffer organizer
*/
class BufferOrganizer {
 public:
  SharedMemoryContext *context_;
  RpcContext *rpc_;
  ThreadPool pool; /**< a pool of threads */

 public:
  /** initialize buffer organizer with \a num_threads number of threads.  */
  explicit BufferOrganizer(SharedMemoryContext *context,
                           RpcContext *rpc, int num_threads);


};

}  // namespace hermes

#endif  // HERMES_BUFFER_ORGANIZER_H_
