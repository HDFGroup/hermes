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

#ifndef HRUN_INCLUDE_HRUN_QUEUE_MANAGER_QUEUE_H_
#define HRUN_INCLUDE_HRUN_QUEUE_MANAGER_QUEUE_H_

#include "hrun/hrun_types.h"
#include "hrun/task_registry/task.h"
#include <vector>

/** Requests in this queue can be processed in any order */
#define QUEUE_READY BIT_OPT(u32, 0)
/** This queue contains only latency-sensitive tasks */
#define QUEUE_LOW_LATENCY BIT_OPT(u32, 1)
/** This queue is currently being resized */
#define QUEUE_RESIZE BIT_OPT(u32, 2)
/** This queue is currently processing updates */
#define QUEUE_UPDATE BIT_OPT(u32, 3)
/** Requests in this queue can be processed in any order */
#define QUEUE_UNORDERED BIT_OPT(u32, 4)
/** Requests in this queue are long-running */
#define QUEUE_LONG_RUNNING BIT_OPT(u32, 5)
/** Requests in this queue should not be scheduled on a traditional worker */
#define QUEUE_DISABLED BIT_OPT(u32, 6)
/** This queue is tethered to another queue */
#define QUEUE_TETHERED BIT_OPT(u32, 7)

namespace hrun {

/** Prioritization info needed to be set by client */
struct PriorityInfo {
  u32 prio_;            /**< Priority ID */
  u32 max_lanes_;       /**< Maximum number of lanes in the queue */
  u32 num_lanes_;       /**< Current number of lanes in use */
  u32 depth_;           /**< The maximum depth of individual lanes */
  bitfield32_t flags_;  /**< Scheduling hints for the queue */
  u32 tether_;          /**< Lanes should be pinned to the same workers as the tether */

  /** Default constructor */
  PriorityInfo() = default;

  /** Emplace constructor */
  PriorityInfo(u32 prio, u32 num_lanes, u32 max_lanes,
               u32 depth, u32 flags, u32 tether = 0) {
    prio_ = prio;
    max_lanes_ = max_lanes;
    num_lanes_ = num_lanes;
    depth_ = depth;
    flags_ = bitfield32_t(flags);
    tether_ = tether;
  }

  /** Emplace constructor */
  PriorityInfo(u32 prio, u32 num_lanes, u32 max_lanes, u32 depth,
               bitfield32_t flags, u32 tether = 0) {
    prio_ = prio;
    max_lanes_ = max_lanes;
    num_lanes_ = num_lanes;
    depth_ = depth;
    flags_ = flags;
    tether_ = tether;
  }

  /** Copy constructor */
  PriorityInfo(const PriorityInfo &priority) {
    prio_ = priority.prio_;
    max_lanes_ = priority.max_lanes_;
    num_lanes_ = priority.num_lanes_;
    depth_ = priority.depth_;
    flags_ = priority.flags_;
    tether_ = priority.tether_;
  }

  /** Move constructor */
  PriorityInfo(PriorityInfo &&priority) noexcept {
    prio_ = priority.prio_;
    max_lanes_ = priority.max_lanes_;
    num_lanes_ = priority.num_lanes_;
    depth_ = priority.depth_;
    flags_ = priority.flags_;
    tether_ = priority.tether_;
  }

  /** Copy assignment operator */
  PriorityInfo& operator=(const PriorityInfo &priority) {
    if (this != &priority) {
      prio_ = priority.prio_;
      max_lanes_ = priority.max_lanes_;
      num_lanes_ = priority.num_lanes_;
      depth_ = priority.depth_;
      flags_ = priority.flags_;
      tether_ = priority.tether_;
    }
    return *this;
  }

  /** Move assignment operator */
  PriorityInfo& operator=(PriorityInfo &&priority) noexcept {
    if (this != &priority) {
      prio_ = priority.prio_;
      max_lanes_ = priority.max_lanes_;
      num_lanes_ = priority.num_lanes_;
      depth_ = priority.depth_;
      flags_ = priority.flags_;
      tether_ = priority.tether_;
    }
    return *this;
  }

  /** Serialize Priority Info */
  template<typename Ar>
  void serialize(Ar &ar) {
    ar & prio_;
    ar & max_lanes_;
    ar & num_lanes_;
    ar & depth_;
    ar & flags_;
    ar & tether_;
  }
};

/** Forward declaration of the queue type */
template <typename QueueT>
class MultiQueueT;

}  // namespace hrun

#endif  // HRUN_INCLUDE_HRUN_QUEUE_MANAGER_QUEUE_H_
