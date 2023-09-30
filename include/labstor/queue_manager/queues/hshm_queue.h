//
// Created by llogan on 7/1/23.
//

#ifndef LABSTOR_INCLUDE_LABSTOR_QUEUE_MANAGER_HSHM_QUEUE_H_
#define LABSTOR_INCLUDE_LABSTOR_QUEUE_MANAGER_HSHM_QUEUE_H_

#include "labstor/queue_manager/queue.h"

namespace labstor {

/** The data stored in a lane */
struct LaneData {
  hipc::Pointer p_;
  bool complete_;
  ABT_thread thread_;

  LaneData() = default;

  LaneData(hipc::Pointer &p, bool complete) {
    p_ = p;
    complete_ = complete;
  }
};

/** Represents a lane tasks can be stored */
typedef hipc::mpsc_queue<LaneData> Lane;

/** Prioritization of different lanes in the queue */
struct LaneGroup : public PriorityInfo {
  u32 prio_;            /**< The priority of the lane group */
  u32 num_scheduled_;   /**< The number of lanes currently scheduled on workers */
  hipc::ShmArchive<hipc::vector<Lane>> lanes_;  /**< The lanes of the queue */

  /** Default constructor */
  HSHM_ALWAYS_INLINE
  LaneGroup() = default;

  /** Set priority info */
  HSHM_ALWAYS_INLINE
  LaneGroup(const PriorityInfo &priority) {
    max_lanes_ = priority.max_lanes_;
    num_lanes_ = priority.num_lanes_;
    num_scheduled_ = 0;
    depth_ = priority.depth_;
    flags_ = priority.flags_;
    // prio_ is set externally
  }

  /** Copy constructor. Should never actually be called. */
  HSHM_ALWAYS_INLINE
  LaneGroup(const LaneGroup &priority) {
    max_lanes_ = priority.max_lanes_;
    num_lanes_ = priority.num_lanes_;
    num_scheduled_ = priority.num_scheduled_;
    depth_ = priority.depth_;
    flags_ = priority.flags_;
    // prio_ is set externally
  }

  /** Move constructor. Should never actually be called. */
  HSHM_ALWAYS_INLINE
  LaneGroup(LaneGroup &&priority) noexcept {
    max_lanes_ = priority.max_lanes_;
    num_lanes_ = priority.num_lanes_;
    num_scheduled_ = priority.num_scheduled_;
    depth_ = priority.depth_;
    flags_ = priority.flags_;
    // prio_ is set externally
  }

  /** Check if this group is long-running or ADMIN */
  HSHM_ALWAYS_INLINE
  bool IsLowPriority() {
    return flags_.Any(QUEUE_LONG_RUNNING) || prio_ == 0;
  }
};

/** Represents the HSHM queue type */
class Hshm {};

/**
 * The shared-memory representation of a Queue
 * */
template<>
struct MultiQueueT<Hshm> : public hipc::ShmContainer {
  SHM_CONTAINER_TEMPLATE((MultiQueueT), (MultiQueueT))
  QueueId id_;          /**< Globally unique ID of this queue */
  hipc::ShmArchive<hipc::vector<LaneGroup>> groups_;  /**< Divide the lanes into groups */
  bitfield32_t flags_;  /**< Flags for the queue */

 public:
  /**====================================
   * Constructor
   * ===================================*/

  /** SHM constructor. Default. */
  explicit MultiQueueT(hipc::Allocator *alloc) {
    shm_init_container(alloc);
    SetNull();
  }

  /** SHM constructor. */
  explicit MultiQueueT(hipc::Allocator *alloc, const QueueId &id,
                       const std::vector<PriorityInfo> &prios) {
    shm_init_container(alloc);
    id_ = id;
    HSHM_MAKE_AR0(groups_, GetAllocator());
    groups_->reserve(prios.size());
    for (u32 prio = 0; prio < prios.size(); ++prio) {
      const PriorityInfo &prio_info = prios[prio];
      groups_->emplace_back(prio_info);
      LaneGroup &lane_group = (*groups_)[prio];
      // Initialize lanes
      HSHM_MAKE_AR0(lane_group.lanes_, GetAllocator());
      lane_group.lanes_->reserve(prio_info.max_lanes_);
      lane_group.prio_ = prio;
      for (u32 lane_id = 0; lane_id < lane_group.num_lanes_; ++lane_id) {
        lane_group.lanes_->emplace_back(lane_group.depth_);
        Lane &lane = lane_group.lanes_->back();
        lane.flags_ = prio_info.flags_;
      }
    }
    SetNull();
  }

  /**====================================
   * Copy Constructors
   * ===================================*/

  /** SHM copy constructor */
  explicit MultiQueueT(hipc::Allocator *alloc, const MultiQueueT &other) {
    shm_init_container(alloc);
    SetNull();
    shm_strong_copy_construct_and_op(other);
  }

  /** SHM copy assignment operator */
  MultiQueueT& operator=(const MultiQueueT &other) {
    if (this != &other) {
      shm_destroy();
      shm_strong_copy_construct_and_op(other);
    }
    return *this;
  }

  /** SHM copy constructor + operator main */
  void shm_strong_copy_construct_and_op(const MultiQueueT &other) {
    (*groups_) = (*other.groups_);
  }

  /**====================================
   * Move Constructors
   * ===================================*/

  /** SHM move constructor. */
  MultiQueueT(hipc::Allocator *alloc,
              MultiQueueT &&other) noexcept {
    shm_init_container(alloc);
    if (GetAllocator() == other.GetAllocator()) {
      (*groups_) = std::move(*other.groups_);
      other.SetNull();
    } else {
      shm_strong_copy_construct_and_op(other);
      other.shm_destroy();
    }
  }

  /** SHM move assignment operator. */
  MultiQueueT& operator=(MultiQueueT &&other) noexcept {
    if (this != &other) {
      shm_destroy();
      if (GetAllocator() == other.GetAllocator()) {
        (*groups_) = std::move(*other.groups_);
        other.SetNull();
      } else {
        shm_strong_copy_construct_and_op(other);
        other.shm_destroy();
      }
    }
    return *this;
  }

  /**====================================
   * Destructor
   * ===================================*/

  /** SHM destructor.  */
  void shm_destroy_main() {
    (*groups_).shm_destroy();
  }

  /** Check if the list is empty */
  HSHM_ALWAYS_INLINE
  bool IsNull() const {
    return (*groups_).IsNull();
  }

  /** Sets this list as empty */
  HSHM_ALWAYS_INLINE
  void SetNull() {}

  /**====================================
   * Helpers
   * ===================================*/

  /** Get the priority struct */
  HSHM_ALWAYS_INLINE LaneGroup& GetGroup(u32 prio) {
    return (*groups_)[prio];
  }

  /** Get a lane of the queue */
  HSHM_ALWAYS_INLINE Lane& GetLane(u32 prio, u32 lane_id) {
    return (*(*groups_)[prio].lanes_)[lane_id];
  }

  /** Get a lane of the queue */
  HSHM_ALWAYS_INLINE Lane& GetLane(LaneGroup &lane_group, u32 lane_id) {
    return (*lane_group.lanes_)[lane_id];
  }

  /** Emplace a SHM pointer to a task */
  HSHM_ALWAYS_INLINE
  bool Emplace(u32 prio, u32 lane_hash, hipc::Pointer &p, bool complete = false) {
    return Emplace(prio, lane_hash, LaneData(p, complete));
  }

  /** Emplace a SHM pointer to a task */
  bool Emplace(u32 prio, u32 lane_hash, const LaneData &data) {
    if (IsEmplacePlugged()) {
      WaitForEmplacePlug();
    }
    LaneGroup &lane_group = GetGroup(prio);
    u32 lane_id = lane_hash % lane_group.num_lanes_;
    Lane &lane = GetLane(lane_group, lane_id);
    hshm::qtok_t ret = lane.emplace(data);
    return !ret.IsNull();
  }

  /**
   * Change the number of active lanes
   * This assumes that PlugForResize and UnplugForResize are called externally.
   * */
  void Resize(u32 num_lanes) {
  }

  /** Begin plugging the queue for resize */
  HSHM_ALWAYS_INLINE bool PlugForResize() {
    return true;
  }

  /** Begin plugging the queue for update tasks */
  HSHM_ALWAYS_INLINE bool PlugForUpdateTask() {
    return true;
  }

  /** Check if emplace operations are plugged */
  HSHM_ALWAYS_INLINE bool IsEmplacePlugged() {
    return flags_.Any(QUEUE_RESIZE);
  }

  /** Check if pop operations are plugged */
  HSHM_ALWAYS_INLINE bool IsPopPlugged() {
    return flags_.Any(QUEUE_UPDATE | QUEUE_RESIZE);
  }

  /** Wait for emplace plug to complete */
  void WaitForEmplacePlug() {
    // NOTE(llogan): will this infinite loop due to CPU caching?
    while (flags_.Any(QUEUE_UPDATE)) {
      HERMES_THREAD_MODEL->Yield();
    }
  }

  /** Enable emplace & pop */
  HSHM_ALWAYS_INLINE void UnplugForResize() {
    flags_.UnsetBits(QUEUE_RESIZE);
  }

  /** Enable pop */
  HSHM_ALWAYS_INLINE void UnplugForUpdateTask() {
    flags_.UnsetBits(QUEUE_UPDATE);
  }
};

}  // namespace labstor


#endif  // LABSTOR_INCLUDE_LABSTOR_QUEUE_MANAGER_HSHM_QUEUE_H_
