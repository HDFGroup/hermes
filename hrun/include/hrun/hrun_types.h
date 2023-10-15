//
// Created by lukemartinlogan on 6/22/23.
//

#ifndef HRUN_INCLUDE_HRUN_HRUN_TYPES_H_
#define HRUN_INCLUDE_HRUN_HRUN_TYPES_H_

#include <hermes_shm/data_structures/ipc/unordered_map.h>
#include <hermes_shm/data_structures/ipc/pod_array.h>
#include <hermes_shm/data_structures/ipc/vector.h>
#include <hermes_shm/data_structures/ipc/list.h>
#include <hermes_shm/data_structures/ipc/slist.h>
#include <hermes_shm/data_structures/data_structure.h>
#include <hermes_shm/data_structures/ipc/string.h>
#include <hermes_shm/data_structures/ipc/mpsc_queue.h>
#include <hermes_shm/data_structures/ipc/mpsc_ptr_queue.h>
#include <hermes_shm/data_structures/ipc/ticket_queue.h>
#include <hermes_shm/data_structures/containers/converters.h>
#include <hermes_shm/data_structures/containers/charbuf.h>
#include <hermes_shm/data_structures/containers/spsc_queue.h>
#include <hermes_shm/data_structures/containers/split_ticket_queue.h>
#include <hermes_shm/data_structures/containers/converters.h>
#include "hermes_shm/data_structures/serialization/shm_serialize.h"
#include <hermes_shm/util/auto_trace.h>
#include <hermes_shm/thread/lock.h>
#include <hermes_shm/thread/thread_model_manager.h>
#include <hermes_shm/types/atomic.h>
#include "hermes_shm/util/singleton.h"
#include "hermes_shm/constants/macros.h"

#include <boost/context/fiber_fcontext.hpp>

namespace bctx = boost::context::detail;

typedef uint8_t u8;   /**< 8-bit unsigned integer */
typedef uint16_t u16; /**< 16-bit unsigned integer */
typedef uint32_t u32; /**< 32-bit unsigned integer */
typedef uint64_t u64; /**< 64-bit unsigned integer */
typedef int8_t i8;    /**< 8-bit signed integer */
typedef int16_t i16;  /**< 16-bit signed integer */
typedef int32_t i32;  /**< 32-bit signed integer */
typedef int64_t i64;  /**< 64-bit signed integer */
typedef float f32;    /**< 32-bit float */
typedef double f64;   /**< 64-bit float */

namespace hrun {

using hshm::RwLock;
using hshm::Mutex;
using hshm::bitfield;
using hshm::bitfield32_t;
typedef hshm::bitfield<uint64_t> bitfield64_t;
using hshm::ScopedRwReadLock;
using hshm::ScopedRwWriteLock;
using hipc::LPointer;

/** Determine the mode that HRUN is initialized for */
enum class HrunMode {
  kNone,
  kClient,
  kServer
};

#define DOMAIN_FLAG_T static inline const int

/**
 * Represents a unique ID for a scheduling domain
 * There are a few domain types:
 * 1. A node domain, which is a single node
 * 2. A global domain, which is all nodes
 * 3. A specific domain, which is a subset of nodes
 * 4. A specific domain + node, temporarily includes this node in a domain
 * */
struct DomainId {
  bitfield32_t flags_;  /**< Flags indicating how to interpret id */
  u32 id_;              /**< The domain id, 0 is NULL */
  DOMAIN_FLAG_T kLocal = BIT_OPT(u32, 0);   /**< Include local node in scheduling decision */
  DOMAIN_FLAG_T kGlobal = BIT_OPT(u32, 1);  /**< Use all nodes in scheduling decision */
  DOMAIN_FLAG_T kSet = BIT_OPT(u32, 2);     /**< ID represents node set ID, not a single node */
  DOMAIN_FLAG_T kNode = BIT_OPT(u32, 3);    /**< ID represents a specific node */

  /** Serialize domain id */
  template<typename Ar>
  void serialize(Ar &ar) {
    ar(flags_, id_);
  }

  /** Default constructor. */
  HSHM_ALWAYS_INLINE
  DomainId() : id_(0) {}

  /** Domain has the local node */
  HSHM_ALWAYS_INLINE
  bool IsRemote(size_t num_hosts, u32 this_node) const {
    if (num_hosts == 1) {
      return false;
    } else {
      return (flags_.Any(kGlobal | kSet) || (flags_.Any(kNode) && id_ != this_node));
    }
    // return flags_.Any(kGlobal | kSet | kNode);
  }

  /** DomainId representing the local node */
  HSHM_ALWAYS_INLINE
  static DomainId GetLocal() {
      DomainId id;
      id.id_ = 0;
      id.flags_.SetBits(kLocal);
      return id;
  }

  /** Get the ID */
  HSHM_ALWAYS_INLINE
  u32 GetId() const {
    return id_;
  }

  /** Domain is a specific node */
  HSHM_ALWAYS_INLINE
  bool IsNode() const {
    return flags_.Any(kNode);
  }

  /** DomainId representing a specific node */
  HSHM_ALWAYS_INLINE
  static DomainId GetNode(u32 node_id) {
    DomainId id;
    id.id_ = node_id;
    id.flags_.SetBits(kNode);
    return id;
  }

  /** DomainId representing a specific node + local node */
  HSHM_ALWAYS_INLINE
  static DomainId GetNodeWithLocal(u32 node_id) {
    DomainId id;
    id.id_ = node_id;
    id.flags_.SetBits(kLocal);
    return id;
  }

  /** Domain represents all nodes */
  HSHM_ALWAYS_INLINE
  bool IsGlobal() const {
    return flags_.Any(kGlobal);
  }

  /** DomainId representing all nodes */
  HSHM_ALWAYS_INLINE
  static DomainId GetGlobal() {
      DomainId id;
      id.id_ = 0;
      id.flags_.SetBits(kGlobal);
      return id;
  }

  /** DomainId represents a named node set */
  HSHM_ALWAYS_INLINE
  bool IsSet() const {
    return flags_.Any(kSet);
  }

  /** DomainId representing a named node set */
  HSHM_ALWAYS_INLINE
  static DomainId GetSet(u32 domain_id) {
    DomainId id;
    id.id_ = domain_id;
    id.flags_.SetBits(kSet);
    return id;
  }

  /** DomainId representing a node set + local node */
  HSHM_ALWAYS_INLINE
  static DomainId GetSetWithLocal(u32 domain_id) {
    DomainId id;
    id.id_ = domain_id;
    id.flags_.SetBits(kSet | kLocal);
    return id;
  }

  /** Copy constructor */
  HSHM_ALWAYS_INLINE
  DomainId(const DomainId &other) {
    id_ = other.id_;
    flags_ = other.flags_;
  }

  /** Copy operator */
  HSHM_ALWAYS_INLINE
  DomainId& operator=(const DomainId &other) {
    if (this != &other) {
      id_ = other.id_;
      flags_ = other.flags_;
    }
    return *this;
  }

  /** Move constructor */
  HSHM_ALWAYS_INLINE
  DomainId(DomainId &&other) noexcept {
    id_ = other.id_;
    flags_ = other.flags_;
  }

  /** Move operator */
  HSHM_ALWAYS_INLINE
  DomainId& operator=(DomainId &&other) noexcept {
    if (this != &other) {
      id_ = other.id_;
      flags_ = other.flags_;
    }
    return *this;
  }

  /** Equality operator */
  HSHM_ALWAYS_INLINE
  bool operator==(const DomainId &other) const {
    return id_ == other.id_ && flags_.bits_ == other.flags_.bits_;
  }

  /** Inequality operator */
  HSHM_ALWAYS_INLINE
  bool operator!=(const DomainId &other) const {
    return id_ != other.id_ || flags_.bits_ != other.flags_.bits_;
  }
};

/** Represents unique ID for states + queues */
template<int TYPE>
struct UniqueId {
  u32 node_id_;  /**< The node the content is on */
  u32 hash_;     /**< The hash of the content the ID represents */
  u64 unique_;   /**< A unique id for the blob */

  /** Serialization */
  template<typename Ar>
  void serialize(Ar &ar) {
    ar & node_id_;
    ar & hash_;
    ar & unique_;
  }

  /** Default constructor */
  HSHM_ALWAYS_INLINE
  UniqueId() = default;

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  UniqueId(u32 node_id, u64 unique)
  : node_id_(node_id), hash_(0), unique_(unique) {}

  /** Emplace constructor (+hash) */
  HSHM_ALWAYS_INLINE explicit
  UniqueId(u32 node_id, u32 hash, u64 unique)
  : node_id_(node_id), hash_(hash), unique_(unique) {}

  /** Copy constructor */
  HSHM_ALWAYS_INLINE
  UniqueId(const UniqueId &other) {
    node_id_ = other.node_id_;
    hash_ = other.hash_;
    unique_ = other.unique_;
  }

  /** Copy constructor */
  template<int OTHER_TYPE=TYPE>
  HSHM_ALWAYS_INLINE
  UniqueId(const UniqueId<OTHER_TYPE> &other) {
    node_id_ = other.node_id_;
    hash_ = other.hash_;
    unique_ = other.unique_;
  }

  /** Copy assignment */
  HSHM_ALWAYS_INLINE
  UniqueId& operator=(const UniqueId &other) {
    if (this != &other) {
      node_id_ = other.node_id_;
      hash_ = other.hash_;
      unique_ = other.unique_;
    }
    return *this;
  }

  /** Move constructor */
  HSHM_ALWAYS_INLINE
  UniqueId(UniqueId &&other) noexcept {
    node_id_ = other.node_id_;
    hash_ = other.hash_;
    unique_ = other.unique_;
  }

  /** Move assignment */
  HSHM_ALWAYS_INLINE
  UniqueId& operator=(UniqueId &&other) noexcept {
    if (this != &other) {
      node_id_ = other.node_id_;
      hash_ = other.hash_;
      unique_ = other.unique_;
    }
    return *this;
  }

  /** Check if null */
  [[nodiscard]]
  HSHM_ALWAYS_INLINE bool IsNull() const {
    return node_id_ == 0;
  }

  /** Get null id */
  HSHM_ALWAYS_INLINE
  static UniqueId GetNull() {
    static const UniqueId id(0, 0);
    return id;
  }

  /** Set to null id */
  HSHM_ALWAYS_INLINE
  void SetNull() {
    node_id_ = 0;
    hash_ = 0;
    unique_ = 0;
  }

  /** Get id of node from this id */
  [[nodiscard]]
  HSHM_ALWAYS_INLINE
  u32 GetNodeId() const { return node_id_; }

  /** Compare two ids for equality */
  HSHM_ALWAYS_INLINE
  bool operator==(const UniqueId &other) const {
    return unique_ == other.unique_ && node_id_ == other.node_id_;
  }

  /** Compare two ids for inequality */
  HSHM_ALWAYS_INLINE
  bool operator!=(const UniqueId &other) const {
    return unique_ != other.unique_ || node_id_ != other.node_id_;
  }
};

/** Uniquely identify a task state */
using TaskStateId = UniqueId<1>;
/** Uniquely identify a queue */
using QueueId = UniqueId<2>;
/** Uniquely identify a task */
using TaskId = UniqueId<3>;

/** Allow unique ids to be printed as strings */
template<int num>
std::ostream &operator<<(std::ostream &os, UniqueId<num> const &obj) {
  return os << (std::to_string(obj.node_id_) + "."
      + std::to_string(obj.unique_));
}

/** The types of I/O that can be performed (for IoCall RPC) */
enum class IoType {
  kRead,
  kWrite,
  kNone
};

}  // namespace hrun

namespace std {

/** Hash function for UniqueId */
template <int TYPE>
struct hash<hrun::UniqueId<TYPE>> {
  HSHM_ALWAYS_INLINE
  std::size_t operator()(const hrun::UniqueId<TYPE> &key) const {
    return
      std::hash<u64>{}(key.unique_) +
      std::hash<u32>{}(key.node_id_);
  }
};

/** Hash function for DomainId */
template<>
struct hash<hrun::DomainId> {
  HSHM_ALWAYS_INLINE
  std::size_t operator()(const hrun::DomainId &key) const {
    return
        std::hash<u32>{}(key.id_) +
        std::hash<u32>{}(key.flags_.bits_);
  }
};

}  // namespace std

#endif  // HRUN_INCLUDE_HRUN_HRUN_TYPES_H_
