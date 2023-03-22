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

#ifndef HERMES_SRC_BUFFER_POOL_H_
#define HERMES_SRC_BUFFER_POOL_H_

#include "hermes_types.h"
#include "rpc.h"

namespace hermes {

class MetadataManager;
class BufferOrganizer;
class BufferPool;

struct BpCoin {
  size_t count_;
  size_t slab_size_;

  BpCoin() : count_(0), slab_size_(0) {}
};

struct BpSlot {
  size_t t_off_;    /**< Offset of the buffer in the target */
  size_t t_size_;   /**< Size of the buffer in the target*/

  BpSlot() : t_size_(0) {}

  BpSlot(size_t t_off, size_t t_size) : t_off_(t_off), t_size_(t_size) {}

  bool IsNull() {
    return t_size_ == 0;
  }
};

struct BpFreeListStat {
  std::atomic<size_t> region_off_;  /**< Current offset in the target */
  std::atomic<size_t> region_size_; /**< Current space remaining in the tgt */
  size_t page_size_;  /**< The size of page in this buffer list */
  size_t cur_count_;  /**< Current number of pages allocated */
  size_t max_count_;  /**< Max pages allocated at one time */
  Mutex lock_;        /**< The modifier lock for this slot */

  /** Default constructor */
  BpFreeListStat() = default;

  /** Copy constructor */
  BpFreeListStat(const BpFreeListStat &other) {
    strong_copy(other);
  }

  /** Copy assignment operator */
  BpFreeListStat& operator=(const BpFreeListStat &other) {
    strong_copy(other);
    return *this;
  }

  /** Move constructor */
  BpFreeListStat(BpFreeListStat &&other) {
    strong_copy(other);
  }

  /** Move assignment operator */
  BpFreeListStat& operator=(BpFreeListStat &&other) {
    strong_copy(other);
    return *this;
  }

  /** Internal copy */
  void strong_copy(const BpFreeListStat &other) {
    region_off_ = other.region_off_.load();
    region_size_ = other.region_size_.load();
    page_size_ = other.page_size_;
    cur_count_ = other.cur_count_;
    max_count_ = other.max_count_;
  }
};

/** Represents the list of available buffers */
typedef hipc::slist<BpSlot> BpFreeList;

/** Represents a cache of buffer size in the target */
typedef hipc::pair<BpFreeListStat, BpFreeList> BpFreeListPair;

/** Represents the set of targets */
typedef hipc::vector<BpFreeListPair> BpTargetAllocs;

/** Find instance of unique target if it exists */
static std::vector<std::pair<i32, size_t>>::iterator
FindUniqueNodeId(std::vector<std::pair<i32, size_t>> &unique_nodes,
                 i32 node_id) {
  for (auto iter = unique_nodes.begin(); iter != unique_nodes.end(); ++iter) {
    if (iter->first == node_id) {
      return iter;
    }
  }
  return unique_nodes.end();
}

/** Get the unique set of targets */
static std::vector<std::pair<i32, size_t>>
GroupByNodeId(std::vector<BufferInfo> &buffers, size_t &total_size) {
  total_size = 0;
  std::vector<std::pair<i32, size_t>> unique_nodes;
  for (BufferInfo &info : buffers) {
    auto iter = FindUniqueNodeId(unique_nodes, info.tid_.GetNodeId());
    if (iter == unique_nodes.end()) {
      unique_nodes.emplace_back(info.tid_.GetNodeId(), info.blob_size_);
    } else {
      (*iter).second += info.blob_size_;
    }
    total_size += info.blob_size_;
  }
  return unique_nodes;
}

/** Get the unique set of targets */
static std::vector<std::pair<i32, size_t>>
GroupByNodeId(PlacementSchema &schema, size_t &total_size) {
  total_size = 0;
  std::vector<std::pair<i32, size_t>> unique_nodes;
  for (auto &plcmnt : schema.plcmnts_) {
    auto iter = FindUniqueNodeId(unique_nodes, plcmnt.tid_.GetNodeId());
    if (iter == unique_nodes.end()) {
      unique_nodes.emplace_back(plcmnt.tid_.GetNodeId(), plcmnt.size_);
    } else {
      (*iter).second += plcmnt.size_;
    }
    total_size += plcmnt.size_;
  }
  return unique_nodes;
}

/**
 * The shared-memory representation of the BufferPool
 * */
template<>
struct ShmHeader<BufferPool> {
  SHM_CONTAINER_HEADER_TEMPLATE(ShmHeader)
  hipc::ShmArchive<BpTargetAllocs> free_lists_;
  u16 ntargets_;
  size_t ncpu_;
  size_t nslabs_;

  /**
   * Copy one header into another
   * */
  void strong_copy(const ShmHeader &other) {
    ntargets_ = other.ntargets_;
    ncpu_ = other.ncpu_;
    nslabs_ = other.nslabs_;
  }

  /**
   * Get the free list of the target
   * This is where region_off_ & region_size_ in the BpFreeListStat are valid
   * */
  size_t GetCpuTargetStat(u16 target, int cpu) {
    return target * ncpu_ * nslabs_ + cpu * nslabs_;
  }

  /**
   * Get the start of the vector of the free list for the CPU in the target
   * This is where page_size_, cur_count_, and max_count_ are valid.
   *
   * [target] [cpu] [slab_id]
   * */
  size_t GetCpuFreeList(u16 target, int cpu, int slab_id) {
    return target * ncpu_ * nslabs_ + cpu * nslabs_ + slab_id;
  }
};

/**
 * Responsible for managing the buffering space of all node-local targets.
 * */
class BufferPool : public hipc::ShmContainer {
 public:
  SHM_CONTAINER_TEMPLATE((BufferPool), (BufferPool), (ShmHeader<BufferPool>))

 private:
  MetadataManager *mdm_;
  BufferOrganizer *borg_;
  RPC_TYPE *rpc_;
  /** Per-target allocator */
  hipc::Ref<BpTargetAllocs> target_allocs_;

 public:
  /**====================================
   * Default Constructor
   * ===================================*/

  /**
   * Initialize the BPM and its shared memory.
   * REQUIRES mdm to be initialized already.
   * */
  explicit BufferPool(ShmHeader<BufferPool> *header, hipc::Allocator *alloc);

  /**====================================
   * Destructor
   * ===================================*/

  /** Whether the BufferPool is NULL */
  bool IsNull() { return false; }

  /** Set to NULL */
  void SetNull() {}

  /** Destroy the BPM shared memory. */
  void shm_destroy_main();

  /**====================================
   * SHM Deserialize
   * ===================================*/

  /** Deserialize the BPM from shared memory */
  void shm_deserialize_main();

  /**====================================
   * Allocate Buffers
   * ===================================*/

  /**
   * Allocate buffers from the targets according to the schema
   * */
  RPC std::vector<BufferInfo>
  LocalAllocateAndSetBuffers(PlacementSchema &schema,
                             const Blob &blob);
  std::vector<BufferInfo>
  GlobalAllocateAndSetBuffers(PlacementSchema &schema,
                              const Blob &blob);

  /**
   * Determines a reasonable allocation of buffers based on the size of I/O.
   * Returns the number of each slab size to allocate
   * */
  std::vector<BpCoin> CoinSelect(hipc::Ref<DeviceInfo> &dev_info,
                                 size_t total_size,
                                 size_t &buffer_count);

  /**
   * Allocate requested slabs from this target.
   * If the target runs out of space, it will provision from the next target.
   * We assume there is a baseline target, which can house an infinite amount
   * of data. This would be the PFS in an HPC machine.
   *
   * @param total_size the total amount of data being placed in this target
   * @param coins The requested number of slabs to allocate from this target
   * @param tid The ID of the (ideal) target to allocate from
   * @param cpu The CPU we are currently scheduled on
   * @param target_stat The metadata for the target on this CPU
   * @param blob_off [out] The current size of the blob which has been placed
   * @param buffers [out] The buffers which were allocated
   * */
  void AllocateBuffers(size_t total_size,
                       std::vector<BpCoin> &coins,
                       TargetId tid,
                       int cpu,
                       hipc::Ref<BpFreeListStat> &target_stat,
                       size_t &blob_off,
                       std::vector<BufferInfo> &buffers);

  /**
   * Allocate a set of slabs of particular size from this target.
   *
   * @param rem_size the amount of data remaining that needs to be allocated
   * @param slab_size Size of the slabs to allocate
   * @param slab_count The count of this slab size to allocate
   * @param slab_id The offset of the slab in the device's slab list
   * @param tid The target to allocate slabs from
   * @param free_list The CPU free list for this slab size
   * @param free_list_stat The metadata for the free list
   * @param target_stat The metadata for the CPU core
   * @param blob_off [out] The current size of the blob which has been placed
   * @param buffers [out] The buffers which were allocated
   * */
  void AllocateSlabs(size_t &rem_size,
                     size_t slab_size,
                     size_t &slab_count,
                     size_t slab_id,
                     TargetId tid,
                     hipc::Ref<BpFreeList> &free_list,
                     hipc::Ref<BpFreeListStat> &free_list_stat,
                     hipc::Ref<BpFreeListStat> &target_stat,
                     size_t &blob_off,
                     std::vector<BufferInfo> &buffers);

  /**
   * Allocate a buffer of a particular size
   *
   * @param slab_id The size of slab to allocate
   * @param tid The target to (ideally) allocate the slab from
   * @param coin The buffer size information
   *
   * @return returns a BufferPool (BP) slot. The slot is NULL if the
   * target didn't have enough remaining space.
   * */
  BpSlot AllocateSlabSize(size_t slab_size,
                          hipc::Ref<BpFreeList> &free_list,
                          hipc::Ref<BpFreeListStat> &stat,
                          hipc::Ref<BpFreeListStat> &target_stat);


  /**====================================
   * Free Buffers
   * ===================================*/

  /**
   * Free buffers from the BufferPool
   * */
  RPC bool LocalReleaseBuffers(std::vector<BufferInfo> &buffers);
  bool GlobalReleaseBuffers(std::vector<BufferInfo> &buffers);

  /**====================================
   * Helper Methods
   * ===================================*/

  /** Get a free list reference */
  void GetFreeListForCpu(u16 target_id, int cpu, int slab_id,
                         hipc::Ref<BpFreeList> &free_list,
                         hipc::Ref<BpFreeListStat> &free_list_stat);

  /** Get the stack allocator from the cpu */
  void GetTargetStatForCpu(u16 target_id, int cpu,
                           hipc::Ref<BpFreeListStat> &target_stat);
};

}  // namespace hermes

#endif  // HERMES_SRC_BUFFER_POOL_H_
