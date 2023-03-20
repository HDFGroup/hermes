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
static std::vector<std::pair<TargetId, size_t>>::iterator
FindUniqueTarget(std::vector<std::pair<TargetId, size_t>> &unique_tgts,
                 TargetId &tid) {
  for (auto iter = unique_tgts.begin(); iter != unique_tgts.end(); ++iter) {
    if (iter->first == tid) {
      return iter;
    }
  }
  return unique_tgts.end();
}

/** Get the unique set of targets */
static std::vector<std::pair<TargetId, size_t>>
GroupByTarget(std::vector<BufferInfo> &buffers, size_t &total_size) {
  total_size = 0;
  std::vector<std::pair<TargetId, size_t>> unique_tgts;
  for (BufferInfo &info : buffers) {
    auto iter = FindUniqueTarget(unique_tgts, info.tid_);
    if (iter == unique_tgts.end()) {
      unique_tgts.emplace_back(info.tid_, info.blob_size_);
    } else {
      (*iter).second += info.blob_size_;
    }
    total_size += info.blob_size_;
  }
  return unique_tgts;
}

/** Get the unique set of targets */
static std::vector<std::pair<TargetId, size_t>>
GroupByTarget(PlacementSchema &schema, size_t &total_size) {
  total_size = 0;
  std::vector<std::pair<TargetId, size_t>> unique_tgts;
  for (auto &plcmnt : schema.plcmnts_) {
    auto iter = FindUniqueTarget(unique_tgts, plcmnt.tid_);
    if (iter == unique_tgts.end()) {
      unique_tgts.emplace_back(plcmnt.tid_, plcmnt.size_);
    } else {
      (*iter).second += plcmnt.size_;
    }
    total_size += plcmnt.size_;
  }
  return unique_tgts;
}

/**
 * The shared-memory representation of the BufferPool
 * */
template<>
struct ShmHeader<BufferPool> {
  SHM_CONTAINER_HEADER_TEMPLATE(ShmHeader)
  hipc::ShmArchive<BpTargetAllocs> free_lists_;
  size_t ntargets_;
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
  size_t GetTargetFreeList(u16 target) {
    return target * ncpu_ * nslabs_;
  }

  /**
   * Get the start of the vector of the free list for the CPU in the target
   * This is where page_size_, cur_count_, and max_count_ are valid.
   * */
  size_t GetCpuFreeList(int target, int cpu) {
    return target * ncpu_ * nslabs_ + cpu * nslabs_;
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
   * BufferPool Methods
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
   * Returns the number of each page size to allocate
   * */
  std::vector<BpCoin> CoinSelect(hipc::Ref<DeviceInfo> &dev_info,
                                 size_t total_size,
                                 size_t &buffer_count,
                                 size_t &total_alloced_size);

  /**
   * Allocate each size of buffer from either the free list or the
   * device growth allocator
   * */
  BpSlot AllocateSlabSize(BpCoin &coin,
                          hipc::Ref<BpFreeListStat> &stat,
                          hipc::Ref<BpFreeList> &free_list,
                          hipc::Ref<BpFreeListStat> &target_stat);

  /**
   * Allocate each size of buffer from either the free list or the
   * device growth allocator
   * */
  void AllocateBuffers(SubPlacement &plcmnt,
                       std::vector<BufferInfo> &buffers,
                       std::vector<BpCoin> &coins,
                       u16 target_id,
                       size_t num_slabs,
                       size_t free_list_start,
                       size_t &blob_off);

  /**
   * Free buffers from the BufferPool
   * */
  RPC bool LocalReleaseBuffers(std::vector<BufferInfo> &buffers);
  bool GlobalReleaseBuffers(std::vector<BufferInfo> &buffers);
};

}  // namespace hermes

#endif  // HERMES_SRC_BUFFER_POOL_H_
