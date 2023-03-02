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

  BpCoin() : count_(0) {}
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
};

/** Represents the list of available buffers */
typedef hipc::slist<BpSlot> BpFreeList;

/** Represents a cache of buffer size in the target */
typedef hipc::pair<BpFreeListStat, BpFreeList> BpFreeListPair;

/** Represents the set of targets */
typedef hipc::vector<BpFreeListPair> BpTargetAllocs;

/**
 * The shared-memory representation of the BufferPool
 * */
template<>
struct ShmHeader<BufferPool> : public hipc::ShmBaseHeader {
  hipc::ShmArchiveOrT<BpTargetAllocs> free_lists_;
  size_t ntargets_;
  size_t ncpu_;
  size_t nslabs_;

  /** Construct all internal objects */
  ShmHeader(hipc::Allocator *alloc) {
    free_lists_.shm_init(alloc);
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
  SHM_CONTAINER_TEMPLATE((BufferPool), (BufferPool), (ShmHeader<BufferPool>))
 private:
  MetadataManager *mdm_;
  BufferOrganizer *borg_;
  /** Per-target allocator */
  hipc::ShmRef<BpTargetAllocs> target_allocs_;

 public:
  BufferPool() = default;

  /**
   * Initialize the BPM and its shared memory.
   * REQUIRES mdm to be initialized already.
   * */
  void shm_init_main(ShmHeader<BufferPool> *header,
                     hipc::Allocator *alloc);

  /** Destroy the BPM shared memory. */
  void shm_destroy_main();

  /** Store the BPM in shared memory */
  void shm_serialize_main() const;

  /** Deserialize the BPM from shared memory */
  void shm_deserialize_main();

  /** Weak move */
  void shm_weak_move_main(ShmHeader<BufferPool> *header,
                          hipc::Allocator *alloc,
                          BufferPool &other) {}

  /** Strong copy */
  void shm_strong_copy_main(ShmHeader<BufferPool> *header,
                            hipc::Allocator *alloc,
                            const BufferPool &other) {}

  /**
   * Allocate buffers from the targets according to the schema
   * */
  RPC hipc::vector<BufferInfo>
  LocalAllocateAndSetBuffers(PlacementSchema &schema,
                             const Blob &blob);

  /**
   * Determines a reasonable allocation of buffers based on the size of I/O.
   * Returns the number of each page size to allocate
   * */
  std::vector<BpCoin> CoinSelect(hipc::ShmRef<DeviceInfo> &dev_info,
                                 size_t total_size,
                                 size_t &buffer_count,
                                 size_t &total_alloced_size);

  /**
   * Allocate each size of buffer from either the free list or the
   * device growth allocator
   * */
  BpSlot AllocateSlabSize(BpCoin &coin,
                          hipc::ShmRef<BpFreeListStat> &stat,
                          hipc::ShmRef<BpFreeList> &free_list,
                          hipc::ShmRef<BpFreeListStat> &target_stat);

  /**
   * Allocate each size of buffer from either the free list or the
   * device growth allocator
   * */
  void AllocateBuffers(SubPlacement &plcmnt,
                       hipc::vector<BufferInfo> &buffers,
                       std::vector<BpCoin> &coins,
                       u16 target_id,
                       size_t num_slabs,
                       size_t free_list_start,
                       size_t &blob_off);

  /**
   * Free buffers from the BufferPool
   * */
  RPC bool LocalReleaseBuffers(hipc::vector<BufferInfo> &buffers);
};

}  // namespace hermes

#endif  // HERMES_SRC_BUFFER_POOL_H_
