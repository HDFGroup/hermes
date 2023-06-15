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

/** Represents the list of available buffers */
typedef hipc::slist<BpSlot> BpFreeList;

/** Represents the set of slabs in shared memory */
 struct BpTargetSlabSetIpc : public hipc::ShmContainer {
  SHM_CONTAINER_TEMPLATE(BpTargetSlabSetIpc, BpTargetSlabSetIpc)
  std::atomic<size_t> region_off_;  /**< Current offset in the target */
   size_t region_size_; /**< Current space remaining in the tgt */
  /** A free list per slab size */
  hipc::ShmArchive<hipc::vector<BpFreeList>> slabs_;
  /** Lock this target */
  Mutex lock_;

   /** SHM constructor. Default. */
   explicit BpTargetSlabSetIpc(hipc::Allocator *alloc) {
     shm_init_container(alloc);
     HSHM_MAKE_AR0(slabs_, alloc)
     SetNull();
   }

   /** SHM emplace constructor */
   explicit BpTargetSlabSetIpc(hipc::Allocator *alloc,
                               size_t nslabs) {
     shm_init_container(alloc);
     HSHM_MAKE_AR(slabs_, alloc, nslabs)
     region_size_ = 0;
     region_off_ =  0;
     SetNull();
   }

   /** SHM copy constructor. */
   explicit BpTargetSlabSetIpc(hipc::Allocator *alloc,
                               const BpTargetSlabSetIpc &other) {
     shm_init_container(alloc);
     SetNull();
   }

   /** SHM copy assignment operator */
   BpTargetSlabSetIpc& operator=(const BpTargetSlabSetIpc &other) {
     if (this != &other) {
       shm_destroy();
       SetNull();
     }
     return *this;
   }

   /** Destructor. */
   void shm_destroy_main() {
     slabs_->shm_destroy();
   }

   /** Check if Null */
   HSHM_ALWAYS_INLINE bool IsNull() {
     return false;
   }

   /** Set to null */
   HSHM_ALWAYS_INLINE void SetNull() {
   }
};

struct BpTargetSlabSet {
  std::atomic<size_t> *region_off_;  /**< Current offset in the target */
  size_t region_size_; /**< Current space remaining in the tgt */
  /** A free list per slab size */
  std::vector<BpFreeList*> slabs_;
  /** The set of slabs sizes */
  std::vector<size_t> slab_sizes_;
  /** Lock this target */
  Mutex *lock_;
};

/**
 * The shared-memory representation of the BufferPool
 * */
struct BufferPoolShm {
  hipc::ShmArchive<hipc::vector<BpTargetSlabSetIpc>> target_allocs_;
};

/**
 * Responsible for managing the buffering space of all node-local targets.
 * */
class BufferPool {
 private:
  MetadataManager *mdm_;
  BufferOrganizer *borg_;
  RPC_TYPE *rpc_;
  BufferPoolShm *header_;
  /** Per-target allocator */
  std::vector<BpTargetSlabSet> target_allocs_;

 public:
  /**====================================
   * Default Constructor
   * ===================================*/

  /** Default constructor */
  BufferPool() = default;

  /**====================================
   * SHM Init
   * ===================================*/

  /**
   * Initialize the BPM and its shared memory.
   * REQUIRES mdm to be initialized already.
   * */
  void shm_init(hipc::ShmArchive<BufferPoolShm> &header,
                hipc::Allocator *alloc);

  /** Create the free list cache */
  void CreateTargetFreeListCache(int ntargets);

  /**====================================
   * SHM Deserialize
   * ===================================*/

  /** Deserialize the BPM from shared memory */
  void shm_deserialize(hipc::ShmArchive<BufferPoolShm> &header);

  /**====================================
   * Destructor
   * ===================================*/

  /** Destroy the BPM shared memory. */
  void shm_destroy_main();

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
  std::vector<BpCoin> CoinSelect(DeviceInfo &dev_info,
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
   * @param blob_off [out] The current size of the blob which has been placed
   * @param buffers [out] The buffers which were allocated
   * */
  void AllocateBuffers(size_t total_size,
                       std::vector<BpCoin> &coins,
                       TargetId tid,
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
   * @param blob_off [out] The current size of the blob which has been placed
   * @param buffers [out] The buffers which were allocated
   * */
  void AllocateSlabs(size_t &rem_size,
                     size_t slab_size,
                     size_t &slab_count,
                     size_t slab_id,
                     TargetId tid,
                     size_t &blob_off,
                     std::vector<BufferInfo> &buffers);

  /**
   * Allocate a buffer of a particular size from the free list or stack
   *
   * @param slab_size The size of slab to allocate
   * @param free_list the free list to allocate from
   * @param target_alloc the overall target allocator
   *
   * @return returns a BufferPool (BP) slot. The slot is NULL if the
   * target didn't have enough remaining space.
   * */
  BpSlot AllocateSlabSize(size_t slab_size,
                          BpFreeList &free_list,
                          BpTargetSlabSet &target_alloc);


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
};

}  // namespace hermes

#endif  // HERMES_SRC_BUFFER_POOL_H_
