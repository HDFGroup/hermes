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

#include "buffer_pool.h"
#include "metadata_manager.h"
#include "hermes.h"
#include "buffer_organizer.h"

namespace hermes {

/**====================================
 * Default Constructor
 * ===================================*/

/**
* Initialize the BPM and its shared memory.
* REQUIRES mdm to be initialized already.
* */
void BufferPool::shm_init(hipc::ShmArchive<BufferPoolShm> &header,
                          hipc::Allocator *alloc) {
  mdm_ = &HERMES->mdm_;
  borg_ = &HERMES->borg_;
  rpc_ = &HERMES->rpc_;
  header_ = header.get();
  int ntargets = mdm_->targets_->size();
  size_t nslabs = 0;
  // Get the maximum number of slab sizes over all targets
  for (TargetInfo &target : *mdm_->targets_) {
    int dev_id = target.id_.GetDeviceId();
    DeviceInfo &dev_info = (*mdm_->devices_)[dev_id];
    if (nslabs < dev_info.slab_sizes_->size()) {
      nslabs = dev_info.slab_sizes_->size();
    }
  }
  // Initialize header
  HSHM_MAKE_AR(header_->target_allocs_, alloc, ntargets, nslabs);
  CreateTargetFreeListCache(ntargets);
}

void BufferPool::CreateTargetFreeListCache(int ntargets) {
  // Create target free lists
  target_allocs_.resize(ntargets);
  for (int i = 0; i < ntargets; ++i) {
    TargetInfo &target = (*mdm_->targets_)[i];
    int dev_id = target.id_.GetDeviceId();
    DeviceInfo &dev_info = (*mdm_->devices_)[dev_id];
    BpTargetSlabSetIpc &target_alloc_ipc = (*header_->target_allocs_)[i];
    BpTargetSlabSet &target_alloc = target_allocs_[i];
    target_alloc_ipc.region_size_ = target.max_cap_;
    target_alloc.region_off_ = &target_alloc_ipc.region_off_;
    target_alloc.region_size_ = target_alloc_ipc.region_size_;
    target_alloc.slab_sizes_ = dev_info.slab_sizes_->vec();
    target_alloc.lock_ = &target_alloc_ipc.lock_;
    target_alloc.slabs_.reserve(target_alloc.slab_sizes_.size());
    for (BpFreeList &free_list : *target_alloc_ipc.slabs_) {
      target_alloc.slabs_.emplace_back(&free_list);
    }
  }
}

/**====================================
 * SHM Deserialize
 * ===================================*/

/** Deserialize the BPM from shared memory */
void BufferPool::shm_deserialize(hipc::ShmArchive<BufferPoolShm> &header) {
  mdm_ = &HERMES->mdm_;
  borg_ = &HERMES->borg_;
  rpc_ = &HERMES->rpc_;
  header_ = header.get();
  int ntargets = mdm_->targets_->size();
  CreateTargetFreeListCache(ntargets);
}

/**====================================
 * Destructor
 * ===================================*/

/** Destroy the BPM shared memory. */
void BufferPool::shm_destroy_main() {
  header_->target_allocs_->shm_destroy();
}

/**====================================
 * Allocate Buffers
 * ===================================*/

/**
* Allocate buffers from the targets according to the schema
* */
std::vector<BufferInfo>
BufferPool::LocalAllocateAndSetBuffers(PlacementSchema &schema,
                                       const Blob &blob) {
  AUTO_TRACE(1)
  std::vector<BufferInfo> buffers;
  size_t blob_off = 0;

  TIMER_START("AllocateBuffers")
  for (SubPlacement &plcmnt : schema.plcmnts_) {
    // Get the target and device in the placement schema
    if (plcmnt.tid_.GetNodeId() != mdm_->rpc_->node_id_) {
      blob_off += plcmnt.size_;
      continue;
    }
    int dev_id = plcmnt.tid_.GetDeviceId();
    DeviceInfo &dev_info = (*mdm_->devices_)[dev_id];

    // Get the number of each buffer size to allocate
    size_t buffer_count;
    std::vector<BpCoin> coins = CoinSelect(dev_info,
                                           plcmnt.size_,
                                           buffer_count);

    // Allocate buffers
    AllocateBuffers(plcmnt.size_,
                    coins,
                    plcmnt.tid_,
                    blob_off,
                    buffers);
  }
  TIMER_END()
  borg_->LocalPlaceBlobInBuffers(blob, buffers);
  return buffers;
}

/**
 * The RPC of LocalAllocateAndSendBuffers
 * */
std::vector<BufferInfo>
BufferPool::GlobalAllocateAndSetBuffers(PlacementSchema &schema,
                                        const Blob &blob) {
  AUTO_TRACE(1)
  // Get the nodes to transfer buffers to
  size_t total_size;
  auto unique_nodes = GroupByNodeId(schema, total_size);
  std::vector<BufferInfo> info;

  // Send the buffers to each node
  for (auto &[node_id, size] : unique_nodes) {
    std::vector<BufferInfo> sub_info;
    if (NODE_ID_IS_LOCAL(node_id)) {
      sub_info = LocalAllocateAndSetBuffers(schema, blob);
    } else {
      sub_info = rpc_->IoCall<std::vector<BufferInfo>>(
          node_id, "RpcAllocateAndSetBuffers",
          IoType::kWrite, blob.data(), blob.size(),
          blob.size(), schema);
    }

    // Concatenate
    info.reserve(info.size() + sub_info.size());
    for (BufferInfo &tmp_info : sub_info) {
      info.emplace_back(tmp_info);
    }
  }

  return info;
}

/**
 * Allocate requested slabs from this target.
 * If the target runs out of space, it will provision from the next target.
 * We assume there is a baseline target, which can house an infinite amount
 * of data. This would be the PFS in an HPC machine.
 *
 * @param total_size the total amount of data being placed in this target
 * @param coins The requested number of slabs to allocate from this target
 * @param target_id The ID of the (ideal) target to allocate from
 * @param blob_off [out] The current size of the blob which has been placed
 * @param buffers [out] The buffers which were allocated
 * */
void BufferPool::AllocateBuffers(size_t total_size,
                                 std::vector<BpCoin> &coins,
                                 TargetId tid,
                                 size_t &blob_off,
                                 std::vector<BufferInfo> &buffers) {
  // Get this target's stack allocator
  size_t rem_size = total_size;

  // Allocate each slab size
  for (size_t slab_id = 0; slab_id < coins.size(); ++slab_id) {
    size_t slab_size = coins[slab_id].slab_size_;
    size_t slab_count = coins[slab_id].count_;
    while (slab_count) {
      // Allocate slabs
      AllocateSlabs(rem_size, slab_size, slab_count, slab_id, tid,
                    blob_off, buffers);

      // Go to the next target if there was not enough space in this target
      if (slab_count > 0) {
        i32 target_id = tid.GetIndex() + 1;
        if (target_id >= (i32)mdm_->targets_->size()) {
          HELOG(kFatal, "BPM ran out of space on all targets."
                " This shouldn't happen."
                " Please increase the amount of space dedicated to PFS.")
        }
        tid = (*mdm_->targets_)[target_id].id_;
      }
    }
  }
}

/**
 * Allocate a set of slabs of particular size from this target.
 *
 * @param rem_size the amount of data remaining that needs to be allocated
 * @param slab_size Size of the slabs to allocate
 * @param slab_count The count of this slab size to allocate
 * @param slab_id The offset of the slab in the device's slab list
 * @param tid The target to allocate slabs from
 * @param cpu the CPU this node is on
 * @param blob_off [out] The current size of the blob which has been placed
 * @param buffers [out] The buffers which were allocated
 * */
void BufferPool::AllocateSlabs(size_t &rem_size,
                               size_t slab_size,
                               size_t &slab_count,
                               size_t slab_id,
                               TargetId tid,
                               size_t &blob_off,
                               std::vector<BufferInfo> &buffers) {
  // Get the free list for this CPU
  BpTargetSlabSet &target_alloc = target_allocs_[tid.GetIndex()];
  BpFreeList *free_list = target_alloc.slabs_[slab_id];
  hshm::ScopedMutex scoped_lock(*target_alloc.lock_, 0);

  while (slab_count > 0) {
    BpSlot slot = AllocateSlabSize(slab_size, *free_list, target_alloc);
    if (slot.IsNull()) {
      return;
    }
    buffers.emplace_back();
    BufferInfo &info = buffers.back();
    info.t_off_ = slot.t_off_;
    info.t_size_ = slot.t_size_;
    info.t_slab_ = slab_id;
    info.blob_off_ = blob_off;
    info.blob_size_ = slot.t_size_;
    if (rem_size < slot.t_size_) {
      info.blob_size_ = rem_size;
    }
    rem_size -= info.blob_size_;
    info.tid_ = tid;
    blob_off += info.blob_size_;
    mdm_->LocalUpdateTargetCapacity(tid, -static_cast<off64_t>(slab_size));
    --slab_count;
  }
}

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
BpSlot BufferPool::AllocateSlabSize(size_t slab_size,
                                    BpFreeList &free_list,
                                    BpTargetSlabSet &target_alloc) {
  BpSlot slot(0, 0);

  // Case 1: Slab is cached on this core
  if (free_list.size()) {
    auto first = free_list.begin();
    slot = *first;
    free_list.erase(first);
    return slot;
  }

  // Case 2: Allocate slab from stack
  size_t region_off = *target_alloc.region_off_;
  size_t rem = target_alloc.region_size_ - region_off;
  if (slot.IsNull() && rem >= slab_size) {
    slot.t_off_ = target_alloc.region_off_->fetch_add(slab_size);
    if (slot.t_off_ > target_alloc.region_size_ - slab_size) {
      slot.t_size_ = 0;
      return slot;
    }
    slot.t_size_ = slab_size;
    return slot;
  }

  // Case 3: Coalesce
  // TOOD(llogan)

  // Case 4: No more space left in this target.
  return slot;
}

/**
   * Determines a reasonable allocation of buffers based on the size of I/O.
   * Returns the number of each slab size to allocate
   * */
std::vector<BpCoin> BufferPool::CoinSelect(DeviceInfo &dev_info,
                                           size_t total_size,
                                           size_t &buffer_count) {
  std::vector<BpCoin> coins(dev_info.slab_sizes_->size());
  size_t rem_size = total_size;
  buffer_count = 0;

  while (rem_size) {
    // Find the slab size nearest to the rem_size
    size_t i = 0, slab_size = 0;
    for (size_t &tmp_slab_size : *dev_info.slab_sizes_) {
      slab_size = tmp_slab_size;
      if (slab_size >= rem_size) {
        break;
      }
      ++i;
    }
    if (i == dev_info.slab_sizes_->size()) { i -= 1; }

    // Divide rem_size into slabs
    if (rem_size > slab_size) {
      coins[i].count_ += rem_size / slab_size;
      coins[i].slab_size_ = slab_size;
      rem_size %= slab_size;
    } else {
      coins[i].count_ += 1;
      coins[i].slab_size_ = slab_size;
      rem_size = 0;
    }
    buffer_count += coins[i].count_;
  }

  return coins;
}

/**====================================
 * Free Buffers
 * ===================================*/

/**
 * Free buffers from the BufferPool
 * */
bool BufferPool::LocalReleaseBuffers(std::vector<BufferInfo> &buffers) {
  AUTO_TRACE(1)
  HILOG(kDebug, "Releasing buffers")
  for (BufferInfo &info : buffers) {
    BpTargetSlabSet &target_alloc = target_allocs_[info.tid_.GetIndex()];
    BpFreeList *free_list = target_alloc.slabs_[info.t_slab_];
    hshm::ScopedMutex scoped_lock(*target_alloc.lock_, 0);
    free_list->emplace_front(info.t_off_, info.t_size_);
  }
  return true;
}

/**
 * Free buffers from the BufferPool (global)
 * */
bool BufferPool::GlobalReleaseBuffers(std::vector<BufferInfo> &buffers) {
  AUTO_TRACE(1)
  // Get the nodes to transfer buffers to
  size_t total_size;
  auto unique_nodes = GroupByNodeId(buffers, total_size);

  // Send the buffers to each node
  for (auto &[node_id, size] : unique_nodes) {
    if (NODE_ID_IS_LOCAL(node_id)) {
      LocalReleaseBuffers(buffers);
    } else {
      rpc_->Call<bool>(node_id, "RpcReleaseBuffers", buffers);
    }
  }

  return true;
}

}  // namespace hermes
