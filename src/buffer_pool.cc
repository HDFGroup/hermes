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
BufferPool::BufferPool(ShmHeader<BufferPool> *header, hipc::Allocator *alloc) {
  shm_init_header(header, alloc);
  mdm_ = HERMES->mdm_.get();
  borg_ = HERMES->borg_.get();
  rpc_ = &HERMES->rpc_;
  // Initialize header
  hipc::make_ref<BpTargetAllocs>(header_->free_lists_, alloc);
  // [target] [cpu] [page_size]
  header_->ntargets_ = mdm_->targets_->size();
  header_->ncpu_ = HERMES_SYSTEM_INFO->ncpu_;
  header_->nslabs_ = 0;
  // Get the maximum number of slab sizes over all targets
  for (hipc::Ref<TargetInfo> target : *mdm_->targets_) {
    int dev_id = target->id_.GetDeviceId();
    hipc::Ref<DeviceInfo> dev_info = (*mdm_->devices_)[dev_id];
    if (header_->nslabs_ < dev_info->slab_sizes_->size()) {
      header_->nslabs_ = dev_info->slab_sizes_->size();
    }
  }
  // Create target free lists
  shm_deserialize_main();
  target_allocs_->resize(header_->ntargets_ * header_->ncpu_ *
                         header_->nslabs_);
  // Initialize target free lists
  for (u16 target_id = 0; target_id < header_->ntargets_; ++target_id) {
    for (size_t cpu = 0; cpu < header_->ncpu_; ++cpu) {
      // Get the target + device info
      hipc::Ref<TargetInfo> target = (*mdm_->targets_)[target_id];
      int dev_id = target->id_.GetDeviceId();
      hipc::Ref<DeviceInfo> dev_info = (*mdm_->devices_)[dev_id];
      size_t size_per_core = target->max_cap_ / header_->ncpu_;
      if (size_per_core < KILOBYTES(1)) {
        LOG(FATAL) << hshm::Formatter::format(
                          "The capacity of the target {} ({} bytes) cannot be divided among"
                          "the {} CPU cores",
                          dev_info->mount_point_->str(),
                          target->max_cap_, header_->ncpu_) << std::endl;
      }

      // Initialize the core's metadata
      hipc::Ref<BpFreeListStat> target_stat;
      GetTargetStatForCpu(target_id, cpu, target_stat);
      target_stat->region_off_ = cpu * size_per_core;
      target_stat->region_size_ = size_per_core;
      target_stat->lock_.Init();
    }
  }
}

/**====================================
 * Destructor
 * ===================================*/

/** Destroy the BPM shared memory. */
void BufferPool::shm_destroy_main() {
  target_allocs_->shm_destroy();
}

/**====================================
 * SHM Deserialize
 * ===================================*/

/** Deserialize the BPM from shared memory */
void BufferPool::shm_deserialize_main() {
  mdm_ = HERMES->mdm_.get();
  borg_ = HERMES->borg_.get();
  rpc_ = &HERMES->rpc_;
  target_allocs_ = hipc::Ref<BpTargetAllocs>(header_->free_lists_, alloc_);
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
  std::vector<BufferInfo> buffers;
  size_t blob_off = 0;
  int cpu = hermes_shm::NodeThreadId().hash() % HERMES_SYSTEM_INFO->ncpu_;

  for (SubPlacement &plcmnt : schema.plcmnts_) {
    // Get the target and device in the placement schema
    if (plcmnt.tid_.GetNodeId() != mdm_->rpc_->node_id_) {
      blob_off += plcmnt.size_;
      continue;
    }
    int dev_id = plcmnt.tid_.GetDeviceId();
    hipc::Ref<DeviceInfo> dev_info = (*mdm_->devices_)[dev_id];

    // Get the number of each buffer size to allocate
    size_t buffer_count;
    std::vector<BpCoin> coins = CoinSelect(dev_info,
                                           plcmnt.size_,
                                           buffer_count);

    // Get this core's target stat and lock
    hipc::Ref<BpFreeListStat> target_stat;
    GetTargetStatForCpu(plcmnt.tid_.GetIndex(), cpu, target_stat);
    hshm::ScopedMutex(target_stat->lock_);

    // Allocate buffers
    AllocateBuffers(plcmnt.size_,
                    coins,
                    plcmnt.tid_,
                    cpu,
                    target_stat,
                    blob_off,
                    buffers);
  }
  borg_->LocalPlaceBlobInBuffers(blob, buffers);
  return buffers;
}

/**
 * The RPC of LocalAllocateAndSendBuffers
 * */
std::vector<BufferInfo>
BufferPool::GlobalAllocateAndSetBuffers(PlacementSchema &schema,
                                        const Blob &blob) {
  // Get the nodes to transfer buffers to
  size_t total_size;
  auto unique_tgts = GroupByTarget(schema, total_size);
  std::vector<BufferInfo> info;

  // Send the buffers to each node
  for (auto &[tid, size] : unique_tgts) {
    std::vector<BufferInfo> sub_info;
    if (NODE_ID_IS_LOCAL(tid.GetNodeId())) {
      sub_info = LocalAllocateAndSetBuffers(schema, blob);
    } else {
      sub_info = rpc_->IoCall<std::vector<BufferInfo>>(
          tid.GetNodeId(), "RpcAllocateAndSetBuffers",
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
                                 int cpu,
                                 hipc::Ref<BpFreeListStat> &target_stat,
                                 size_t &blob_off,
                                 std::vector<BufferInfo> &buffers) {
  // Get this target's stack allocator
  size_t rem_size = total_size;

  // Allocate each slab size
  for (size_t slab_id = 0; slab_id < coins.size(); ++slab_id) {
    size_t slab_size = coins[slab_id].slab_size_;
    size_t slab_count = coins[slab_id].count_;
    while(slab_count) {
      // Get this core's free list for the current slab size
      hipc::Ref<BpFreeListStat> free_list_stat;
      hipc::Ref<BpFreeList> free_list;
      GetFreeListForCpu(tid.GetIndex(), cpu, slab_id,
                        free_list, free_list_stat);

      // Allocate slabs
      AllocateSlabs(rem_size, slab_size, slab_count, slab_id, tid, free_list,
                    free_list_stat, target_stat, blob_off, buffers);

      // Go to the next target if there was not enough space in this target
      if (slab_count > 0) {
        tid.bits_.index_ += 1;
        if (tid.GetIndex() >= mdm_->targets_->size()) {
          LOG(FATAL) << "BORG ran out of space on all targets."
                     << "This shouldn't happen."
                     << "Please increase the amount of space dedicated to PFS"
                     << std::endl;
        }
        GetTargetStatForCpu(tid.GetIndex(), cpu, target_stat);
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
 * @param free_list The CPU free list for this slab size
 * @param free_list_stat The metadata for the free list
 * @param target_stat The metadata for the CPU core
 * @param blob_off [out] The current size of the blob which has been placed
 * @param buffers [out] The buffers which were allocated
 * */
void BufferPool::AllocateSlabs(size_t &rem_size,
                               size_t slab_size,
                               size_t &slab_count,
                               size_t slab_id,
                               TargetId tid,
                               hipc::Ref<BpFreeList> &free_list,
                               hipc::Ref<BpFreeListStat> &free_list_stat,
                               hipc::Ref<BpFreeListStat> &target_stat,
                               size_t &blob_off,
                               std::vector<BufferInfo> &buffers) {
  while (slab_count > 0) {
    BpSlot slot =
        AllocateSlabSize(slab_size, free_list, free_list_stat, target_stat);
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
 * Allocate a buffer of a particular size
 *
 * @param slab_id The size of slab to allocate
 * @param tid The target to (ideally) allocate the slab from
 * @param coin The buffer size information
 *
 * @return returns a BufferPool (BP) slot. The slot is NULL if the
 * target didn't have enough remaining space.
 * */
BpSlot BufferPool::AllocateSlabSize(size_t slab_size,
                                    hipc::Ref<BpFreeList> &free_list,
                                    hipc::Ref<BpFreeListStat> &free_list_stat,
                                    hipc::Ref<BpFreeListStat> &target_stat) {
  BpSlot slot(0, 0);

  // Case 1: Slab is cached on this core
  if (free_list->size()) {
    auto first = free_list->begin();
    slot = **first;
    free_list->erase(first);
    return slot;
  }

  // Case 2: Allocate slab from stack
  if (slot.IsNull() && target_stat->region_size_ >= slab_size) {
    slot.t_off_ = target_stat->region_off_.fetch_add(slab_size);
    slot.t_size_ = slab_size;
    target_stat->region_size_.fetch_sub(slab_size);
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
std::vector<BpCoin> BufferPool::CoinSelect(hipc::Ref<DeviceInfo> &dev_info,
                                           size_t total_size,
                                           size_t &buffer_count) {
  std::vector<BpCoin> coins(dev_info->slab_sizes_->size());
  size_t rem_size = total_size;
  buffer_count = 0;

  while (rem_size) {
    // Find the slab size nearest to the rem_size
    size_t i = 0, slab_size = 0;
    for (hipc::Ref<size_t> tmp_slab_size : *dev_info->slab_sizes_) {
      slab_size = *tmp_slab_size;
      if (slab_size >= rem_size) {
        break;
      }
      ++i;
    }
    if (i == dev_info->slab_sizes_->size()) { i -= 1; }

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
  int cpu = hermes_shm::NodeThreadId().hash() % HERMES_SYSTEM_INFO->ncpu_;
  for (BufferInfo &info : buffers) {
    // Acquire the main CPU lock for the target
    hipc::Ref<BpFreeListStat> target_stat;
    GetTargetStatForCpu(info.tid_.GetIndex(), cpu, target_stat);
    hermes_shm::ScopedMutex(target_stat->lock_);

    // Get this core's free list for the page_size
    hipc::Ref<BpFreeListStat> free_list_stat;
    hipc::Ref<BpFreeList> free_list;
    GetFreeListForCpu(info.tid_.GetIndex(), cpu, info.t_slab_,
                      free_list, free_list_stat);
    free_list->emplace_front(info.t_off_, info.t_size_);
  }
  return true;
}

/**
 * Free buffers from the BufferPool (global)
 * */
bool BufferPool::GlobalReleaseBuffers(std::vector<BufferInfo> &buffers) {
  // Get the nodes to transfer buffers to
  size_t total_size;
  auto unique_tgts = GroupByTarget(buffers, total_size);

  // Send the buffers to each node
  for (auto &[tid, size] : unique_tgts) {
    if (NODE_ID_IS_LOCAL(tid.GetNodeId())) {
      LocalReleaseBuffers(buffers);
    } else {
      rpc_->Call<bool>(tid.GetNodeId(), "RpcReleaseBuffers", buffers);
    }
  }

  return true;
}

/**====================================
 * Helper Methods
 * ===================================*/

/** Get a free list reference */
void BufferPool::GetFreeListForCpu(u16 target_id, int cpu, int slab_id,
                                   hipc::Ref<BpFreeList> &free_list,
                                   hipc::Ref<BpFreeListStat> &free_list_stat) {
  size_t cpu_free_list_idx = header_->GetCpuFreeList(target_id,
                                                     cpu, slab_id);
  if (cpu_free_list_idx >= target_allocs_->size()) {
    LOG(FATAL) << "For some reason, the CPU free list was "
                  "not allocated properly and overflowed." << std::endl;
  }
  hipc::Ref<BpFreeListPair> free_list_p =
      (*target_allocs_)[cpu_free_list_idx];
  free_list_stat = free_list_p->first_;
  free_list = free_list_p->second_;
}

/** Get the stack allocator from the cpu */
void BufferPool::GetTargetStatForCpu(u16 target_id, int cpu,
                                     hipc::Ref<BpFreeListStat> &target_stat) {
  size_t tgt_free_list_start = header_->GetCpuTargetStat(target_id, cpu);
  hipc::Ref<BpFreeListPair> free_list_p =
      (*target_allocs_)[tgt_free_list_start];
  target_stat = free_list_p->first_;
}

}  // namespace hermes
