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
  shm_deserialize(header);
  // Initialize header
  HSHM_MAKE_AR0(header_->free_lists_, alloc);
  // [target] [cpu] [page_size]
  header_->ntargets_ = mdm_->targets_->size();
  header_->concurrency_ = HERMES_SYSTEM_INFO->ncpu_;
  header_->nslabs_ = 0;
  // Get the maximum number of slab sizes over all targets
  for (TargetInfo &target : *mdm_->targets_) {
    int dev_id = target.id_.GetDeviceId();
    DeviceInfo &dev_info = (*mdm_->devices_)[dev_id];
    if (header_->nslabs_ < dev_info.slab_sizes_->size()) {
      header_->nslabs_ = dev_info.slab_sizes_->size();
    }
  }
  // Create target free lists
  target_allocs_->resize(header_->ntargets_ * header_->concurrency_ *
                         header_->nslabs_);
  // Initialize target free lists
  for (u16 target_id = 0; target_id < header_->ntargets_; ++target_id) {
    for (size_t cpu = 0; cpu < header_->concurrency_; ++cpu) {
      // Get the target + device info
      TargetInfo &target = (*mdm_->targets_)[target_id];
      int dev_id = target.id_.GetDeviceId();
      DeviceInfo &dev_info = (*mdm_->devices_)[dev_id];
      size_t size_per_core = target.max_cap_ / header_->concurrency_;
      if (size_per_core < MEGABYTES(1)) {
        HELOG(kFatal, "The capacity of the target {} ({} bytes)"
              " is not enough to give each of the {} CPUs at least"
              " 1MB of space",
              dev_info.mount_point_->str(),
              target.max_cap_, header_->concurrency_)
      }

      // Initialize the core's metadata
      BpFreeListStat *target_stat;
      GetTargetStatForCpu(target_id, cpu, target_stat);
      target_stat->region_off_ = cpu * size_per_core;
      target_stat->region_size_ = size_per_core;
      target_stat->lock_.Init();
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
  target_allocs_ = header_->free_lists_.get();
}

/**====================================
 * Destructor
 * ===================================*/

/** Destroy the BPM shared memory. */
void BufferPool::shm_destroy_main() {
  target_allocs_->shm_destroy();
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
  int cpu = hshm::NodeThreadId().hash() % header_->concurrency_;

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
                    cpu,
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
                                 int cpu,
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
                    cpu, blob_off, buffers);

      // Go to the next target if there was not enough space in this target
      if (slab_count > 0) {
        i32 target_id = tid.GetIndex() + 1;
        if (target_id >= (i32)mdm_->targets_->size()) {
          HELOG(kFatal, "BORG ran out of space on all targets."
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
                               int cpu,
                               size_t &blob_off,
                               std::vector<BufferInfo> &buffers) {
  int cpu_off = 0;
  int ncpu = header_->concurrency_;

  // Get the free list for this CPU
  BpFreeList *free_list;
  BpFreeListStat *free_list_stat;
  BpFreeListStat *target_stat;
  GetFreeListForCpu(tid.GetIndex(), cpu, slab_id, free_list,
                    free_list_stat);
  GetTargetStatForCpu(tid.GetIndex(), cpu, target_stat);
  target_stat->lock_.Lock(10);

  while (slab_count > 0) {
    BpSlot slot =
        AllocateSlabSize(cpu, slab_size,
                         free_list, free_list_stat, target_stat);
    if (slot.IsNull()) {
      if (cpu_off < ncpu) {
        cpu = (cpu + 1) % ncpu;
        target_stat->lock_.Unlock();
        GetFreeListForCpu(tid.GetIndex(), cpu, slab_id, free_list,
                          free_list_stat);
        GetTargetStatForCpu(tid.GetIndex(), cpu, target_stat);
        target_stat->lock_.Lock(11);
        cpu_off += 1;
        continue;
      } else {
        target_stat->lock_.Unlock();
        return;
      }
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
  target_stat->lock_.Unlock();
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
BpSlot BufferPool::AllocateSlabSize(int cpu,
                                    size_t slab_size,
                                    BpFreeList *free_list,
                                    BpFreeListStat *free_list_stat,
                                    BpFreeListStat *target_stat) {
  BpSlot slot(0, 0);

  // Case 1: Slab is cached on this core
  if (free_list->size()) {
    auto first = free_list->begin();
    slot = *first;
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
  int cpu = hshm::NodeThreadId().hash() % header_->concurrency_;
  for (BufferInfo &info : buffers) {
    // Acquire the main CPU lock for the target
    BpFreeListStat *target_stat;
    GetTargetStatForCpu(info.tid_.GetIndex(), cpu, target_stat);
    hshm::ScopedMutex bpm_lock(target_stat->lock_, 12);

    // Get this core's free list for the page_size
    BpFreeListStat *free_list_stat;
    BpFreeList *free_list;
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

/**====================================
 * Helper Methods
 * ===================================*/

/** Get a free list reference */
void BufferPool::GetFreeListForCpu(u16 target_id, int cpu, int slab_id,
                                   BpFreeList* &free_list,
                                   BpFreeListStat* &free_list_stat) {
  size_t cpu_free_list_idx = header_->GetCpuFreeList(target_id,
                                                     cpu, slab_id);
  if (cpu_free_list_idx >= target_allocs_->size()) {
    HELOG(kFatal, "For some reason, the CPU free list was "
          "not allocated properly and overflowed.")
  }
  BpFreeListPair &free_list_p =
      (*target_allocs_)[cpu_free_list_idx];
  free_list_stat = &free_list_p.GetFirst();
  free_list = &free_list_p.GetSecond();
}

/** Get the stack allocator from the cpu */
void BufferPool::GetTargetStatForCpu(u16 target_id, int cpu,
                                     BpFreeListStat* &target_stat) {
  size_t tgt_free_list_start = header_->GetCpuTargetStat(target_id, cpu);
  BpFreeListPair &free_list_p =
      (*target_allocs_)[tgt_free_list_start];
  target_stat = &free_list_p.GetFirst();
}

}  // namespace hermes
