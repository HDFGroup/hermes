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

/**
* Initialize the BPM and its shared memory.
* REQUIRES mdm to be initialized already.
* */
void BufferPool::shm_init_main(ShmHeader<BufferPool> *header,
                               hipc::Allocator *alloc) {
  shm_init_allocator(alloc);
  shm_init_header(header, alloc_);
  mdm_ = &HERMES->mdm_;
  borg_ = &HERMES->borg_;
  // [target] [cpu] [page_size]
  header_->ntargets_ = mdm_->targets_->size();
  header_->ncpu_ = HERMES_SYSTEM_INFO->ncpu_;
  header_->nslabs_ = 0;
  // Get the maximum number of slabs over all targets
  for (hipc::ShmRef<TargetInfo> target : *mdm_->targets_) {
    int dev_id = target->id_.GetDeviceId();
    hipc::ShmRef<DeviceInfo> dev_info = (*mdm_->devices_)[dev_id];
    if (header_->nslabs_ < dev_info->slab_sizes_->size()) {
      header_->nslabs_ = dev_info->slab_sizes_->size();
    }
  }
  // Create target free lists
  shm_deserialize_main();
  target_allocs_->resize(header_->ntargets_ * header_->ncpu_ *
                         header_->nslabs_);
  // Initialize target free lists
  for (size_t i = 0; i < header_->ntargets_; ++i) {
    for (size_t j = 0; j < header_->ncpu_; ++j) {
      size_t free_list_start = header_->GetCpuFreeList(i, j);
      hipc::ShmRef<TargetInfo> target = (*mdm_->targets_)[i];
      int dev_id = target->id_.GetDeviceId();
      hipc::ShmRef<DeviceInfo> dev_info = (*mdm_->devices_)[dev_id];
      for (size_t k = 0; k < dev_info->slab_sizes_->size(); ++k) {
        hipc::ShmRef<BpFreeListPair> free_list_p =
            (*target_allocs_)[free_list_start + k];
        hipc::ShmRef<BpFreeListStat> &stat = free_list_p->first_;
        hipc::ShmRef<BpFreeList> &free_list = free_list_p->second_;
        stat->page_size_ = *(*dev_info->slab_sizes_)[k];
        stat->region_off_ = 0;
        stat->region_size_ = target->max_cap_;
        stat->lock_.Init();
      }
    }
  }
}

/** Store the BPM in shared memory */
void BufferPool::shm_serialize_main() const {}

/** Deserialize the BPM from shared memory */
void BufferPool::shm_deserialize_main() {
  mdm_ = &HERMES->mdm_;
  borg_ = &HERMES->borg_;
  target_allocs_ = hipc::ShmRef<BpTargetAllocs>(
      header_->free_lists_.internal_ref(alloc_));
}

/** Destroy the BPM shared memory. */
void BufferPool::shm_destroy_main() {
  target_allocs_->shm_destroy();
}

/**
* Allocate buffers from the targets according to the schema
* */
hipc::vector<BufferInfo>
BufferPool::LocalAllocateAndSetBuffers(PlacementSchema &schema,
                                       const Blob &blob) {
  hipc::vector<BufferInfo> buffers(HERMES->main_alloc_);
  size_t blob_off = 0;
  int cpu = hermes_shm::NodeThreadId().hash() % HERMES_SYSTEM_INFO->ncpu_;

  for (SubPlacement &plcmnt : schema.plcmnts_) {
    // Get the target and device in the placement schema
    if (plcmnt.tid_.GetNodeId() != mdm_->rpc_->node_id_) {
      blob_off += plcmnt.size_;
      continue;
    }
    int dev_id = plcmnt.tid_.GetDeviceId();
    hipc::ShmRef<DeviceInfo> dev_info = (*mdm_->devices_)[dev_id];

    // Get the first CPU free list pair & acquire lock
    size_t free_list_start = header_->GetCpuFreeList(
        plcmnt.tid_.GetIndex(), cpu);
    hipc::ShmRef<BpFreeListPair> first_free_list_p =
        (*target_allocs_)[free_list_start];
    hermes_shm::ScopedMutex(first_free_list_p->first_->lock_);

    // Get the number of each buffer size to allocate
    size_t buffer_count, total_alloced_size;
    std::vector<BpCoin> coins = CoinSelect(dev_info,
                                           plcmnt.size_,
                                           buffer_count,
                                           total_alloced_size);

    // Allocate buffers
    AllocateBuffers(plcmnt, buffers, coins,
                    plcmnt.tid_.GetIndex(),
                    dev_info->slab_sizes_->size(),
                    free_list_start,
                    blob_off);
    mdm_->LocalUpdateTargetCapacity(plcmnt.tid_,
                                    -static_cast<off64_t>(total_alloced_size));
  }
  borg_->LocalPlaceBlobInBuffers(blob, buffers);
  return std::move(buffers);
}

/**
* Allocate each size of buffer from either the free list or the
* device growth allocator
* */
void BufferPool::AllocateBuffers(SubPlacement &plcmnt,
                                 hipc::vector<BufferInfo> &buffers,
                                 std::vector<BpCoin> &coins,
                                 u16 target_id,
                                 size_t num_slabs,
                                 size_t free_list_start,
                                 size_t &blob_off) {
  // Get this target's stack allocator
  size_t target_free_list_start = header_->GetTargetFreeList(
      target_id);
  hipc::ShmRef<BpFreeListPair> target_free_list_p =
      (*target_allocs_)[target_free_list_start];
  hipc::ShmRef<BpFreeListStat> &target_stat = target_free_list_p->first_;
  size_t rem_size = plcmnt.size_;

  for (size_t i = 0; i < num_slabs; ++i) {
    if (coins[i].count_ == 0) { continue; }

    // Get this core's free list for the page_size
    hipc::ShmRef<BpFreeListPair> free_list_p =
        (*target_allocs_)[free_list_start + i];
    hipc::ShmRef<BpFreeListStat> &stat = free_list_p->first_;
    hipc::ShmRef<BpFreeList> &free_list = free_list_p->second_;

    // Allocate slabs
    for (size_t j = 0; j < coins[i].count_; ++j) {
      BpSlot slot = AllocateSlabSize(coins[i], stat, free_list, target_stat);
      BufferInfo info;
      info.t_off_ = slot.t_off_;
      info.t_size_ = slot.t_size_;
      info.t_slab_ = j;
      info.blob_off_ = blob_off;
      info.blob_size_ = slot.t_size_;
      if (rem_size < slot.t_size_) {
        info.blob_size_ = rem_size;
      }
      rem_size -= info.blob_size_;
      info.tid_ = plcmnt.tid_;
      blob_off += info.blob_size_;
      buffers.emplace_back(info);
    }
  }
}

/**
* Allocate each size of buffer from either the free list or the
* device growth allocator
* */
BpSlot BufferPool::AllocateSlabSize(BpCoin &coin,
                                    hipc::ShmRef<BpFreeListStat> &stat,
                                    hipc::ShmRef<BpFreeList> &free_list,
                                    hipc::ShmRef<BpFreeListStat> &target_stat) {
  BpSlot slot;

  // Case 1: Slab is cached on this core
  if (free_list->size()) {
    auto first = free_list->begin();
    slot = **first;
    free_list->erase(first);
    return slot;
  }

  // Case 2: Allocate slab from stack
  if (slot.IsNull()) {
    slot.t_off_ = target_stat->region_off_.fetch_add(coin.slab_size_);
    slot.t_size_ = coin.slab_size_;
    target_stat->region_size_ -= coin.slab_size_;
    return slot;
  }

  // Case 3: Coalesce
  // TOOD(llogan)

  // Case 4: Out of space
  LOG(FATAL) << "Ran out of space in BufferPool" << std::endl;
}

/**
* Determines a reasonable allocation of buffers based on the size of I/O.
* */
std::vector<BpCoin> BufferPool::CoinSelect(hipc::ShmRef<DeviceInfo> &dev_info,
                                           size_t total_size,
                                           size_t &buffer_count,
                                           size_t &total_alloced_size) {
  std::vector<BpCoin> coins(dev_info->slab_sizes_->size());
  size_t rem_size = total_size;
  buffer_count = 0;
  total_alloced_size = 0;

  while (rem_size) {
    // Find the slab size nearest to the rem_size
    size_t i = 0, slab_size = 0;
    for (hipc::ShmRef<size_t> tmp_slab_size : *dev_info->slab_sizes_) {
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
    total_alloced_size += coins[i].count_ * coins[i].slab_size_;
  }

  return coins;
}

/**
* Free buffers from the BufferPool
* */
bool BufferPool::LocalReleaseBuffers(hipc::vector<BufferInfo> &buffers) {
  int cpu = hermes_shm::NodeThreadId().hash() % HERMES_SYSTEM_INFO->ncpu_;

  for (hipc::ShmRef<BufferInfo> info : buffers) {
    // Acquire the main CPU lock for the target
    size_t free_list_start =
        header_->GetCpuFreeList(info->tid_.GetIndex(), cpu);
    hipc::ShmRef<BpFreeListPair> first_free_list_p =
        (*target_allocs_)[free_list_start];
    hermes_shm::ScopedMutex(first_free_list_p->first_->lock_);

    // Get this core's free list for the page_size
    hipc::ShmRef<BpFreeListPair> free_list_p =
        (*target_allocs_)[free_list_start + info->t_slab_];
    hipc::ShmRef<BpFreeListStat> &stat = free_list_p->first_;
    hipc::ShmRef<BpFreeList> &free_list = free_list_p->second_;
    free_list->emplace_front(info->t_off_, info->t_size_);
  }
  return true;
}

}  // namespace hermes
