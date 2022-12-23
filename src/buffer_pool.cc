//
// Created by lukemartinlogan on 12/19/22.
//

#include "buffer_pool.h"
#include "metadata_manager.h"

namespace hermes {

/** Initialize the BPM and its shared memory. */
void BufferPoolManager::shm_init(MetadataManager *mdm) {
  mdm_ = mdm;
  target_allocs_.shm_init(nullptr);
  target_allocs_.resize(mdm_->targets_.size());
  for (auto &target : mdm_->targets_) {
    auto &dev_info = mdm_->devices_[target.id_.GetDeviceId()];
    dev_info.mount_point_;
  }
}

/** Store the BPM in shared memory */
void BufferPoolManager::shm_serialize(BufferPoolManagerShmHeader *header) {
  target_allocs_ >> header->alloc_ar_;
}

/** Deserialize the BPM from shared memory */
void BufferPoolManager::shm_deserialize(BufferPoolManagerShmHeader *header) {
  target_allocs_ << header->alloc_ar_;
}

/**
 * Allocate buffers from the targets according to the schema
 *
 * TODO(llogan): use better allocator policy
 * */
lipc::vector<BufferInfo>
BufferPoolManager::LocalAllocateBuffers(PlacementSchema &schema, Blob &blob) {
  lipc::vector<BufferInfo> buffers(nullptr);
  for (auto plcmnt : schema.plcmnts_) {
    if (plcmnt.tid_.GetNodeId() != mdm_->rpc_->node_id_) {
      continue;
    }
    auto &alloc = target_allocs_[plcmnt.tid_.GetDeviceId()];
    BufferInfo info;
    info.off_ = alloc.cur_off_;
    info.size_ = plcmnt.size_;
    info.tid_ = plcmnt.tid_;
    alloc.cur_off_ += plcmnt.size_;
    buffers.emplace_back(info);
    mdm_->LocalUpdateTargetCapacity(info.tid_,
                                    static_cast<off64_t>(info.size_));
  }
  return buffers;
}

/**
 * Free buffers from the BufferPool
 *
 * TODO(llogan): actually implement
 * */
bool BufferPoolManager::LocalReleaseBuffers(lipc::vector<BufferInfo> &buffers) {
  return true;
}

}  // namespace hermes