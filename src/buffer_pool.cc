//
// Created by lukemartinlogan on 12/19/22.
//

#include "buffer_pool.h"
#include "metadata_manager.h"
#include "hermes.h"

namespace hermes {

/**
 * Initialize the BPM and its shared memory.
 * REQUIRES mdm to be initialized already.
 * */
void BufferPool::shm_init(BufferPoolShmHeader *header) {
  mdm_ = &HERMES->mdm_;
  borg_ = &HERMES->borg_;
  target_allocs_ = lipc::make_mptr<lipc::vector<BufferPoolAllocator>>(nullptr);
  target_allocs_->resize(mdm_->targets_->size());
  shm_serialize(header);
}

/** Destroy the BPM shared memory. */
void BufferPool::shm_destroy() {
  target_allocs_.shm_destroy();
}

/** Store the BPM in shared memory */
void BufferPool::shm_serialize(BufferPoolShmHeader *header) {
  target_allocs_ >> header->alloc_ar_;
}

/** Deserialize the BPM from shared memory */
void BufferPool::shm_deserialize(BufferPoolShmHeader *header,
                                 MetadataManager *mdm) {
  mdm_ = mdm;
  target_allocs_ << header->alloc_ar_;
}

/**
 * Allocate buffers from the targets according to the schema
 *
 * TODO(llogan): use better allocator policy
 * */
lipc::vector<BufferInfo>
BufferPool::LocalAllocateAndSetBuffers(PlacementSchema &schema,
                                              Blob &blob) {
  lipc::vector<BufferInfo> buffers(nullptr);
  for (auto plcmnt : schema.plcmnts_) {
    if (plcmnt.tid_.GetNodeId() != mdm_->rpc_->node_id_) {
      continue;
    }
    auto &alloc = (*target_allocs_)[plcmnt.tid_.GetDeviceId()];
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
bool BufferPool::LocalReleaseBuffers(lipc::vector<BufferInfo> &buffers) {
  return true;
}

}  // namespace hermes