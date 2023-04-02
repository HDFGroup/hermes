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

#include "buffer_organizer.h"
#include "metadata_manager.h"
#include "borg_io_clients/borg_io_client_factory.h"
#include "hermes.h"
#include "bucket.h"

namespace hermes {

/**====================================
 * BORG I/O thread manager
 * ===================================*/

/** Spawn the I/O threads */
void BorgIoThreadManager::Spawn(int num_threads) {
  // Create the queues
  queues_.resize(num_threads);

  // Define flush worker thread function
  // The function will continue working until all pending flushes have
  // been processed
  auto flush = [](void *params) {
    BorgIoThreadQueue &bq = reinterpret_cast<BorgIoThreadQueue&>(params);
    while (HERMES_THREAD_MANAGER->Alive() ||
           !(HERMES_THREAD_MANAGER->Alive() && bq.queue_.size())) {
      /**
       * NOTE(llogan): this while condition is unsafe. The flushing thread
       * may not yet be scheduled and the server terminates. We need to
       * make this safer.
       *
       * In addition, the Hermes daemon may terminate before all daemons
       * finish. We need to add back MPI_Barrier.
       * */
      while (bq.queue_.size()) {
        Blob blob;
        BorgIoTask& info = bq.queue_.front();
        std::shared_ptr<api::Bucket> bkt = HERMES->GetBucket(info.bkt_id_);
        bkt->Get(info.blob_id_, blob, bkt->GetContext());
        FlushTraitParams trait_params;
        for (Trait* trait : info.traits_) {
          trait_params.blob_ = &blob;
          trait_params.bkt_ = &bkt;
          trait->Run(HERMES_TRAIT_FLUSH, &trait_params);
        }
        bq.queue_.pop();
        bq.load_.fetch_sub(info.blob_size_);
      }
      tl::thread::self().sleep(*HERMES->rpc_.server_engine_, 50);
    }
  };

  // Create the flushing threads
  for (int i = 0; i < num_threads; ++i) {
    queues_[i].id_ = i;
    queues_[i].load_ = 0;
    HERMES_THREAD_MANAGER->Spawn(flush, &queues_[i]);
  }
}

/**====================================
 * Default Constructor
 * ===================================*/

/**
 * Initialize the BORG
 * REQUIRES mdm to be initialized already.
 * */
BufferOrganizer::BufferOrganizer(ShmHeader<BufferOrganizer> *header,
                                 hipc::Allocator *alloc) {
  mdm_ = HERMES->mdm_.get();
  rpc_ = &HERMES->rpc_;

  // Initialize device information
  for (hipc::Ref<TargetInfo> target : (*mdm_->targets_)) {
    hipc::Ref<DeviceInfo> dev_info =
        (*mdm_->devices_)[target->id_.GetDeviceId()];
    if (dev_info->mount_dir_->size() == 0) {
      dev_info->header_->io_api_ = IoInterface::kRam;
    } else {
      dev_info->header_->io_api_ = IoInterface::kPosix;
    }
    auto io_client = borg::BorgIoClientFactory::Get(dev_info->header_->io_api_);
    io_client->Init(*dev_info);
  }

  // Print out device info
  mdm_->PrintDeviceInfo();

  // Spawn the thread for flushing the bucket map
  auto flush_scheduler = [](void *args) {
    HILOG(kDebug, "Flushing scheduler thread has started")
    (void) args;
    BufferOrganizer *borg = HERMES->borg_.get();
    while (HERMES_THREAD_MANAGER->Alive()) {
      borg->LocalFlush();
      // TODO(llogan): make configurable
      tl::thread::self().sleep(*HERMES->rpc_.server_engine_, 1000);
    }
    HILOG(kDebug, "Flush scheduler thread has stopped")
  };
  HERMES_THREAD_MANAGER->Spawn(flush_scheduler);
}

/**====================================
 * SHM Deserialization
 * ===================================*/

/** Deserialize the BORG from shared memory */
void BufferOrganizer::shm_deserialize_main()  {
  mdm_ = HERMES->mdm_.get();
  rpc_ = &HERMES->rpc_;
}

/**====================================
 * Destructors
 * ===================================*/

/** Finalize the BORG */
void BufferOrganizer::shm_destroy_main() {}

/**====================================
 * BORG Methods
 * ===================================*/

/** Stores a blob into a set of buffers */
RPC void BufferOrganizer::LocalPlaceBlobInBuffers(
    const Blob &blob, std::vector<BufferInfo> &buffers) {
  AUTO_TRACE(1)
  size_t blob_off = 0;
  for (BufferInfo &buffer_info : buffers) {
    if (buffer_info.tid_.GetNodeId() != mdm_->rpc_->node_id_) {
      continue;
    }
    TIMER_START("DeviceInfo")
    hipc::Ref<DeviceInfo> dev_info =
        (*mdm_->devices_)[buffer_info.tid_.GetDeviceId()];
    if (buffer_info.t_off_ + buffer_info.blob_size_ >
        dev_info->header_->capacity_) {
      HELOG(kFatal, "Out of bounds: attempting to write to offset: {} / {} "
            "on device {}: {}",
            buffer_info.t_off_ + buffer_info.blob_size_,
            dev_info->header_->capacity_,
            buffer_info.tid_.GetDeviceId(),
            dev_info->mount_point_->str())
    }
    auto io_client = borg::BorgIoClientFactory::Get(dev_info->header_->io_api_);
    TIMER_END()

    TIMER_START("IO")
    bool ret = io_client->Write(*dev_info,
                                blob.data() + blob_off,
                                buffer_info.t_off_,
                                buffer_info.blob_size_);
    TIMER_END()
    blob_off += buffer_info.blob_size_;
    if (!ret) {
      mdm_->PrintDeviceInfo();
      HELOG(kFatal, "Could not perform I/O in BORG."
            " Reading from target ID:"
            " (node_id: {}, tgt_id: {}, dev_id: {})",
            buffer_info.tid_.GetNodeId(),
            buffer_info.tid_.GetIndex(),
            buffer_info.tid_.GetDeviceId())
    }
  }
}

/** Globally store a blob into a set of buffers */
void BufferOrganizer::GlobalPlaceBlobInBuffers(
    const Blob &blob, std::vector<BufferInfo> &buffers) {
  AUTO_TRACE(1)
  // Get the nodes to transfer buffers to
  size_t total_size;
  auto unique_nodes = BufferPool::GroupByNodeId(buffers, total_size);

  // Send the buffers to each node
  for (auto &[node_id, size] : unique_nodes) {
    if (NODE_ID_IS_LOCAL(node_id)) {
      LocalPlaceBlobInBuffers(blob, buffers);
    } else {
      rpc_->IoCall<void>(
          node_id, "RpcPlaceBlobInBuffers",
          IoType::kWrite, blob.data(), blob.size(),
          blob.size(), buffers);
    }
  }
}

/** Stores a blob into a set of buffers */
RPC void BufferOrganizer::LocalReadBlobFromBuffers(
    Blob &blob, std::vector<BufferInfo> &buffers) {
  AUTO_TRACE(1)
  size_t blob_off = 0;
  for (BufferInfo &buffer_info : buffers) {
    if (buffer_info.tid_.GetNodeId() != mdm_->rpc_->node_id_) {
      continue;
    }
    hipc::Ref<DeviceInfo> dev_info =
        (*mdm_->devices_)[buffer_info.tid_.GetDeviceId()];
    if (buffer_info.t_off_ + buffer_info.blob_size_ >
        dev_info->header_->capacity_) {
      HELOG(kFatal, "Out of bounds: attempting to read from offset: {} / {}"
            " on device {}: {}",
            buffer_info.t_off_ + buffer_info.blob_size_,
            dev_info->header_->capacity_,
            buffer_info.tid_.GetDeviceId(),
            dev_info->mount_point_->str())
    }
    auto io_client = borg::BorgIoClientFactory::Get(dev_info->header_->io_api_);
    bool ret = io_client->Read(*dev_info,
                               blob.data() + blob_off,
                               buffer_info.t_off_,
                               buffer_info.blob_size_);
    blob_off += buffer_info.blob_size_;
    if (!ret) {
      HELOG(kFatal, "Could not perform I/O in BORG");
    }
  }
}

/** The Global form of ReadBLobFromBuffers */
Blob BufferOrganizer::GlobalReadBlobFromBuffers(
    std::vector<BufferInfo> &buffers) {
  AUTO_TRACE(1)
  // Get the nodes to transfer buffers to
  size_t total_size = 0;
  auto unique_nodes = BufferPool::GroupByNodeId(buffers, total_size);

  // Send the buffers to each node
  std::vector<Blob> blobs;
  blobs.reserve(unique_nodes.size());
  for (auto &[node_id, size] : unique_nodes) {
    blobs.emplace_back(size);
    if (NODE_ID_IS_LOCAL(node_id)) {
      LocalReadBlobFromBuffers(blobs.back(), buffers);
    } else {
      rpc_->IoCall<void>(
          node_id, "RpcReadBlobFromBuffers",
          IoType::kRead, blobs.back().data(), size,
          size, buffers);
    }
  }

  // If the blob was only on one node
  if (unique_nodes.size() == 1) {
    return std::move(blobs.back());
  }

  // Merge the blobs at the end
  hapi::Blob blob(total_size);
  for (size_t i = 0; i < unique_nodes.size(); ++i) {
    auto &[node_id, size] = unique_nodes[i];
    auto &tmp_blob = blobs[i];
    size_t tmp_blob_off = 0;
    for (BufferInfo &info : buffers) {
      if (info.tid_.GetNodeId() != node_id) {
        continue;
      }
      memcpy(blob.data() + info.blob_off_,
             tmp_blob.data() + tmp_blob_off,
             info.blob_size_);
      tmp_blob_off += info.blob_size_;
    }
  }
  return blob;
}

/** Re-organize blobs based on a score */
void BufferOrganizer::GlobalOrganizeBlob(const std::string &bucket_name,
                                         const std::string &blob_name,
                                         float score) {
  AUTO_TRACE(1)
  auto bkt = HERMES->GetBucket(bucket_name);
  BlobId blob_id;
  bkt->GetBlobId(blob_name, blob_id);
  float blob_score = bkt->GetBlobScore(blob_id);
  Context ctx;

  HILOG(kDebug, "Changing blob score from: {} to {}", blob_score, score)

  // Skip organizing if below threshold
  if (abs(blob_score - score) < .05) {
    return;
  }

  // Lock the blob to ensure it doesn't get modified
  bkt->LockBlob(blob_id, MdLockType::kExternalWrite);

  // Get the blob
  hapi::Blob blob;
  bkt->Get(blob_id, blob, ctx);

  // Re-emplace the blob with new score
  BlobId tmp_id;
  ctx.blob_score_ = score;
  bkt->Put(blob_name, blob, tmp_id, ctx);

  // Unlock the blob
  bkt->UnlockBlob(blob_id, MdLockType::kExternalWrite);
}

/** Flush all blobs registered in this daemon */
void BufferOrganizer::LocalFlush() {
  mdm_->header_->lock_[kBlobMapLock].ReadLock();
  for (hipc::Ref<hipc::pair<BlobId, BlobInfo>> blob_p : *mdm_->blob_map_) {
    // Get the bucket corresponding to this blob &
    // check if it has a FLUSH trait
    BlobId &blob_id = *blob_p->first_;
    BlobInfo &info = *blob_p->second_;
    TagId &bkt_id = info.header_->tag_id_;
    size_t blob_size = info.header_->blob_size_;
    std::vector<Trait*> traits = HERMES->GetTraits(bkt_id,
                                                   HERMES_TRAIT_FLUSH);
    if (traits.size() == 0) {
      continue;
    }
    // Schedule the blob on an I/O worker thread
    HERMES_BORG_IO_THREAD_MANAGER->Enqueue(bkt_id, blob_id, blob_size,
                                           std::move(traits));
  }
  mdm_->header_->lock_[kBlobMapLock].ReadUnlock();
}

}  // namespace hermes
