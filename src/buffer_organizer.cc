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
      LOG(FATAL) << hshm::Formatter::format(
                        "Out of bounds: attempting to write to offset: {} / {} "
                        "on device {}: {}",
                        buffer_info.t_off_ + buffer_info.blob_size_,
                        dev_info->header_->capacity_,
                        buffer_info.tid_.GetDeviceId(),
                        dev_info->mount_point_->str()) << std::endl;
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
      LOG(FATAL) << hshm::Formatter::format(
          "Could not perform I/O in BORG."
          " Reading from target ID:"
          " (node_id: {}, tgt_id: {}, dev_id: {})",
          buffer_info.tid_.GetNodeId(),
          buffer_info.tid_.GetIndex(),
          buffer_info.tid_.GetDeviceId()) << std::endl;
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
      LOG(FATAL) << hshm::Formatter::format(
                        "Out of bounds: attempting to read from offset: {} / {}"
                        " on device {}: {}",
                        buffer_info.t_off_ + buffer_info.blob_size_,
                        dev_info->header_->capacity_,
                        buffer_info.tid_.GetDeviceId(),
                        dev_info->mount_point_->str()) << std::endl;
    }
    auto io_client = borg::BorgIoClientFactory::Get(dev_info->header_->io_api_);
    bool ret = io_client->Read(*dev_info,
                               blob.data() + blob_off,
                               buffer_info.t_off_,
                               buffer_info.blob_size_);
    blob_off += buffer_info.blob_size_;
    if (!ret) {
      LOG(FATAL) << "Could not perform I/O in BORG" << std::endl;
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

  VLOG(kDebug) << "Changing blob score from: " << blob_score
            << " to: " << score << std::endl;

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

}  // namespace hermes
