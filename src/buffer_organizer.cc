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

namespace hermes {

/**
 * Initialize the BORG
 * REQUIRES mdm to be initialized already.
 * */
void BufferOrganizer::shm_init() {
  mdm_ = &(*HERMES->mdm_);
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
}

/** Finalize the BORG */
void BufferOrganizer::shm_destroy() {}

/** Deserialize the BORG from shared memory */
void BufferOrganizer::shm_deserialize()  {
  mdm_ = &(*HERMES->mdm_);
  rpc_ = &HERMES->rpc_;
}

/** Stores a blob into a set of buffers */
RPC void BufferOrganizer::LocalPlaceBlobInBuffers(
    const Blob &blob, std::vector<BufferInfo> &buffers) {
  size_t blob_off = 0;
  for (BufferInfo &buffer_info : buffers) {
    if (buffer_info.tid_.GetNodeId() != mdm_->rpc_->node_id_) {
      continue;
    }
    hipc::Ref<DeviceInfo> dev_info =
        (*mdm_->devices_)[buffer_info.tid_.GetDeviceId()];
    auto io_client = borg::BorgIoClientFactory::Get(dev_info->header_->io_api_);
    bool ret = io_client->Write(*dev_info,
                                blob.data() + blob_off,
                                buffer_info.t_off_,
                                buffer_info.blob_size_);
    blob_off += buffer_info.blob_size_;
    if (!ret) {
      LOG(FATAL) << "Could not perform I/O in BORG" << std::endl;
    }
  }
}

/** Globally store a blob into a set of buffers */
void BufferOrganizer::GlobalPlaceBlobInBuffers(
    const Blob &blob, std::vector<BufferInfo> &buffers) {
  // Get the nodes to transfer buffers to
  size_t total_size;
  auto unique_tgts = GroupByTarget(buffers, total_size);

  // Send the buffers to each node
  for (auto &[tid, size] : unique_tgts) {
    if (NODE_ID_IS_LOCAL(tid.GetNodeId())) {
      LocalPlaceBlobInBuffers(blob, buffers);
    } else {
      rpc_->IoCall<void>(
          tid.GetNodeId(), "RpcPlaceBlobInBuffers",
          IoType::kWrite, blob.data(), blob.size(),
          blob.size(), buffers);
    }
  }
}

/** Stores a blob into a set of buffers */
RPC Blob BufferOrganizer::LocalReadBlobFromBuffers(
    std::vector<BufferInfo> &buffers) {
  Blob blob(SumBufferBlobSizes(buffers));
  size_t blob_off = 0;
  for (BufferInfo &buffer_info : buffers) {
    if (buffer_info.tid_.GetNodeId() != mdm_->rpc_->node_id_) {
      continue;
    }
    hipc::Ref<DeviceInfo> dev_info =
        (*mdm_->devices_)[buffer_info.tid_.GetDeviceId()];
    auto io_client = borg::BorgIoClientFactory::Get(dev_info->header_->io_api_);
    bool ret = io_client->Read(*dev_info, blob.data() + blob_off,
                                buffer_info.t_off_,
                                buffer_info.blob_size_);
    blob_off += buffer_info.blob_size_;
    if (!ret) {
      LOG(FATAL) << "Could not perform I/O in BORG" << std::endl;
    }
  }
  return blob;
}

/** The Global form of ReadBLobFromBuffers */
Blob BufferOrganizer::GlobalReadBlobFromBuffers(
    std::vector<BufferInfo> &buffers) {
  // Get the nodes to transfer buffers to
  size_t total_size = 0;
  auto unique_tgts = GroupByTarget(buffers, total_size);

  // Send the buffers to each node
  std::vector<Blob> blobs;
  for (auto &[tid, size] : unique_tgts) {
    blobs.emplace_back(size);
    if (NODE_ID_IS_LOCAL(tid.GetNodeId())) {
      LocalReadBlobFromBuffers(buffers);
    } else {
      rpc_->IoCall<void>(
          tid.GetNodeId(), "RpcReadBlobFromBuffers",
          IoType::kRead, blobs.back().data(), size,
          buffers);
    }
  }

  // Merge the blobs at the end
  hapi::Blob blob(total_size);
  for (size_t i = 0; i < unique_tgts.size(); ++i) {
    auto &[tid, size] = unique_tgts[i];
    auto &tmp_blob = blobs[i];
    size_t tmp_blob_off = 0;
    for (BufferInfo &info : buffers) {
      if (info.tid_.GetNodeId() != tid.GetNodeId()) {
        continue;
      }
      memcpy(blob.data() + info.blob_off_,
             tmp_blob.data(), info.blob_size_);
      tmp_blob_off += info.blob_size_;
    }
  }
  return blob;
}

/** Copies one buffer set into another buffer set */
RPC void BufferOrganizer::LocalCopyBuffers(std::vector<BufferInfo> &dst,
                                           std::vector<BufferInfo> &src) {
}

}  // namespace hermes
