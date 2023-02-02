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
  mdm_ = &HERMES->mdm_;
  for (lipc::ShmRef<TargetInfo> target : (*mdm_->targets_)) {
    lipc::ShmRef<DeviceInfo> dev_info =
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

/** Serialize the BORG into shared memory */
void BufferOrganizer::shm_serialize()  {
}

/** Deserialize the BORG from shared memory */
void BufferOrganizer::shm_deserialize()  {
  mdm_ = &HERMES->mdm_;
}

/** Stores a blob into a set of buffers */
RPC void BufferOrganizer::LocalPlaceBlobInBuffers(
    const Blob &blob, lipc::vector<BufferInfo> &buffers) {
  size_t blob_off = 0;
  for (lipc::ShmRef<BufferInfo> buffer_info : buffers) {
    if (buffer_info->tid_.GetNodeId() != mdm_->rpc_->node_id_) {
      continue;
    }
    lipc::ShmRef<DeviceInfo> dev_info =
        (*mdm_->devices_)[buffer_info->tid_.GetDeviceId()];
    auto io_client = borg::BorgIoClientFactory::Get(dev_info->header_->io_api_);
    bool ret = io_client->Write(*dev_info,
                                blob.data() + blob_off,
                                buffer_info->t_off_,
                                buffer_info->blob_size_);
    blob_off += buffer_info->blob_size_;
    if (!ret) {
      LOG(FATAL) << "Could not perform I/O in BORG" << std::endl;
    }
  }
}

/** Stores a blob into a set of buffers */
RPC Blob BufferOrganizer::LocalReadBlobFromBuffers(
    lipc::vector<BufferInfo> &buffers) {
  Blob blob(SumBufferBlobSizes(buffers));
  size_t blob_off = 0;
  for (lipc::ShmRef<BufferInfo> buffer_info : buffers) {
    if (buffer_info->tid_.GetNodeId() != mdm_->rpc_->node_id_) {
      continue;
    }
    lipc::ShmRef<DeviceInfo> dev_info =
        (*mdm_->devices_)[buffer_info->tid_.GetDeviceId()];
    auto io_client = borg::BorgIoClientFactory::Get(dev_info->header_->io_api_);
    bool ret = io_client->Read(*dev_info, blob.data() + blob_off,
                                buffer_info->t_off_,
                                buffer_info->blob_size_);
    blob_off += buffer_info->blob_size_;
    if (!ret) {
      LOG(FATAL) << "Could not perform I/O in BORG" << std::endl;
    }
  }
  return blob;
}

/** Copies one buffer set into another buffer set */
RPC void BufferOrganizer::LocalCopyBuffers(lipc::vector<BufferInfo> &dst,
                                           lipc::vector<BufferInfo> &src) {
}

}  // namespace hermes
