//
// Created by lukemartinlogan on 12/19/22.
//

#include "buffer_organizer.h"
#include "metadata_manager.h"
#include "io_clients/io_client_factory.h"
#include "hermes.h"

namespace hermes {

static size_t SumBufferBlobSizes(lipc::vector<BufferInfo> &buffers) {
  size_t sum = 0;
  for (lipc::Ref<BufferInfo> buffer_ref : buffers) {
    sum += (*buffer_ref).blob_size_;
  }
  return sum;
}

/**
 * Initialize the BORG
 * REQUIRES mdm to be initialized already.
 * */
void BufferOrganizer::shm_init() {
  mdm_ = &HERMES->mdm_;
  for (lipc::Ref<TargetInfo> target_ref : (*mdm_->targets_)) {
    auto &target = *target_ref;
    DeviceInfo &dev_info = *(*mdm_->devices_)[target.id_.GetDeviceId()];
    if (dev_info.mount_dir_.size() == 0) {
      dev_info.header_->io_api_ = IoInterface::kRam;
    } else {
      dev_info.header_->io_api_ = IoInterface::kPosix;
    }
    auto io_client = IoClientFactory::Get(dev_info.header_->io_api_);
    io_client->Init(dev_info);
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
    Blob &blob, lipc::vector<BufferInfo> &buffers) {
  size_t blob_off = 0;
  for (lipc::Ref<BufferInfo> buffer_info_ref : buffers) {
    BufferInfo &buffer_info = *buffer_info_ref;
    if (buffer_info.tid_.GetNodeId() != mdm_->rpc_->node_id_) {
      continue;
    }
    DeviceInfo &dev_info = *(*mdm_->devices_)[buffer_info.tid_.GetDeviceId()];
    auto io_client = IoClientFactory::Get(dev_info.header_->io_api_);
    bool ret = io_client->Write(dev_info, blob.data() + blob_off,
                                buffer_info.t_off_,
                                buffer_info.blob_size_);
    blob_off += buffer_info.blob_size_;
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
  for (lipc::Ref<BufferInfo> buffer_info_ref : buffers) {
    BufferInfo &buffer_info = *buffer_info_ref;
    if (buffer_info.tid_.GetNodeId() != mdm_->rpc_->node_id_) {
      continue;
    }
    auto dev_info_ref = (*mdm_->devices_)[buffer_info.tid_.GetDeviceId()];
    DeviceInfo &dev_info = *dev_info_ref;
    auto io_client = IoClientFactory::Get(dev_info.header_->io_api_);
    bool ret = io_client->Read(dev_info, blob.data_mutable() + blob_off,
                                buffer_info.t_off_,
                                buffer_info.blob_size_);
    blob_off += buffer_info.blob_size_;
    if (!ret) {
      LOG(FATAL) << "Could not perform I/O in BORG" << std::endl;
    }
  }
  return std::move(blob);
}

/** Copies one buffer set into another buffer set */
RPC void BufferOrganizer::LocalCopyBuffers(lipc::vector<BufferInfo> &dst,
                                           lipc::vector<BufferInfo> &src) {
}

}  // namespace hermes