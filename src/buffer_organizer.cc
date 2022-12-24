//
// Created by lukemartinlogan on 12/19/22.
//

#include "buffer_organizer.h"
#include "metadata_manager.h"
#include "io_clients/io_client_factory.h"
#include "hermes.h"

namespace hermes {

/**
 * Initialize the BORG
 * REQUIRES mdm to be initialized already.
 * */
void BufferOrganizer::shm_init() {
  mdm_ = &HERMES->mdm_;
  for (auto &target : (*mdm_->targets_)) {
    auto &dev_info = (*mdm_->devices_)[target.id_.GetDeviceId()];
    if (dev_info.mount_point_.size() == 0) {
      dev_info.io_api_ = IoInterface::kRam;
    } else {
      dev_info.io_api_ = IoInterface::kPosix;
    }
    auto io_client = IoClientFactory::Get(dev_info.io_api_);
    io_client->Init(dev_info);
  }
}

/** Finalize the BORG */
void BufferOrganizer::shm_destroy() {}

/** Stores a blob into a set of buffers */
RPC void BufferOrganizer::LocalPlaceBlobInBuffers(
    Blob &blob, lipc::vector<BufferInfo> &buffers) {
  for (auto &buffer_info : buffers) {
    auto &dev_info = (*mdm_->devices_)[buffer_info.tid_.GetDeviceId()];
    auto io_client = IoClientFactory::Get(dev_info.io_api_);
    bool ret = io_client->Write(dev_info, blob.data(),
                                buffer_info.off_, buffer_info.size_);
    if (!ret) {
      LOG(FATAL) << "Could not perform I/O in BORG" << std::endl;
    }
  }
}

/** Stores a blob into a set of buffers */
RPC void LocalReadBlobFromBuffers(Blob &blob,
                                  lipc::vector<BufferInfo> &buffers) {
}

/** Copies one buffer set into another buffer set */
RPC void LocalCopyBuffers(lipc::vector<BufferInfo> &dst,
                          lipc::vector<BufferInfo> &src) {
}

}  // namespace hermes