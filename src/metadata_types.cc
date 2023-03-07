//
// Created by lukemartinlogan on 3/7/23.
//

#include "metadata_types.h"
#include "buffer_pool.h"
#include "metadata_manager.h"
#include "hermes.h"

namespace hermes {

void BlobInfo::shm_destroy_main() {
  auto &bpm = HERMES->bpm_;
  bpm->GlobalReleaseBuffers(*buffers_);
  name_->shm_destroy();
  buffers_->shm_destroy();
  tags_->shm_destroy();
}

void BucketInfo::shm_destroy_main() {
  auto mdm = &HERMES->mdm_;
  name_->shm_destroy();
  for (hipc::ShmRef<BlobId> blob_id : *blobs_) {
    mdm->GlobalDestroyBlob(header_->bkt_id_, *blob_id);
  }
  blobs_->shm_destroy();
}

}  // namespace hermes