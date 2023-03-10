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
  auto buffers_std = buffers_->vec();
  bpm->GlobalReleaseBuffers(buffers_std);
  name_->shm_destroy();
  buffers_->shm_destroy();
  tags_->shm_destroy();
}

void TagInfo::shm_destroy_main() {
  auto mdm = &HERMES->mdm_;
  name_->shm_destroy();
  if (header_->owner_) {
    for (hipc::ShmRef<BlobId> blob_id : *blobs_) {
      mdm->GlobalDestroyBlob(header_->tag_id_, *blob_id);
    }
  }
  blobs_->shm_destroy();
}

}  // namespace hermes