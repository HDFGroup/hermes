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

#include "metadata_types.h"
#include "buffer_pool.h"
#include "metadata_manager.h"
#include "hermes.h"

namespace hermes {

void BlobInfo::shm_destroy_main() {
  auto &bpm = HERMES->bpm_;
  auto buffers_std = buffers_->vec();
  bpm->GlobalReleaseBuffers(buffers_std);
  name_.shm_destroy();
  buffers_.shm_destroy();
  tags_.shm_destroy();
}

void TagInfo::shm_destroy_main() {
  auto mdm = HERMES->mdm_.get();
  name_->shm_destroy();
  if (header_->owner_) {
    for (hipc::Ref<BlobId> blob_id : *blobs_) {
      mdm->GlobalDestroyBlob(header_->tag_id_, *blob_id);
    }
  }
  blobs_->shm_destroy();
  traits_->shm_destroy();
}

}  // namespace hermes
