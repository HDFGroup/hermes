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

#include <cstdlib>
#include <string>

#include <mpi.h>
#include <glog/logging.h>
#include "hermes.h"
#include "buffer_pool.h"

#include "basic_test.h"

namespace hapi = hermes::api;

void MainPretest() {
  hapi::Hermes::Create(hermes::HermesType::kClient);
}

void MainPosttest() {
  HERMES->Finalize();
}

TEST_CASE("TestBufferPool") {
  std::vector<std::pair<size_t, int>> sizes = {
    {23, 1},   // 23B blob on 1 target
    {KILOBYTES(1), 1},   // 1KB blob on 1 target
    {KILOBYTES(4), 1},   // 4KB blob on 1 target
    {KILOBYTES(8), 1},   // 8KB blob on 1 target
    {KILOBYTES(16), 1},  // 16KB blob on 1 target
    {KILOBYTES(64), 1},  // 64KB blob on 1 target
    {MEGABYTES(4), 1},  // 4MB blob on 1 target

    {KILOBYTES(4), 3},   // 4KB blob on 3 targets
    {KILOBYTES(8), 3},   // 8KB blob on 3 targets
    {KILOBYTES(16), 3},  // 16KB blob on 3 targets
    {KILOBYTES(64), 3},  // 64KB blob on 3 targets
    {MEGABYTES(4), 3},  // 4MB blob on 3 targets
    {MEGABYTES(4) + KILOBYTES(32), 3},  // 4MB blob on 3 targets
  };

  for (auto &[size, num_targets] : sizes) {
    // Divide the blob among targets
    hermes::PlacementSchema schema;
    size_t tgt_size = size / num_targets;
    size_t last_size = tgt_size + (size % num_targets);
    for (int i = 0; i < num_targets; ++i) {
      hipc::Ref<hermes::TargetInfo> target = (*HERMES->mdm_->targets_)[i];
      if (i < num_targets - 1) {
        schema.plcmnts_.emplace_back(tgt_size, target->id_);
      } else {
        schema.plcmnts_.emplace_back(last_size, target->id_);
      }
    }

    // Create the blob to write
    hermes::Blob write_blob(size);
    for (size_t i = 0; i < size; ++i) {
      write_blob.data()[i] = i;
    }

    // Allocate the buffers and set them
    std::vector<hermes::BufferInfo> buffers =
        HERMES->bpm_->LocalAllocateAndSetBuffers(schema, write_blob);

    // Read back the buffers
    hermes::Blob read_blob =
        HERMES->borg_->LocalReadBlobFromBuffers(buffers);

    // Verify they are the same
    REQUIRE(read_blob == write_blob);
  }
}

