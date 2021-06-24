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

#include "hermes.h"
#include "buffer_organizer.h"
#include "data_placement_engine.h"

namespace hermes {

void BoMove(void*) {
  printf("%s\n", __func__);
}

void BoCopy(void*) {
  printf("%s\n", __func__);
}

void BoDelete(void*) {
  printf("%s\n", __func__);
}

bool LocalEnqueueBoTask(SharedMemoryContext *context, BoTask task,
                        BoPriority priority) {
  (void)task;
  (void)context;
  (void)priority;
  bool result = false;
#if 0
  switch (task.op) {
    case BoOperation::kMove: {
      // TODO(chogan): Task memory
      hermes::BoTask task = {};
      task.op = hermes::BoOperation::kMove;
      task.args.move_args = {};
      ABT_thread_create(bo->pools[(int)priority], BoMove, (void *)&task,
                        // TODO(chogan): Which thread?
                        ABT_THREAD_ATTR_NULL, &bo->threads[i]);
      break;
    }
    case BoOperation::kCopy: {
      // TODO(chogan):
      break;
    }
    case BoOperation::kDelete: {
      // TODO(chogan):
      break;
    }
    default: {
      HERMES_INVALID_CODE_PATH;
    }
  }
#endif
  return result;
}

Status PlaceInHierarchy(SharedMemoryContext *context, RpcContext *rpc,
                        SwapBlob swap_blob, const std::string &name,
                        const api::Context &ctx) {
  std::vector<PlacementSchema> schemas;
  std::vector<size_t> sizes(1, swap_blob.size);
  Status result = CalculatePlacement(context, rpc, sizes, schemas, ctx);

  if (result.Succeeded()) {
    std::vector<u8> blob_mem(swap_blob.size);
    Blob blob = {};
    blob.data = blob_mem.data();
    blob.size = blob_mem.size();
    ReadFromSwap(context, blob, swap_blob);
    result = PlaceBlob(context, rpc, schemas[0], blob, name,
                       swap_blob.bucket_id, ctx, true);
  } else {
    LOG(ERROR) << result.Msg();
  }

  return result;
}

int MoveToTarget(SharedMemoryContext *context, RpcContext *rpc, BlobID blob_id,
                 TargetID dest) {
  (void)(context);
  (void)(rpc);
  (void)(blob_id);
  (void)(dest);
// TODO(chogan): Move blob from current location to Target dest
  HERMES_NOT_IMPLEMENTED_YET;
  int result = 0;
  return result;
}

}  // namespace hermes
