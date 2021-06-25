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

void BoMove(SharedMemoryContext *context, BufferID src, TargetID dest) {
  (void)context;
  printf("%s(%d, %d)\n", __func__, (int)src.as_int, (int)dest.as_int);
}

void BoCopy(SharedMemoryContext *context, BufferID src, TargetID dest) {
  (void)context;
  printf("%s(%d, %d)\n", __func__, (int)src.as_int, (int)dest.as_int);
}

void BoDelete(SharedMemoryContext *context, BufferID src) {
  (void)context;
  printf("%s(%d)\n", __func__, (int)src.as_int);
}

bool LocalEnqueueBoTask(SharedMemoryContext *context, const ThreadPool &pool,
                        BoTask task, BoPriority priority) {
  // TODO(chogan): Limit queue size and return false when full
  bool result = true;
  bool is_high_priority = priority == BoPriority::kHigh;

  switch (task.op) {
    case BoOperation::kMove: {
      pool.run(std::bind(BoMove, context, task.args.move_args.src,
                 task.args.move_args.dest), is_high_priority);
      break;
    }
    case BoOperation::kCopy: {
      pool.run(std::bind(BoCopy, context, task.args.copy_args.src,
                 task.args.copy_args.dest), is_high_priority);
      break;
    }
    case BoOperation::kDelete: {
      pool.run(std::bind(BoDelete, context, task.args.delete_args.src),
               is_high_priority);
      break;
    }
    default: {
      HERMES_INVALID_CODE_PATH;
    }
  }

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
