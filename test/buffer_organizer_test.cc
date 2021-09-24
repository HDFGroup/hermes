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

#include <numeric>

#include "hermes.h"
#include "vbucket.h"
#include "metadata_management_internal.h"
#include "buffer_pool_internal.h"
#include "test_utils.h"

#include <mpi.h>

namespace hapi = hermes::api;
using HermesPtr = std::shared_ptr<hapi::Hermes>;

void TestIsBoFunction() {
  using hermes::IsBoFunction;
  Assert(IsBoFunction("BO::TriggerBufferOrganizer"));
  Assert(IsBoFunction("BO::A"));
  Assert(IsBoFunction("BO::"));
  Assert(!IsBoFunction(""));
  Assert(!IsBoFunction("A"));
  Assert(!IsBoFunction("BO:"));
  Assert(!IsBoFunction("BO:A"));
  Assert(!IsBoFunction("TriggerBufferOrganizer"));
}

void TestBoTasks() {
  using hermes::BoTask;
  using hermes::BufferID;
  using hermes::TargetID;

  HermesPtr hermes = hermes::InitHermesDaemon();

  const int kNumThreads = 3;
  for (int i = 0; i < kNumThreads; ++i) {
    BoTask task = {};
    BufferID bid = {};
    TargetID tid = {};
    bid.as_int = i;
    tid.as_int = i;
    if (i == 0) {
      task.op = hermes::BoOperation::kMove;
      task.args.move_args = {bid, tid};
    } else if (i == 1) {
      task.op = hermes::BoOperation::kCopy;
      task.args.copy_args = {bid, tid};
    } else {
      task.op = hermes::BoOperation::kDelete;
      task.args.delete_args = {bid};
    }
    LocalEnqueueBoTask(&hermes->context_, task);
  }

  hermes->Finalize(true);
}
void TestBackgroundFlush() {
  using hermes::u8;
  HermesPtr hermes = hermes::InitHermesDaemon();
  const int io_size = KILOBYTES(4);
  const int iters = 4;
  const int total_bytes = io_size * iters;
  std::string final_destination = "./test_background_flush.out";
  std::string bkt_name = "background_flush";
  hapi::Bucket bkt(bkt_name, hermes);
  hapi::VBucket vbkt(bkt_name, hermes);

  hapi::FileMappingTrait mapping;
  mapping.filename = final_destination;
  hapi::PersistTrait persist_trait(mapping, false);
  vbkt.Attach(&persist_trait);

  std::vector<u8> data(iters);
  std::iota(data.begin(), data.end(), 'a');

  for (int i = 0; i < iters; ++i) {
    hapi::Blob blob(io_size, data[i]);
    std::string blob_name = std::to_string(i);
    bkt.Put(blob_name, blob);

    persist_trait.file_mapping.offset_map.emplace(blob_name, i * io_size);
    vbkt.Link(blob_name, bkt_name);
  }

  vbkt.Destroy();
  bkt.Destroy();

  hermes->Finalize(true);

  // NOTE(chogan): Verify that file is on disk with correct data
  FILE *fh = fopen(final_destination.c_str(), "r");
  Assert(fh);

  std::vector<u8> result(total_bytes, 0);
  size_t bytes_read = fread(result.data(), 1, total_bytes, fh);
  Assert(bytes_read == total_bytes);

  for (int i = 0; i < iters; ++i) {
    for (int j = 0; j < io_size; ++j) {
      int index = i * io_size + j;
      Assert(result[index] == data[i]);
    }
  }

  Assert(fclose(fh) == 0);
  Assert(std::remove(final_destination.c_str()) == 0);
}

void TestBoMove() {
  using hermes::BufferID;
  using hermes::BucketID;
  using hermes::BlobID;
  using hermes::TargetID;
  using hermes::BufferInfo;
  using hermes::f32;

  hermes::Config config = {};
  hermes::InitDefaultConfig(&config);
  config.default_placement_policy = hapi::PlacementPolicy::kRoundRobin;

  HermesPtr hermes = hermes::InitHermesDaemon(&config);
  hermes::SharedMemoryContext *context = &hermes->context_;
  hermes::RpcContext *rpc = &hermes->rpc_;

  std::string bkt_name = "BoMove";
  hapi::Bucket bkt(bkt_name, hermes);

  // Force Blob to RAM tier
  hermes::RoundRobinState rr_state;
  size_t num_devices = rr_state.GetNumDevices();
  rr_state.SetCurrentDeviceIndex(0);

  std::string blob_name = "1";
  hapi::Blob blob(KILOBYTES(20), 'z');
  hapi::Status status = bkt.Put(blob_name, blob);
  Assert(status.Succeeded());

  BucketID bucket_id = {};
  bucket_id.as_int = bkt.GetId();
  BlobID old_blob_id = GetBlobId(context, rpc, blob_name, bucket_id, false);
  Assert(!IsNullBlobId(old_blob_id));
  std::vector<BufferID> old_buffer_ids = GetBufferIdList(context, rpc,
                                                         old_blob_id);
  Assert(old_buffer_ids.size() > 0);
  std::vector<BufferInfo> old_buffer_info = GetBufferInfo(context, rpc,
                                                          old_buffer_ids);
  Assert(old_buffer_info.size() == old_buffer_ids.size());

  f32 old_access_score = ComputeBlobAccessScore(context, old_buffer_info);
  Assert(old_access_score == 1);

  // move ram buffers to lowest tier buffers
  BufferID src = old_buffer_ids[0];
  hermes::BufferHeader *header = GetHeaderByBufferId(context, src);
  Assert(header);

  std::vector<TargetID> targets = LocalGetNodeTargets(context);
  hermes::PlacementSchema schema;
  schema.push_back(std::pair(header->used, targets[targets.size() - 1]));

  std::vector<BufferID> destinations = hermes::GetBuffers(context, schema);
  Assert(destinations.size());

  std::string internal_blob_name = MakeInternalBlobName(blob_name, bucket_id);
  hermes::BoMove(context, rpc, src, destinations, old_blob_id, bucket_id,
                 internal_blob_name);

  BlobID new_blob_id = GetBlobId(context, rpc, blob_name, bucket_id, false);
  Assert(!IsNullBlobId(new_blob_id));
  std::vector<BufferID> new_buffer_ids = GetBufferIdList(context, rpc,
                                                         new_blob_id);
  Assert(new_buffer_ids.size() > 0);
  Assert(new_buffer_ids != old_buffer_ids);
  std::vector<BufferInfo> new_buffer_info = GetBufferInfo(context, rpc,
                                                          new_buffer_ids);
  Assert(new_buffer_info.size() == new_buffer_ids.size());
  Assert(new_buffer_info != old_buffer_info);

  f32 new_access_score = ComputeBlobAccessScore(context, new_buffer_info);
  Assert(new_access_score < old_access_score);

  // move large low tier buffer to ram
  rr_state.SetCurrentDeviceIndex(num_devices - 1);

  hermes->Finalize(true);
}

int main(int argc, char *argv[]) {
  int mpi_threads_provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &mpi_threads_provided);
  if (mpi_threads_provided < MPI_THREAD_MULTIPLE) {
    fprintf(stderr, "Didn't receive appropriate MPI threading specification\n");
    return 1;
  }

  // TestIsBoFunction();
  // TestBoTasks();
  // TestBackgroundFlush();
  TestBoMove();

  MPI_Finalize();

  return 0;
}
