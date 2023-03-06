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

#include "rpc_thallium.h"
#include "metadata_manager.h"
#include "rpc_thallium_serialization.h"
#include "data_structures.h"

#include "hermes.h"

namespace hermes {

using thallium::request;

void ThalliumRpc::DefineRpcs() {
  RPC_CLASS_INSTANCE_DEFS_START
  MetadataManager *mdm = this->mdm_;
  BufferPool *bpm = &(*HERMES->bpm_);
  BufferOrganizer *borg = &HERMES->borg_;
  RPC_CLASS_INSTANCE_DEFS_END

  /*RPC_AUTOGEN_START*/
  RegisterRpc("RpcGetOrCreateBucket", [mdm](const request &req,
                                            hipc::charbuf &bkt_name,
                                            const IoClientContext &opts) {
    auto ret = mdm->LocalGetOrCreateBucket(bkt_name, opts);
    req.respond(ret);
  });
  RegisterRpc("RpcGetBucketId", [mdm](const request &req,
                                        hipc::charbuf &bkt_name) {
    auto ret = mdm->LocalGetBucketId(bkt_name);
    req.respond(ret);
  });
  RegisterRpc("RpcRpcGetBucketSize", [mdm](const request &req,
                                           BucketId bkt_id,
                                           const IoClientContext &opts) {
    auto ret = mdm->LocalGetBucketSize(bkt_id, opts);
    req.respond(ret);
  });
  RegisterRpc("RpcLockBucket", [mdm](const request &req,
                                     BucketId bkt_id,
                                     MdLockType lock_type) {
    mdm->LocalLockBucket(bkt_id, lock_type);
    req.respond(true);
  });
  RegisterRpc("RpcUnlockBucket", [mdm](const request &req,
                                       BucketId bkt_id,
                                       MdLockType lock_type) {
    mdm->LocalUnlockBucket(bkt_id, lock_type);
    req.respond(true);
  });
  RegisterRpc("RpcBucketContainsBlob", [mdm](const request &req,
                                             BucketId bkt_id,
                                             BlobId blob_id) {
    auto ret = mdm->LocalBucketContainsBlob(bkt_id, blob_id);
    req.respond(ret);
  });
  RegisterRpc("RpcBucketGetContainedBlobIds", [mdm](const request &req,
                                                    BucketId bkt_id) {
    auto ret = mdm->LocalBucketGetContainedBlobIds(bkt_id);
    req.respond(ret);
  });
  RegisterRpc("RpcRenameBucket", [mdm](const request &req,
                                       BucketId bkt_id,
                                       hipc::charbuf &new_bkt_name) {
    auto ret = mdm->LocalRenameBucket(bkt_id, new_bkt_name);
    req.respond(ret);
  });
  RegisterRpc("RpcRpcDestroyBucket", [mdm](const request &req,
                                           BucketId bkt_id) {
    auto ret = mdm->LocalDestroyBucket(bkt_id);
    req.respond(ret);
  });
  RegisterRpc("RpcBucketPutBlob", [mdm](const request &req,
                                        BucketId bkt_id,
                                        const hipc::charbuf &blob_name,
                                        size_t blob_size,
                                        hipc::vector<BufferInfo> &buffers) {
    auto ret = mdm->LocalBucketPutBlob(bkt_id, blob_name, blob_size, buffers);
    req.respond(ret);
  });
  RegisterRpc("RpcBucketRegisterBlobId", [mdm](const request &req,
                                               BucketId bkt_id,
                                               BlobId blob_id,
                                               size_t orig_blob_size,
                                               size_t new_blob_size,
                                               bool did_create,
                                               const IoClientContext &opts) {
    auto ret = mdm->LocalBucketRegisterBlobId(bkt_id, blob_id,
                                   orig_blob_size, new_blob_size,
                                   did_create, opts);
    req.respond(ret);
  });
  RegisterRpc("RpcBucketUnregisterBlobId", [mdm](const request &req,
                                                 BucketId bkt_id,
                                                 BlobId blob_id,
                                                 const IoClientContext &opts) {
    auto ret = mdm->LocalBucketUnregisterBlobId(bkt_id, blob_id, opts);
    req.respond(ret);
  });
  RegisterRpc("RpcBucketTryCreateBlob", [mdm](const request &req,
                                              BucketId bkt_id,
                                              const hipc::charbuf &blob_name) {
    auto ret = mdm->LocalBucketTryCreateBlob(bkt_id, blob_name);
    req.respond(ret);
  });
  RegisterRpc("RpcBucketGetBlob", [mdm](const request &req,
                                        BlobId blob_id) {
    auto ret = mdm->LocalBucketGetBlob(blob_id);
    req.respond(ret);
  });
  RegisterRpc("RpcGetBlobId", [mdm](const request &req,
                                    BucketId bkt_id,
                                    const hipc::charbuf &blob_name) {
    auto ret = mdm->LocalGetBlobId(bkt_id, blob_name);
    req.respond(ret);
  });
  RegisterRpc("RpcGetBlobName", [mdm](const request &req,
                                      BlobId blob_id) {
    auto ret = mdm->LocalGetBlobName(blob_id);
    req.respond(ret);
  });
  RegisterRpc("RpcLockBlob", [mdm](const request &req,
                                   BlobId blob_id,
                                   MdLockType lock_type) {
    auto ret = mdm->LocalLockBlob(blob_id, lock_type);
    req.respond(ret);
  });
  RegisterRpc("RpcUnlockBlob", [mdm](const request &req,
                                     BlobId blob_id,
                                     MdLockType lock_type) {
    auto ret = mdm->LocalUnlockBlob(blob_id, lock_type);
    req.respond(ret);
  });
  RegisterRpc("RpcGetBlobBuffers", [mdm](const request &req,
                                         BlobId blob_id) {
    auto ret = mdm->LocalGetBlobBuffers(blob_id);
    req.respond(ret);
  });
  RegisterRpc("RpcRenameBlob", [mdm](const request &req,
                                     BucketId bkt_id,
                                     BlobId blob_id,
                                     hipc::charbuf &new_blob_name) {
    auto ret = mdm->LocalRenameBlob(bkt_id, blob_id, new_blob_name);
    req.respond(ret);
  });
  RegisterRpc("RpcDestroyBlob", [mdm](const request &req,
                                      BucketId bkt_id,
                                      BlobId blob_id) {
    auto ret = mdm->LocalDestroyBlob(bkt_id, blob_id);
    req.respond(ret);
  });
  RegisterRpc("RpcUpdateTargetCapacity", [mdm](const request &req,
                                               TargetId tid,
                                               off64_t offset) {
    mdm->LocalUpdateTargetCapacity(tid, offset);
    req.respond(true);
  });

  // IO Calls
  RegisterRpc("RpcPlaceBlobInBuffers", [this, borg](
                                           const request &req,
                                           tl::bulk &bulk,
                                           size_t blob_size,
                                           hipc::vector<BufferInfo> &buffers) {
    hapi::Blob blob(blob_size);
    this->IoCallServer(req, bulk, IoType::kWrite, blob.data(), blob.size());
    borg->LocalPlaceBlobInBuffers(blob, buffers);
    req.respond(true);
  });
  RegisterRpc("RpcReadBlobFromBuffers", [this, borg](
                                           const request &req,
                                           tl::bulk &bulk,
                                           hipc::vector<BufferInfo> &buffers) {
    hapi::Blob blob;
    blob = borg->LocalReadBlobFromBuffers(buffers);
    this->IoCallServer(req, bulk, IoType::kRead, blob.data(), blob.size());
    req.respond(true);
  });
  RegisterRpc("RpcAllocateAndSetBuffers", [this, bpm](
                                           const request &req,
                                           tl::bulk &bulk,
                                           size_t blob_size,
                                           PlacementSchema &schema) {
    hapi::Blob blob(blob_size);
    this->IoCallServer(req, bulk, IoType::kWrite, blob.data(), blob.size());
    auto ret = bpm->LocalAllocateAndSetBuffers(schema, blob);
    req.respond(ret);
  });
  RPC_AUTOGEN_END
}

}  // namespace hermes
