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
  MetadataManager *mdm = HERMES->mdm_.get();
  BufferPool *bpm = HERMES->bpm_.get();
  BufferOrganizer *borg = HERMES->borg_.get();
  RPC_CLASS_INSTANCE_DEFS_END

  /**====================================
   * Bucket Operations
   * ===================================*/

  RegisterRpc("RpcGetBucketSize", [mdm](const request &req,
                                           TagId bkt_id,
                                           bool backend) {
    auto ret = mdm->LocalGetBucketSize(bkt_id, backend);
    req.respond(ret);
  });
  RegisterRpc("RpcUpdateBucketSize", [mdm](const request &req,
                                           TagId bkt_id,
                                           ssize_t delta,
                                           BucketUpdate mode) {
    auto ret = mdm->LocalUpdateBucketSize(bkt_id, delta, mode);
    req.respond(ret);
  });
  RegisterRpc("RpcClearBucket", [mdm](const request &req,
                                      TagId bkt_id,
                                      bool backend) {
    auto ret = mdm->LocalClearBucket(bkt_id, backend);
    req.respond(ret);
  });

  /**====================================
   * Blob Operations
   * ===================================*/

  RegisterRpc("RpcPutBlobMetadata", [mdm](const request &req,
                                          TagId bkt_id,
                                          const std::string &blob_name,
                                          size_t blob_size,
                                          std::vector<BufferInfo> &buffers,
                                          float score) {
    auto ret = mdm->LocalPutBlobMetadata(bkt_id, blob_name,
                                         blob_size, buffers, score);
    req.respond(ret);
  });
  RegisterRpc("RpcTryCreateBlob", [mdm](const request &req,
                                        TagId bkt_id,
                                        const std::string &blob_name) {
    auto ret = mdm->LocalTryCreateBlob(bkt_id, blob_name);
    req.respond(ret);
  });
  RegisterRpc("RpcTagBlob", [mdm](const request &req,
                                  BlobId blob_id,
                                  TagId tag_id) {
    auto ret = mdm->LocalTagBlob(blob_id, tag_id);
    req.respond(ret);
  });
  RegisterRpc("RpcBlobHasTag", [mdm](const request &req,
                                     BlobId blob_id,
                                     TagId tag_id) {
    auto ret = mdm->LocalBlobHasTag(blob_id, tag_id);
    req.respond(ret);
  });
  RegisterRpc("RpcGetBlobId", [mdm](const request &req,
                                    TagId bkt_id,
                                    const std::string &blob_name) {
    auto ret = mdm->LocalGetBlobId(bkt_id, blob_name);
    req.respond(ret);
  });
  RegisterRpc("RpcGetBlobName", [mdm](const request &req,
                                      BlobId blob_id) {
    auto ret = mdm->LocalGetBlobName(blob_id);
    req.respond(ret);
  });
  RegisterRpc("RpcGetBlobScore", [mdm](const request &req,
                                      BlobId blob_id) {
    auto ret = mdm->LocalGetBlobScore(blob_id);
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
                                     TagId bkt_id,
                                     BlobId blob_id,
                                     const std::string &new_blob_name) {
    auto ret = mdm->LocalRenameBlob(bkt_id, blob_id, new_blob_name);
    req.respond(ret);
  });
  RegisterRpc("RpcDestroyBlob", [mdm](const request &req,
                                      TagId bkt_id,
                                      BlobId blob_id) {
    auto ret = mdm->LocalDestroyBlob(bkt_id, blob_id);
    req.respond(ret);
  });

  /**====================================
   * Target Operations
   * ===================================*/

  RegisterRpc("RpcUpdateTargetCapacity", [mdm](const request &req,
                                               TargetId tid,
                                               off64_t offset) {
    mdm->LocalUpdateTargetCapacity(tid, offset);
    req.respond(true);
  });
  RegisterRpc("RpcClear", [mdm](const request &req) {
    mdm->LocalClear();
    req.respond(true);
  });

  /**====================================
   * Tag Operations
   * ===================================*/

  RegisterRpc("RpcGetOrCreateTag", [mdm](const request &req,
                                         const std::string &tag_name,
                                         bool owner,
                                         std::vector<TraitId> &traits,
                                         size_t backend_size) {
    auto ret = mdm->LocalGetOrCreateTag(tag_name, owner, traits, backend_size);
    req.respond(ret);
  });
  RegisterRpc("RpcGetTagId", [mdm](const request &req,
                                   std::string &tag_name) {
    auto ret = mdm->LocalGetTagId(tag_name);
    req.respond(ret);
  });
  RegisterRpc("RpcRenameTag", [mdm](const request &req,
                                    TagId tag,
                                    const std::string &new_name) {
    mdm->LocalRenameTag(tag, new_name);
    req.respond(true);
  });
  RegisterRpc("RpcDestroyTag", [mdm](const request &req,
                                     TagId tag) {
    mdm->LocalDestroyTag(tag);
    req.respond(true);
  });
  RegisterRpc("RpcTagAddBlob", [mdm](const request &req,
                                     TagId tag_id,
                                     BlobId blob_id) {
    auto ret = mdm->LocalTagAddBlob(tag_id, blob_id);
    req.respond(ret);
  });
  RegisterRpc("RpcTagRemoveBlob", [mdm](const request &req,
                                        TagId tag_id,
                                        BlobId blob_id) {
    auto ret = mdm->LocalTagRemoveBlob(tag_id, blob_id);
    req.respond(ret);
  });
  RegisterRpc("RpcGroupByTag", [mdm](const request &req,
                                     TagId tag_id) {
    auto ret = mdm->LocalGroupByTag(tag_id);
    req.respond(ret);
  });
  RegisterRpc("RpcTagAddTrait", [mdm](const request &req,
                                      TagId tag_id,
                                      TraitId trait_id) {
    mdm->LocalTagAddTrait(tag_id, trait_id);
    req.respond(true);
  });
  RegisterRpc("RpcTagGetTraits", [mdm](const request &req,
                                      TagId tag_id) {
    auto ret = mdm->LocalTagGetTraits(tag_id);
    req.respond(ret);
  });

  /**====================================
   * Trait Operations
   * ===================================*/

  RegisterRpc("RpcRegisterTrait", [mdm](const request &req,
                                        TraitId trait_id,
                                        const std::string &trait_uuid,
                                        hshm::charbuf &trait_params) {
    auto ret = mdm->LocalRegisterTrait(trait_id, trait_uuid, trait_params);
    req.respond(ret);
  });
  RegisterRpc("RpcGetTraitInfo", [mdm](const request &req,
                                       const std::string &trait_uuid) {
    auto ret = mdm->LocalGetTraitInfo(trait_uuid);
    req.respond(ret);
  });

  /**====================================
   * Buffer Pool Operations
   * ===================================*/

  RegisterRpc("RpcReleaseBuffers", [bpm](const request &req,
                                         std::vector<BufferInfo> &buffers) {
    bpm->LocalReleaseBuffers(buffers);
    req.respond(true);
  });
  RegisterRpc("RpcAllocateAndSetBuffers", [this, bpm](
                                              const request &req,
                                              tl::bulk &bulk,
                                              size_t blob_size,
                                              PlacementSchema &schema) {
    hapi::Blob blob(blob_size);
    this->IoCallServer(req, bulk, IoType::kWrite, blob.data(),
                       blob.size());
    auto ret = bpm->LocalAllocateAndSetBuffers(schema, blob);
    req.respond(ret);
  });


  /**====================================
   * Buffer Organizer Operations
   * ===================================*/

  RegisterRpc("RpcPlaceBlobInBuffers", [this, borg](
                                           const request &req,
                                           tl::bulk &bulk,
                                           size_t blob_size,
                                           std::vector<BufferInfo> &buffers) {
    hapi::Blob blob(blob_size);
    this->IoCallServer(req, bulk, IoType::kWrite, blob.data(), blob.size());
    borg->LocalPlaceBlobInBuffers(blob, buffers);
    req.respond(true);
  });
  RegisterRpc("RpcReadBlobFromBuffers", [this, borg](
                                           const request &req,
                                           tl::bulk &bulk,
                                           size_t size,
                                           std::vector<BufferInfo> &buffers) {
    hapi::Blob blob(size);
    borg->LocalReadBlobFromBuffers(blob, buffers);
    this->IoCallServer(req, bulk, IoType::kRead, blob.data(), blob.size());
    req.respond(true);
  });

  RPC_AUTOGEN_END
}

}  // namespace hermes
