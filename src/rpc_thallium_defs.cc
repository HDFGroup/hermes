//
// Created by lukemartinlogan on 12/3/22.
//

#include "rpc_thallium.h"
#include "metadata_manager.h"

namespace hermes {

using thallium::request;

void ThalliumRpc::DefineRpcs() {
  RPC_CLASS_INSTANCE_DEFS_START
  MetadataManager *mdm = this->mdm_;
  RPC_CLASS_INSTANCE_DEFS_END

  RPC_AUTOGEN_START
  auto rpc_get_or_create_bucket = 
    [mdm](const request &req, std::string bkt_name) {
      auto result = mdm->LocalGetOrCreateBucket(bkt_name);
      req.respond(result);
    };
  server_engine_->define("GetOrCreateBucket", rpc_get_or_create_bucket);
  auto rpc_get_bucket_id = 
    [mdm](const request &req, std::string bkt_name) {
      auto result = mdm->LocalGetBucketId(bkt_name);
      req.respond(result);
    };
  server_engine_->define("GetBucketId", rpc_get_bucket_id);
  auto rpc_bucket_contains_blob = 
    [mdm](const request &req, BucketID bkt_id, BlobID blob_id) {
      auto result = mdm->LocalBucketContainsBlob(bkt_id, blob_id);
      req.respond(result);
    };
  server_engine_->define("BucketContainsBlob", rpc_bucket_contains_blob);
  auto rpc_rename_bucket = 
    [mdm](const request &req, BucketID bkt_id) {
      auto result = mdm->LocalRenameBucket(bkt_id);
      req.respond(result);
    };
  server_engine_->define("RenameBucket", rpc_rename_bucket);
  auto rpc_destroy_bucket = 
    [mdm](const request &req, BucketID bkt_id) {
      auto result = mdm->LocalDestroyBucket(bkt_id);
      req.respond(result);
    };
  server_engine_->define("DestroyBucket", rpc_destroy_bucket);
  auto rpc_bucket_put_blob = 
    [mdm](const request &req, BucketID bkt_id, std::string blob_name, Blob data, std::vector<BufferInfo>& buffers) {
      auto result = mdm->LocalBucketPutBlob(bkt_id, blob_name, data, buffers);
      req.respond(result);
    };
  server_engine_->define("BucketPutBlob", rpc_bucket_put_blob);
  auto rpc_get_blob_id = 
    [mdm](const request &req, BucketID bkt_id, std::string blob_name) {
      auto result = mdm->LocalGetBlobId(bkt_id, blob_name);
      req.respond(result);
    };
  server_engine_->define("GetBlobId", rpc_get_blob_id);
  auto rpc_set_blob_buffers = 
    [mdm](const request &req, BlobID blob_id, std::vector<BufferInfo>& buffers) {
      auto result = mdm->LocalSetBlobBuffers(blob_id, buffers);
      req.respond(result);
    };
  server_engine_->define("SetBlobBuffers", rpc_set_blob_buffers);
  auto rpc_get_blob_buffers = 
    [mdm](const request &req, BlobID blob_id) {
      auto result = mdm->LocalGetBlobBuffers(blob_id);
      req.respond(result);
    };
  server_engine_->define("GetBlobBuffers", rpc_get_blob_buffers);
  auto rpc_rename_blob = 
    [mdm](const request &req, BucketID bkt_id, std::string new_blob_name) {
      auto result = mdm->LocalRenameBlob(bkt_id, new_blob_name);
      req.respond(result);
    };
  server_engine_->define("RenameBlob", rpc_rename_blob);
  auto rpc_destroy_blob = 
    [mdm](const request &req, BucketID bkt_id, std::string blob_name) {
      auto result = mdm->LocalDestroyBlob(bkt_id, blob_name);
      req.respond(result);
    };
  server_engine_->define("DestroyBlob", rpc_destroy_blob);
  auto rpc_write_lock_blob = 
    [mdm](const request &req, BlobID blob_id) {
      auto result = mdm->LocalWriteLockBlob(blob_id);
      req.respond(result);
    };
  server_engine_->define("WriteLockBlob", rpc_write_lock_blob);
  auto rpc_write_unlock_blob = 
    [mdm](const request &req, BlobID blob_id) {
      auto result = mdm->LocalWriteUnlockBlob(blob_id);
      req.respond(result);
    };
  server_engine_->define("WriteUnlockBlob", rpc_write_unlock_blob);
  auto rpc_read_lock_blob = 
    [mdm](const request &req, BlobID blob_id) {
      auto result = mdm->LocalReadLockBlob(blob_id);
      req.respond(result);
    };
  server_engine_->define("ReadLockBlob", rpc_read_lock_blob);
  auto rpc_read_unlock_blob = 
    [mdm](const request &req, BlobID blob_id) {
      auto result = mdm->LocalReadUnlockBlob(blob_id);
      req.respond(result);
    };
  server_engine_->define("ReadUnlockBlob", rpc_read_unlock_blob);
  auto rpc_get_or_create_v_bucket = 
    [mdm](const request &req, std::string vbkt_name) {
      auto result = mdm->LocalGetOrCreateVBucket(vbkt_name);
      req.respond(result);
    };
  server_engine_->define("GetOrCreateVBucket", rpc_get_or_create_v_bucket);
  auto rpc_get_v_bucket_id = 
    [mdm](const request &req, std::string vbkt_name) {
      auto result = mdm->LocalGetVBucketId(vbkt_name);
      req.respond(result);
    };
  server_engine_->define("GetVBucketId", rpc_get_v_bucket_id);
  auto rpc_unlink_blob_v_bucket = 
    [mdm](const request &req, VBucketID vbkt_id, BucketID bkt_id, std::string blob_name) {
      auto result = mdm->LocalUnlinkBlobVBucket(vbkt_id, bkt_id, blob_name);
      req.respond(result);
    };
  server_engine_->define("UnlinkBlobVBucket", rpc_unlink_blob_v_bucket);
  auto rpc_get_links_v_bucket = 
    [mdm](const request &req, VBucketID vbkt_id) {
      auto result = mdm->LocalGetLinksVBucket(vbkt_id);
      req.respond(result);
    };
  server_engine_->define("GetLinksVBucket", rpc_get_links_v_bucket);
  auto rpc_rename_v_bucket = 
    [mdm](const request &req, VBucketID vbkt_id, std::string new_vbkt_name) {
      auto result = mdm->LocalRenameVBucket(vbkt_id, new_vbkt_name);
      req.respond(result);
    };
  server_engine_->define("RenameVBucket", rpc_rename_v_bucket);
  auto rpc_destroy_v_bucket = 
    [mdm](const request &req, VBucketID vbkt_id, std::string new_vbkt_name) {
      auto result = mdm->LocalDestroyVBucket(vbkt_id, new_vbkt_name);
      req.respond(result);
    };
  server_engine_->define("DestroyVBucket", rpc_destroy_v_bucket);
  RPC_AUTOGEN_END
}

}  // namespace hermes