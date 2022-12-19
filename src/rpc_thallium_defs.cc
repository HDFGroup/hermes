//
// Created by lukemartinlogan on 12/3/22.
//

#include "rpc_thallium.h"
#include "metadata_manager.h"
#include "rpc_thallium_serialization.h"
#include "data_structures.h"

namespace hermes {

using thallium::request;

void ThalliumRpc::DefineRpcs() {
  RPC_CLASS_INSTANCE_DEFS_START
  MetadataManager *mdm = this->mdm_;
  RPC_CLASS_INSTANCE_DEFS_END

  RPC_AUTOGEN_START
  auto remote_get_or_create_bucket = 
    [mdm](const request &req, lipc::charbuf& bkt_name) {
      auto result = mdm->LocalGetOrCreateBucket(bkt_name);
      req.respond(result);
    };
  server_engine_->define("GetOrCreateBucket", remote_get_or_create_bucket);
  auto remote_get_bucket_id = 
    [mdm](const request &req, lipc::charbuf& bkt_name) {
      auto result = mdm->LocalGetBucketId(bkt_name);
      req.respond(result);
    };
  server_engine_->define("GetBucketId", remote_get_bucket_id);
  auto remote_bucket_contains_blob = 
    [mdm](const request &req, BucketID bkt_id, BlobID blob_id) {
      auto result = mdm->LocalBucketContainsBlob(bkt_id, blob_id);
      req.respond(result);
    };
  server_engine_->define("BucketContainsBlob", remote_bucket_contains_blob);
  auto remote_rename_bucket = 
    [mdm](const request &req, BucketID bkt_id, lipc::charbuf& new_bkt_name) {
      auto result = mdm->LocalRenameBucket(bkt_id, new_bkt_name);
      req.respond(result);
    };
  server_engine_->define("RenameBucket", remote_rename_bucket);
  auto remote_destroy_bucket = 
    [mdm](const request &req, BucketID bkt_id) {
      auto result = mdm->LocalDestroyBucket(bkt_id);
      req.respond(result);
    };
  server_engine_->define("DestroyBucket", remote_destroy_bucket);
  auto remote_bucket_put_blob = 
    [mdm](const request &req, BucketID bkt_id, lipc::charbuf& blob_name, Blob& data, lipc::vector<BufferInfo>& buffers) {
      auto result = mdm->LocalBucketPutBlob(bkt_id, blob_name, data, buffers);
      req.respond(result);
    };
  server_engine_->define("BucketPutBlob", remote_bucket_put_blob);
  auto remote_get_blob_id = 
    [mdm](const request &req, BucketID bkt_id, lipc::charbuf& blob_name) {
      auto result = mdm->LocalGetBlobId(bkt_id, blob_name);
      req.respond(result);
    };
  server_engine_->define("GetBlobId", remote_get_blob_id);
  auto remote_set_blob_buffers = 
    [mdm](const request &req, BlobID blob_id, lipc::vector<BufferInfo>& buffers) {
      auto result = mdm->LocalSetBlobBuffers(blob_id, buffers);
      req.respond(result);
    };
  server_engine_->define("SetBlobBuffers", remote_set_blob_buffers);
  auto remote_get_blob_buffers = 
    [mdm](const request &req, BlobID blob_id) {
      auto result = mdm->LocalGetBlobBuffers(blob_id);
      req.respond(std::ref(result));
    };
  server_engine_->define("GetBlobBuffers", remote_get_blob_buffers);
  auto remote_rename_blob = 
    [mdm](const request &req, BucketID bkt_id, BlobID blob_id, lipc::charbuf& new_blob_name) {
      auto result = mdm->LocalRenameBlob(bkt_id, blob_id, new_blob_name);
      req.respond(result);
    };
  server_engine_->define("RenameBlob", remote_rename_blob);
  auto remote_destroy_blob = 
    [mdm](const request &req, BucketID bkt_id, lipc::charbuf& blob_name) {
      auto result = mdm->LocalDestroyBlob(bkt_id, blob_name);
      req.respond(result);
    };
  server_engine_->define("DestroyBlob", remote_destroy_blob);
  auto remote_write_lock_blob = 
    [mdm](const request &req, BlobID blob_id) {
      auto result = mdm->LocalWriteLockBlob(blob_id);
      req.respond(result);
    };
  server_engine_->define("WriteLockBlob", remote_write_lock_blob);
  auto remote_write_unlock_blob = 
    [mdm](const request &req, BlobID blob_id) {
      auto result = mdm->LocalWriteUnlockBlob(blob_id);
      req.respond(result);
    };
  server_engine_->define("WriteUnlockBlob", remote_write_unlock_blob);
  auto remote_read_lock_blob = 
    [mdm](const request &req, BlobID blob_id) {
      auto result = mdm->LocalReadLockBlob(blob_id);
      req.respond(result);
    };
  server_engine_->define("ReadLockBlob", remote_read_lock_blob);
  auto remote_read_unlock_blob = 
    [mdm](const request &req, BlobID blob_id) {
      auto result = mdm->LocalReadUnlockBlob(blob_id);
      req.respond(result);
    };
  server_engine_->define("ReadUnlockBlob", remote_read_unlock_blob);
  auto remote_get_or_create_v_bucket = 
    [mdm](const request &req, lipc::charbuf& vbkt_name) {
      auto result = mdm->LocalGetOrCreateVBucket(vbkt_name);
      req.respond(result);
    };
  server_engine_->define("GetOrCreateVBucket", remote_get_or_create_v_bucket);
  auto remote_get_v_bucket_id = 
    [mdm](const request &req, lipc::charbuf& vbkt_name) {
      auto result = mdm->LocalGetVBucketId(vbkt_name);
      req.respond(result);
    };
  server_engine_->define("GetVBucketId", remote_get_v_bucket_id);
  auto remote_v_bucket_link_blob = 
    [mdm](const request &req, VBucketID vbkt_id, BucketID bkt_id, lipc::charbuf& blob_name) {
      auto result = mdm->LocalVBucketLinkBlob(vbkt_id, bkt_id, blob_name);
      req.respond(result);
    };
  server_engine_->define("VBucketLinkBlob", remote_v_bucket_link_blob);
  auto remote_v_bucket_unlink_blob = 
    [mdm](const request &req, VBucketID vbkt_id, BucketID bkt_id, lipc::charbuf& blob_name) {
      auto result = mdm->LocalVBucketUnlinkBlob(vbkt_id, bkt_id, blob_name);
      req.respond(result);
    };
  server_engine_->define("VBucketUnlinkBlob", remote_v_bucket_unlink_blob);
  auto remote_v_bucket_get_links = 
    [mdm](const request &req, VBucketID vbkt_id) {
      auto result = mdm->LocalVBucketGetLinks(vbkt_id);
      req.respond(result);
    };
  server_engine_->define("VBucketGetLinks", remote_v_bucket_get_links);
  auto remote_rename_v_bucket = 
    [mdm](const request &req, VBucketID vbkt_id, lipc::charbuf& new_vbkt_name) {
      auto result = mdm->LocalRenameVBucket(vbkt_id, new_vbkt_name);
      req.respond(result);
    };
  server_engine_->define("RenameVBucket", remote_rename_v_bucket);
  auto remote_destroy_v_bucket = 
    [mdm](const request &req, VBucketID vbkt_id) {
      auto result = mdm->LocalDestroyVBucket(vbkt_id);
      req.respond(result);
    };
  server_engine_->define("DestroyVBucket", remote_destroy_v_bucket);
  RPC_AUTOGEN_END
}

}  // namespace hermes