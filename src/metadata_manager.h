//
// Created by lukemartinlogan on 12/4/22.
//

#ifndef HERMES_SRC_METADATA_MANAGER_H_
#define HERMES_SRC_METADATA_MANAGER_H_

#include "rpc_decorator.h"
#include "hermes_types.h"
#include "hermes_status.h"
#include "rpc.h"

namespace hermes {

using api::Blob;

struct BufferInfo {
  size_t off_;
  size_t size_;
  TargetID target_;
};

struct BlobInfo {
  lipcl::charbuf name_;
  std::vector<BufferInfo> buffers_;
};

struct BucketInfo {
  lipcl::charbuf name_;
  std::vector<BlobID> blobs_;
};

struct VBucketInfo {
  std::vector<char> name_;
  std::vector<BlobID> blobs_;
};

class MetadataManager {
 private:
  RPC_TYPE *rpc_;
  std::unordered_map<std::string, BlobID> blob_id_map_;
  std::unordered_map<std::string, BucketID> bkt_id_map_;
  std::unordered_map<std::string, VBucketID> vbkt_id_map_;

  std::unordered_map<BlobID, BlobInfo> blob_map_;
  std::unordered_map<BucketID, BucketInfo> bkt_map_;
  std::unordered_map<VBucketID, VBucketInfo> vbkt_map_;

 public:
  MetadataManager() = default;
  void Init();

  RPC BucketID LocalGetOrCreateBucket(std::string bkt_name);
  RPC BucketID LocalGetBucketId(std::string bkt_name);
  RPC bool LocalBucketContainsBlob(BucketID bkt_id, BlobID blob_id);
  RPC bool LocalRenameBucket(BucketID bkt_id);
  RPC bool LocalDestroyBucket(BucketID bkt_id);

  RPC BlobID LocalBucketPutBlob(BucketID bkt_id,
                                std::string blob_name,
                                Blob data,
                                std::vector<BufferInfo> &buffers);
  RPC BlobID LocalGetBlobId(BucketID bkt_id, std::string blob_name);
  RPC bool LocalSetBlobBuffers(BlobID blob_id,
                               std::vector<BufferInfo> &buffers);
  RPC std::vector<BufferInfo>&
      LocalGetBlobBuffers(BlobID blob_id);
  RPC bool LocalRenameBlob(BucketID bkt_id, std::string new_blob_name);
  RPC bool LocalDestroyBlob(BucketID bkt_id, std::string blob_name);

  RPC bool LocalWriteLockBlob(BlobID blob_id);
  RPC bool LocalWriteUnlockBlob(BlobID blob_id);
  RPC bool LocalReadLockBlob(BlobID blob_id);
  RPC bool LocalReadUnlockBlob(BlobID blob_id);

  RPC VBucketID LocalGetOrCreateVBucket(std::string vbkt_name);
  RPC VBucketID LocalGetVBucketId(std::string vbkt_name);
  RPC VBucketID LocalLinkBlobVBucket(VBucketID vbkt_id,
                                     BucketID bkt_id,
                                     std::string blob_name);
  RPC VBucketID LocalUnlinkBlobVBucket(VBucketID vbkt_id,
                                       BucketID bkt_id,
                                       std::string blob_name);
  RPC std::list<BlobID> LocalGetLinksVBucket(VBucketID vbkt_id);
  RPC bool LocalRenameVBucket(VBucketID vbkt_id,
                              std::string new_vbkt_name);
  RPC bool LocalDestroyVBucket(VBucketID vbkt_id,
                               std::string new_vbkt_name);

 public:
  RPC_AUTOGEN_START
  RPC BucketID GetOrCreateBucket(std::string bkt_name) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalGetOrCreateBucket(
        bkt_name);
    } else {
      return rpc_->Call<RPC BucketID>(
        target_node, "GetOrCreateBucket",
        bkt_name);
    }
  }
  RPC BucketID GetBucketId(std::string bkt_name) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalGetBucketId(
        bkt_name);
    } else {
      return rpc_->Call<RPC BucketID>(
        target_node, "GetBucketId",
        bkt_name);
    }
  }
  RPC bool BucketContainsBlob(BucketID bkt_id, BlobID blob_id) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalBucketContainsBlob(
        bkt_id, blob_id);
    } else {
      return rpc_->Call<RPC bool>(
        target_node, "BucketContainsBlob",
        bkt_id, blob_id);
    }
  }
  RPC bool RenameBucket(BucketID bkt_id) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalRenameBucket(
        bkt_id);
    } else {
      return rpc_->Call<RPC bool>(
        target_node, "RenameBucket",
        bkt_id);
    }
  }
  RPC bool DestroyBucket(BucketID bkt_id) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalDestroyBucket(
        bkt_id);
    } else {
      return rpc_->Call<RPC bool>(
        target_node, "DestroyBucket",
        bkt_id);
    }
  }
  RPC BlobID BucketPutBlob(BucketID bkt_id, std::string blob_name, Blob data, std::vector<BufferInfo>& buffers) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalBucketPutBlob(
        bkt_id, blob_name, data, buffers);
    } else {
      return rpc_->Call<RPC BlobID>(
        target_node, "BucketPutBlob",
        bkt_id, blob_name, data, buffers);
    }
  }
  RPC BlobID GetBlobId(BucketID bkt_id, std::string blob_name) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalGetBlobId(
        bkt_id, blob_name);
    } else {
      return rpc_->Call<RPC BlobID>(
        target_node, "GetBlobId",
        bkt_id, blob_name);
    }
  }
  RPC bool SetBlobBuffers(BlobID blob_id, std::vector<BufferInfo>& buffers) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalSetBlobBuffers(
        blob_id, buffers);
    } else {
      return rpc_->Call<RPC bool>(
        target_node, "SetBlobBuffers",
        blob_id, buffers);
    }
  }
  RPC std::vector<BufferInfo>& GetBlobBuffers(BlobID blob_id) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalGetBlobBuffers(
        blob_id);
    } else {
      return rpc_->Call<RPC std::vector<BufferInfo>&>(
        target_node, "GetBlobBuffers",
        blob_id);
    }
  }
  RPC bool RenameBlob(BucketID bkt_id, std::string new_blob_name) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalRenameBlob(
        bkt_id, new_blob_name);
    } else {
      return rpc_->Call<RPC bool>(
        target_node, "RenameBlob",
        bkt_id, new_blob_name);
    }
  }
  RPC bool DestroyBlob(BucketID bkt_id, std::string blob_name) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalDestroyBlob(
        bkt_id, blob_name);
    } else {
      return rpc_->Call<RPC bool>(
        target_node, "DestroyBlob",
        bkt_id, blob_name);
    }
  }
  RPC bool WriteLockBlob(BlobID blob_id) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalWriteLockBlob(
        blob_id);
    } else {
      return rpc_->Call<RPC bool>(
        target_node, "WriteLockBlob",
        blob_id);
    }
  }
  RPC bool WriteUnlockBlob(BlobID blob_id) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalWriteUnlockBlob(
        blob_id);
    } else {
      return rpc_->Call<RPC bool>(
        target_node, "WriteUnlockBlob",
        blob_id);
    }
  }
  RPC bool ReadLockBlob(BlobID blob_id) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalReadLockBlob(
        blob_id);
    } else {
      return rpc_->Call<RPC bool>(
        target_node, "ReadLockBlob",
        blob_id);
    }
  }
  RPC bool ReadUnlockBlob(BlobID blob_id) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalReadUnlockBlob(
        blob_id);
    } else {
      return rpc_->Call<RPC bool>(
        target_node, "ReadUnlockBlob",
        blob_id);
    }
  }
  RPC VBucketID GetOrCreateVBucket(std::string vbkt_name) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalGetOrCreateVBucket(
        vbkt_name);
    } else {
      return rpc_->Call<RPC VBucketID>(
        target_node, "GetOrCreateVBucket",
        vbkt_name);
    }
  }
  RPC VBucketID GetVBucketId(std::string vbkt_name) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalGetVBucketId(
        vbkt_name);
    } else {
      return rpc_->Call<RPC VBucketID>(
        target_node, "GetVBucketId",
        vbkt_name);
    }
  }
  RPC VBucketID UnlinkBlobVBucket(VBucketID vbkt_id, BucketID bkt_id, std::string blob_name) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalUnlinkBlobVBucket(
        vbkt_id, bkt_id, blob_name);
    } else {
      return rpc_->Call<RPC VBucketID>(
        target_node, "UnlinkBlobVBucket",
        vbkt_id, bkt_id, blob_name);
    }
  }
  RPC std::list<BlobID> GetLinksVBucket(VBucketID vbkt_id) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalGetLinksVBucket(
        vbkt_id);
    } else {
      return rpc_->Call<RPC std::list<BlobID>>(
        target_node, "GetLinksVBucket",
        vbkt_id);
    }
  }
  RPC bool RenameVBucket(VBucketID vbkt_id, std::string new_vbkt_name) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalRenameVBucket(
        vbkt_id, new_vbkt_name);
    } else {
      return rpc_->Call<RPC bool>(
        target_node, "RenameVBucket",
        vbkt_id, new_vbkt_name);
    }
  }
  RPC bool DestroyVBucket(VBucketID vbkt_id, std::string new_vbkt_name) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalDestroyVBucket(
        vbkt_id, new_vbkt_name);
    } else {
      return rpc_->Call<RPC bool>(
        target_node, "DestroyVBucket",
        vbkt_id, new_vbkt_name);
    }
  }
  RPC_AUTOGEN_END
};

}  // namespace hermes

#endif  // HERMES_SRC_METADATA_MANAGER_H_