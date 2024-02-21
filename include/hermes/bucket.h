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

#ifndef HRUN_TASKS_HERMES_CONF_INCLUDE_HERMES_CONF_BUCKET_H_
#define HRUN_TASKS_HERMES_CONF_INCLUDE_HERMES_CONF_BUCKET_H_

#include "hermes/hermes_types.h"
#include "hermes_mdm/hermes_mdm.h"
#include "hermes/config_manager.h"

namespace hermes {

#include "hrun/hrun_namespace.h"
using hermes::blob_mdm::PutBlobTask;
using hermes::blob_mdm::GetBlobTask;

class Bucket {
 public:
  mdm::Client *mdm_;
  blob_mdm::Client *blob_mdm_;
  bucket_mdm::Client *bkt_mdm_;
  TagId id_;
  std::string name_;
  Context ctx_;
  bitfield32_t flags_;

 public:
  /**====================================
   * Bucket Operations
   * ===================================*/

  /**
   * Get or create \a bkt_name bucket.
   *
   * Called from hermes.h in GetBucket(). Should not
   * be used directly.
   * */
  explicit Bucket(const std::string &bkt_name,
                  size_t backend_size = 0,
                  u32 flags = 0) {
    mdm_ = &HERMES_CONF->mdm_;
    blob_mdm_ = &HERMES_CONF->blob_mdm_;
    bkt_mdm_ = &HERMES_CONF->bkt_mdm_;
    id_ = bkt_mdm_->GetOrCreateTagRoot(
        hshm::charbuf(bkt_name), true,
        std::vector<TraitId>(), backend_size, flags);
    name_ = bkt_name;
  }

  /**
   * Get or create \a bkt_name bucket.
   *
   * Called from hermes.h in GetBucket(). Should not
   * be used directly.
   * */
  explicit Bucket(const std::string &bkt_name,
                  Context &ctx,
                  size_t backend_size = 0,
                  u32 flags = 0) {
    mdm_ = &HERMES_CONF->mdm_;
    blob_mdm_ = &HERMES_CONF->blob_mdm_;
    bkt_mdm_ = &HERMES_CONF->bkt_mdm_;
    id_ = bkt_mdm_->GetOrCreateTagRoot(
        hshm::charbuf(bkt_name), true,
        std::vector<TraitId>(), backend_size, flags, ctx);
    name_ = bkt_name;
  }

  /**
   * Get an existing bucket.
   * */
  explicit Bucket(TagId tag_id) {
    id_ = tag_id;
    mdm_ = &HERMES_CONF->mdm_;
    blob_mdm_ = &HERMES_CONF->blob_mdm_;
    bkt_mdm_ = &HERMES_CONF->bkt_mdm_;
  }

  /** Default constructor */
  Bucket() = default;

  /** Default copy constructor */
  Bucket(const Bucket &other) = default;

  /** Default copy assign */
  Bucket& operator=(const Bucket &other) = default;

  /** Default move constructor */
  Bucket(Bucket &&other) = default;

  /** Default move assign */
  Bucket& operator=(Bucket &&other) = default;

 public:
  /**
   * Get the name of this bucket. Name is cached instead of
   * making an RPC. Not coherent if Rename is called.
   * */
  const std::string& GetName() const {
    return name_;
  }

  /**
   * Get the identifier of this bucket
   * */
  TagId GetId() const {
    return id_;
  }

  /**
   * Get the context object of this bucket
   * */
  Context& GetContext() {
    return ctx_;
  }

  /**
   * Attach a trait to the bucket
   * */
  void AttachTrait(TraitId trait_id) {
    // TODO(llogan)
  }

  /**
   * Get the current size of the bucket
   * */
  size_t GetSize() {
    return bkt_mdm_->GetSizeRoot(id_);
  }

  /**
   * Rename this bucket
   * */
  void Rename(const std::string &new_bkt_name) {
    bkt_mdm_->RenameTagRoot(id_, hshm::to_charbuf(new_bkt_name));
  }

  /**
   * Clears the buckets contents, but doesn't destroy its metadata
   * */
  void Clear() {
    bkt_mdm_->TagClearBlobsRoot(id_);
  }

  /**
   * Destroys this bucket along with all its contents.
   * */
  void Destroy() {
    bkt_mdm_->DestroyTagRoot(id_);
  }

  /**
   * Check if this bucket is valid
   * */
  bool IsNull() {
    return id_.IsNull();
  }

 public:
  /**====================================
   * Blob Operations
   * ===================================*/

  /**
   * Get the id of a blob from the blob name
   *
   * @param blob_name the name of the blob
   * @param blob_id (output) the returned blob_id
   * @return
   * */
  BlobId GetBlobId(const std::string &blob_name) {
    return blob_mdm_->GetBlobIdRoot(id_, hshm::to_charbuf(blob_name));
  }

  /**
   * Get the name of a blob from the blob id
   *
   * @param blob_id the blob_id
   * @param blob_name the name of the blob
   * @return The Status of the operation
   * */
  std::string GetBlobName(const BlobId &blob_id) {
    return blob_mdm_->GetBlobNameRoot(id_, blob_id);
  }


  /**
   * Get the score of a blob from the blob id
   *
   * @param blob_id the blob_id
   * @return The Status of the operation
   * */
  float GetBlobScore(const BlobId &blob_id) {
    return blob_mdm_->GetBlobScoreRoot(id_, blob_id);
  }

  /**
   * Label \a blob_id blob with \a tag_name TAG
   * */
  Status TagBlob(BlobId &blob_id,
                 TagId &tag_id) {
    bkt_mdm_->TagAddBlobRoot(tag_id, blob_id);
    return Status();
  }

  /**
   * Put \a blob_name Blob into the bucket
   * */
  template<bool PARTIAL, bool ASYNC>
  HSHM_ALWAYS_INLINE
  BlobId BasePut(const std::string &blob_name,
                 const BlobId &orig_blob_id,
                 const Blob &blob,
                 size_t blob_off,
                 Context &ctx) {
    BlobId blob_id = orig_blob_id;
    bitfield32_t flags, task_flags(
        TASK_FIRE_AND_FORGET | TASK_DATA_OWNER | TASK_LOW_LATENCY);
    // Copy data to shared memory
    LPointer<char> p = HRUN_CLIENT->AllocateBufferClient(blob.size());
    char *data = p.ptr_;
    memcpy(data, blob.data(), blob.size());
    // Put to shared memory
    hshm::charbuf blob_name_buf = hshm::to_charbuf(blob_name);
    if constexpr (!ASYNC) {
      if (blob_id.IsNull()) {
        flags.SetBits(HERMES_GET_BLOB_ID);
        task_flags.UnsetBits(TASK_FIRE_AND_FORGET);
      }
    }
    if constexpr(!PARTIAL) {
      flags.SetBits(HERMES_BLOB_REPLACE);
    }
    LPointer<hrunpq::TypedPushTask<PutBlobTask>> push_task;
    push_task = blob_mdm_->AsyncPutBlobRoot(id_, blob_name_buf,
                                            blob_id, blob_off, blob.size(),
                                            p.shm_, ctx.blob_score_,
                                            flags.bits_, ctx, task_flags.bits_);
    if constexpr (!ASYNC) {
      if (flags.Any(HERMES_GET_BLOB_ID)) {
        push_task->Wait();
        PutBlobTask *task = push_task->get();
        blob_id = task->blob_id_;
        HRUN_CLIENT->DelTask(push_task);
      }
    }
    return blob_id;
  }

  /**
   * Put \a blob_name Blob into the bucket
   * */
  template<typename T, bool PARTIAL, bool ASYNC>
  HSHM_ALWAYS_INLINE
  BlobId SrlBasePut(const std::string &blob_name,
                    const BlobId &orig_blob_id,
                    const T &data,
                    Context &ctx) {
    std::stringstream ss;
    cereal::BinaryOutputArchive ar(ss);
    ar << data;
    Blob blob(ss.str());
    return BasePut<PARTIAL, ASYNC>(blob_name, orig_blob_id, blob, 0, ctx);
  }

  /**
   * Put \a blob_name Blob into the bucket
   * */
  template<typename T = Blob>
  BlobId Put(const std::string &blob_name,
             const T &blob,
             Context &ctx) {
    if constexpr(std::is_same_v<T, Blob>) {
      return BasePut<false, false>(
          blob_name, BlobId::GetNull(), blob, 0, ctx);
    } else {
      return SrlBasePut<T, false, false>(
          blob_name, BlobId::GetNull(), blob, ctx);
    }
  }

  /**
   * Put \a blob_id Blob into the bucket
   * */
  template<typename T = Blob>
  BlobId Put(const BlobId &blob_id,
             const T &blob,
             Context &ctx) {
    if constexpr(std::is_same_v<T, Blob>) {
      return BasePut<false, false>("", blob_id, blob, 0, ctx);
    } else {
      return SrlBasePut<T, false, false>("", blob_id, blob, ctx);
    }
  }

  /**
   * Put \a blob_name Blob into the bucket
   * */
  template<typename T = Blob>
  HSHM_ALWAYS_INLINE
  void AsyncPut(const std::string &blob_name,
                const Blob &blob,
                Context &ctx) {
    if constexpr(std::is_same_v<T, Blob>) {
      BasePut<false, true>(blob_name, BlobId::GetNull(), blob, 0, ctx);
    } else {
      SrlBasePut<T, false, true>(blob_name, BlobId::GetNull(), blob, ctx);
    }
  }

  /**
   * Put \a blob_id Blob into the bucket
   * */
  template<typename T>
  HSHM_ALWAYS_INLINE
  void AsyncPut(const BlobId &blob_id,
                const Blob &blob,
                Context &ctx) {
    if constexpr(std::is_same_v<T, Blob>) {
      BasePut<false, true>("", blob_id, blob, 0, ctx);
    } else {
      SrlBasePut<T, false, true>("", blob_id, blob, ctx);
    }
  }

  /**
   * PartialPut \a blob_name Blob into the bucket
   * */
  BlobId PartialPut(const std::string &blob_name,
                    const Blob &blob,
                    size_t blob_off,
                    Context &ctx) {
    return BasePut<true, false>(blob_name,
                                BlobId::GetNull(),
                                blob, blob_off, ctx);
  }

  /**
   * PartialPut \a blob_id Blob into the bucket
   * */
  BlobId PartialPut(const BlobId &blob_id,
                    const Blob &blob,
                    size_t blob_off,
                    Context &ctx) {
    return BasePut<true, false>("", blob_id, blob, blob_off, ctx);
  }

  /**
   * AsyncPartialPut \a blob_name Blob into the bucket
   * */
  void AsyncPartialPut(const std::string &blob_name,
                       const Blob &blob,
                       size_t blob_off,
                       Context &ctx) {
    BasePut<true, true>(blob_name, BlobId::GetNull(), blob, blob_off, ctx);
  }

  /**
   * AsyncPartialPut \a blob_id Blob into the bucket
   * */
  void AsyncPartialPut(const BlobId &blob_id,
                       const Blob &blob,
                       size_t blob_off,
                       Context &ctx) {
    BasePut<true, true>("", blob_id, blob, blob_off, ctx);
  }

  /**
   * Append \a blob_name Blob into the bucket (fully asynchronous)
   * */
  void Append(const Blob &blob, size_t page_size, Context &ctx) {
    LPointer<char> p = HRUN_CLIENT->AllocateBufferClient(blob.size());
    char *data = p.ptr_;
    memcpy(data, blob.data(), blob.size());
    bkt_mdm_->AppendBlobRoot(
        id_, blob.size(), p.shm_, page_size,
        ctx.blob_score_, ctx.node_id_, ctx);
  }

  /**
   * Reorganize a blob to a new score or node
   * */
  void ReorganizeBlob(const std::string &name,
                      float score,
                      const Context &ctx = Context()) {
    blob_mdm_->AsyncReorganizeBlobRoot(
        id_, hshm::charbuf(name), BlobId::GetNull(), score, true, ctx);
  }

  /**
   * Reorganize a blob to a new score or node
   * */
  void ReorganizeBlob(const BlobId &blob_id,
                      float score,
                      const Context &ctx = Context()) {
    blob_mdm_->AsyncReorganizeBlobRoot(
        id_, hshm::charbuf(""), blob_id, score, true, ctx);
  }

  /**
   * Reorganize a blob to a new score or node
   *
   * @depricated
   * */
  void ReorganizeBlob(const BlobId &blob_id,
                      float score,
                      u32 node_id,
                      Context &ctx) {
    ctx.node_id_ = node_id;
    blob_mdm_->AsyncReorganizeBlobRoot(
        id_, hshm::charbuf(""), blob_id, score, true, ctx);
  }

  /**
   * Get the current size of the blob in the bucket
   * */
  size_t GetBlobSize(const BlobId &blob_id) {
    return blob_mdm_->GetBlobSizeRoot(id_, hshm::charbuf(""), blob_id);
  }

  /**
   * Get the current size of the blob in the bucket
   * */
  size_t GetBlobSize(const std::string &name) {
    return blob_mdm_->GetBlobSizeRoot(
        id_, hshm::charbuf(name), BlobId::GetNull());
  }

  /**
   * Get \a blob_id Blob from the bucket (async)
   * */
  LPointer<hrunpq::TypedPushTask<GetBlobTask>>
  HSHM_ALWAYS_INLINE
  AsyncBaseGet(const std::string &blob_name,
               const BlobId &blob_id,
               Blob &blob,
               size_t blob_off,
               Context &ctx) {
    bitfield32_t flags;
    // Get the blob ID
    if (blob_id.IsNull()) {
      flags.SetBits(HERMES_GET_BLOB_ID);
    }
    // Get from shared memory
    size_t data_size = blob.size();
    LPointer data_p = HRUN_CLIENT->AllocateBufferClient(blob.size());
    LPointer<hrunpq::TypedPushTask<GetBlobTask>> push_task;
    push_task = blob_mdm_->AsyncGetBlobRoot(id_, hshm::to_charbuf(blob_name),
                                            blob_id, blob_off,
                                            data_size, data_p.shm_,
                                            ctx, flags.bits_);
    return push_task;
  }

  /**
   * Get \a blob_id Blob from the bucket (sync)
   * */
  BlobId BaseGet(const std::string &blob_name,
                 BlobId blob_id,
                 Blob &blob,
                 size_t blob_off,
                 Context &ctx) {
    // Get the blob ID
    if (blob_id.IsNull()) {
      auto &blob_id_map = HERMES_CONF->blob_mdm_.blob_id_map_;
      auto blob_name_buf = blob_mdm::Client::GetBlobNameWithBucket(id_, blob_name);
      auto it = blob_id_map.find(*blob_name_buf);
      if (it != blob_id_map.end()) {
        blob_id = *it.val_->second_;
      }
    }
    if (!blob_id.IsNull()) {
      auto &blob_map = HERMES_CONF->blob_mdm_.blob_map_;
      auto it = blob_map.find(blob_id);
      if (it != blob_map.end()) {
        BlobInfo &blob_info = *it.val_->second_;
        if (blob_off + blob.size() > blob_info.blob_size_) {
          if (blob_info.blob_size_ < blob_off) {
            return BlobId::GetNull();
          }
          blob.resize(blob_info.blob_size_ - blob_off);
        }
        if (!blob_info.data_.shm_.IsNull()) {
          char *data = HRUN_CLIENT->GetDataPointer(blob_info.data_.shm_);
          memcpy(blob.data(), data + blob_off, blob.size());
          return blob_id;
        }
      }
    }
    size_t data_size = blob.size();
    if (blob.size() == 0) {
      data_size = blob_mdm_->GetBlobSizeRoot(
          id_, hshm::charbuf(blob_name), blob_id);
      blob.resize(data_size);
    }
    HILOG(kDebug, "Getting blob of size {}", data_size);
    LPointer<hrunpq::TypedPushTask<GetBlobTask>> push_task;
    push_task = AsyncBaseGet(blob_name, blob_id, blob, blob_off, ctx);
    push_task->Wait();
    GetBlobTask *task = push_task->get();
    blob_id = task->blob_id_;
    char *data = HRUN_CLIENT->GetDataPointer(task->data_);
    memcpy(blob.data(), data, task->data_size_);
    blob.resize(task->data_size_);
    HRUN_CLIENT->FreeBuffer(task->data_);
    HRUN_CLIENT->DelTask(push_task);
    return blob_id;
  }

  /**
   * Get \a blob_id Blob from the bucket (sync)
   * */
  template<typename T>
  BlobId SrlBaseGet(const std::string &blob_name,
                    const BlobId &orig_blob_id,
                    T &data,
                    Context &ctx) {
    Blob blob;
    BlobId blob_id = BaseGet(blob_name, orig_blob_id, blob, 0, ctx);
    if (blob.size() == 0) {
      return BlobId::GetNull();
    }
    std::stringstream ss(std::string(blob.data(), blob.size()));
    cereal::BinaryInputArchive ar(ss);
    ar >> data;
    return blob_id;
  }

  /**
   * Get \a blob_id Blob from the bucket
   * */
  template<typename T>
  BlobId Get(const std::string &blob_name,
             T &blob,
             Context &ctx) {
    if constexpr(std::is_same_v<T, Blob>) {
      return BaseGet(blob_name, BlobId::GetNull(), blob, 0, ctx);
    } else {
      return SrlBaseGet<T>(blob_name, BlobId::GetNull(), blob, ctx);
    }
  }

  /**
   * Get \a blob_id Blob from the bucket
   * */
  template<typename T>
  BlobId Get(const BlobId &blob_id,
             T &blob,
             Context &ctx) {
    if constexpr(std::is_same_v<T, Blob>) {
      return BaseGet("", blob_id, blob, 0, ctx);
    } else {
      return SrlBaseGet<T>("", blob_id, blob, ctx);
    }
  }

  /**
   * AsyncGet \a blob_name Blob from the bucket
   * */
  LPointer<hrunpq::TypedPushTask<GetBlobTask>>
  AsyncGet(const std::string &blob_name,
           Blob &blob,
           Context &ctx) {
    return AsyncBaseGet(blob_name, BlobId::GetNull(), blob, 0, ctx);
  }

  /**
   * AsyncGet \a blob_id Blob from the bucket
   * */
  LPointer<hrunpq::TypedPushTask<GetBlobTask>>
  AsyncGet(const BlobId &blob_id,
           Blob &blob,
           Context &ctx) {
    return AsyncBaseGet("", blob_id, blob, 0, ctx);
  }

  /**
   * Put \a blob_name Blob into the bucket
   * */
  BlobId PartialGet(const std::string &blob_name,
                    Blob &blob,
                    size_t blob_off,
                    Context &ctx) {
    return BaseGet(blob_name, BlobId::GetNull(), blob, blob_off, ctx);
  }

  /**
   * Put \a blob_name Blob into the bucket
   * */
  BlobId PartialGet(const BlobId &blob_id,
                    Blob &blob,
                    size_t blob_off,
                    Context &ctx) {
    return BaseGet("", blob_id, blob, blob_off, ctx);
  }

  /**
   * AsyncGet \a blob_name Blob from the bucket
   * */
  LPointer<hrunpq::TypedPushTask<GetBlobTask>>
  AsyncPartialGet(const std::string &blob_name,
                  Blob &blob,
                  size_t blob_off,
                  Context &ctx) {
    return AsyncBaseGet(blob_name, BlobId::GetNull(), blob, blob_off, ctx);
  }

  /**
   * AsyncGet \a blob_id Blob from the bucket
   * */
  LPointer<hrunpq::TypedPushTask<GetBlobTask>>
  AsyncPartialGet(const BlobId &blob_id,
                  Blob &blob,
                  size_t blob_off,
                  Context &ctx) {
    return AsyncBaseGet("", blob_id, blob, blob_off, ctx);
  }

  /**
   * Determine if the bucket contains \a blob_id BLOB
   * */
  bool ContainsBlob(const std::string &blob_name) {
    BlobId new_blob_id = blob_mdm_->GetBlobIdRoot(
        id_, hshm::to_charbuf(blob_name));
    return !new_blob_id.IsNull();
  }

  /**
   * Rename \a blob_id blob to \a new_blob_name new name
   * */
  void RenameBlob(const BlobId &blob_id,
                  std::string new_blob_name,
                  Context &ctx) {
    blob_mdm_->RenameBlobRoot(id_, blob_id, hshm::to_charbuf(new_blob_name));
  }

  /**
   * Delete \a blob_id blob
   * */
  void DestroyBlob(const BlobId &blob_id, Context &ctx) {
    blob_mdm_->DestroyBlobRoot(id_, blob_id);
  }

  /**
   * Get the set of blob IDs contained in the bucket
   * */
  std::vector<BlobId> GetContainedBlobIds() {
    return bkt_mdm_->GetContainedBlobIdsRoot(id_);
  }
};

}  // namespace hermes

#endif  // HRUN_TASKS_HERMES_CONF_INCLUDE_HERMES_CONF_BUCKET_H_
