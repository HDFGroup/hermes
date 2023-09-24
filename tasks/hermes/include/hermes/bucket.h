//
// Created by lukemartinlogan on 7/9/23.
//

#ifndef LABSTOR_TASKS_HERMES_CONF_INCLUDE_HERMES_CONF_BUCKET_H_
#define LABSTOR_TASKS_HERMES_CONF_INCLUDE_HERMES_CONF_BUCKET_H_

#include "hermes/hermes_types.h"
#include "hermes_mdm/hermes_mdm.h"
#include "hermes/config_manager.h"

namespace hermes {

#include "labstor/labstor_namespace.h"
using hermes::blob_mdm::PutBlobTask;
using hermes::blob_mdm::GetBlobTask;

#define HERMES_BUCKET_IS_FILE BIT_OPT(u32, 1)

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
    id_ = bkt_mdm_->GetOrCreateTagRoot(hshm::charbuf(bkt_name), true,
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
    id_ = bkt_mdm_->GetOrCreateTagRoot(hshm::charbuf(bkt_name), true,
                                       std::vector<TraitId>(), backend_size, flags);
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
                 const Blob &blob,
                 size_t blob_off,
                 const BlobId &orig_blob_id,
                 Context &ctx) {
    BlobId blob_id = orig_blob_id;
    bitfield32_t flags, task_flags(TASK_FIRE_AND_FORGET | TASK_DATA_OWNER | TASK_LOW_LATENCY);
    // Copy data to shared memory
    LPointer<char> p = LABSTOR_CLIENT->AllocateBuffer(blob.size());
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
    LPointer<labpq::TypedPushTask<PutBlobTask>> push_task;
    push_task = blob_mdm_->AsyncPutBlobRoot(id_, blob_name_buf,
                                            blob_id, blob_off, blob.size(), p.shm_, ctx.blob_score_,
                                            flags, ctx, task_flags);
    if (flags.Any(HERMES_GET_BLOB_ID)) {
      push_task->Wait();
      PutBlobTask *task = push_task->get();
      blob_id = task->blob_id_;
      LABSTOR_CLIENT->DelTask(push_task);
    }
    return blob_id;
  }

  /**
   * Put \a blob_name Blob into the bucket
   * */
  template<typename T, bool PARTIAL, bool ASYNC>
  HSHM_ALWAYS_INLINE
  BlobId SrlBasePut(const std::string &blob_name,
                    const T &data,
                    const BlobId &orig_blob_id,
                    Context &ctx) {
    std::stringstream ss;
    cereal::BinaryOutputArchive ar(ss);
    ar << data;
    Blob blob(ss.str());
    return BasePut<PARTIAL, ASYNC>(blob_name, blob, 0, orig_blob_id, ctx);
  }

  /**
   * Put \a blob_name Blob into the bucket
   * */
  template<typename T = Blob>
  BlobId Put(const std::string &blob_name,
             const T &blob,
             Context &ctx) {
    if (std::is_same_v<T, Blob>) {
      return BasePut<false, false>(blob_name, blob, 0, BlobId::GetNull(), ctx);
    } else {
      return SrlBasePut<T, false, false>(blob_name, blob, BlobId::GetNull(), ctx);
    }
  }

  /**
   * Put \a blob_id Blob into the bucket
   * */
  template<typename T = Blob>
  BlobId Put(const BlobId &blob_id,
             const T &blob,
             Context &ctx) {
    if (std::is_same_v<T, Blob>) {
      return BasePut<false, false>("", blob, 0, blob_id, ctx);
    } else {
      return SrlBasePut<T, false, false>("", blob, blob_id, ctx);
    }
  }

  /**
   * Put \a blob_name Blob into the bucket
   * */
  template<typename T = Blob>
  HSHM_ALWAYS_INLINE
  void AsyncPut(const std::string &blob_name,
                const T &blob,
                Context &ctx) {
    Put<T, false, true>(blob_name, blob, ctx);
  }

  /**
   * Put \a blob_id Blob into the bucket
   * */
  template<typename T = Blob>
  HSHM_ALWAYS_INLINE
  void AsyncPut(const BlobId &blob_id,
                const T &blob,
                Context &ctx) {
    Put<T, false, true>(blob_id, blob, ctx);
  }

  /**
   * PartialPut \a blob_name Blob into the bucket
   * */
  BlobId PartialPut(const std::string &blob_name,
                    const Blob &blob,
                    size_t blob_off,
                    Context &ctx) {
    return BasePut<true, false>(blob_name, blob, blob_off, BlobId::GetNull(), ctx);
  }

  /**
   * PartialPut \a blob_id Blob into the bucket
   * */
  BlobId PartialPut(const BlobId &blob_id,
                    const Blob &blob,
                    size_t blob_off,
                    Context &ctx) {
    return BasePut<true, false>("", blob, blob_off, blob_id, ctx);
  }

  /**
   * AsyncPartialPut \a blob_name Blob into the bucket
   * */
  void AsyncPartialPut(const std::string &blob_name,
                       const Blob &blob,
                       size_t blob_off,
                       Context &ctx) {
    BasePut<true, true>(blob_name, blob, blob_off, BlobId::GetNull(), ctx);
  }

  /**
   * AsyncPartialPut \a blob_id Blob into the bucket
   * */
  void AsyncPartialPut(const BlobId &blob_id,
                       const Blob &blob,
                       size_t blob_off,
                       Context &ctx) {
    BasePut<true, true>("", blob, blob_off, blob_id, ctx);
  }

  /**
   * Append \a blob_name Blob into the bucket (fully asynchronous)
   * */
  void Append(const Blob &blob, size_t page_size, Context &ctx) {
    LPointer<char> p = LABSTOR_CLIENT->AllocateBuffer(blob.size());
    char *data = p.ptr_;
    memcpy(data, blob.data(), blob.size());
    bkt_mdm_->AppendBlobRoot(id_, blob.size(), p.shm_, page_size, ctx.blob_score_, ctx.node_id_, ctx);
  }

  /**
   * Reorganize a blob to a new score or node
   * */
  void ReorganizeBlob(const BlobId &blob_id,
                      float score,
                      u32 node_id,
                      Context &ctx) {
    blob_mdm_->AsyncReorganizeBlobRoot(id_, blob_id, score, node_id);
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
    return blob_mdm_->GetBlobSizeRoot(id_, hshm::charbuf(name), BlobId::GetNull());
  }

  /**
   * Get \a blob_id Blob from the bucket (async)
   * */
  LPointer<labpq::TypedPushTask<GetBlobTask>>
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
    LPointer data_p = LABSTOR_CLIENT->AllocateBuffer(data_size);
    LPointer<labpq::TypedPushTask<GetBlobTask>> push_task;
    push_task = blob_mdm_->AsyncGetBlobRoot(id_, hshm::to_charbuf(blob_name),
                                            blob_id, blob_off,
                                            data_size, data_p.shm_,
                                            ctx, flags);
    return push_task;
  }

  /**
   * Get \a blob_id Blob from the bucket (sync)
   * */
  BlobId BaseGet(const std::string &blob_name,
                 const BlobId &orig_blob_id,
                 Blob &blob,
                 size_t blob_off,
                 Context &ctx) {
    // TODO(llogan): intercept mmap to avoid copy
    // TODO(llogan): make GetBlobSize work with blob_name
    size_t data_size = blob.size();
    if (blob.size() == 0) {
      data_size = blob_mdm_->GetBlobSizeRoot(id_, hshm::charbuf(blob_name), orig_blob_id);
      blob.resize(data_size);
    }
    HILOG(kDebug, "Getting blob of size {}", data_size);
    BlobId blob_id;
    LPointer<labpq::TypedPushTask<GetBlobTask>> push_task;
    push_task = AsyncBaseGet(blob_name, orig_blob_id, blob, blob_off, ctx);
    push_task->Wait();
    GetBlobTask *task = push_task->get();
    blob_id = task->blob_id_;
    char *data = LABSTOR_CLIENT->GetPrivatePointer(task->data_);
    memcpy(blob.data(), data, data_size);
    LABSTOR_CLIENT->FreeBuffer(task->data_);
    LABSTOR_CLIENT->DelTask(task);
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
    std::stringstream ss;
    cereal::BinaryInputArchive ar(ss);
    ar >> data;
    Blob blob(ss.str());
    return BaseGet(blob_name, orig_blob_id, blob, 0, ctx);
  }

  /**
   * Get \a blob_id Blob from the bucket
   * */
  template<typename T>
  BlobId Get(const std::string &blob_name,
             T &blob,
             Context &ctx) {
    if (std::is_same_v<T, Blob>) {
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
    if (std::is_same_v<T, Blob>) {
      return BaseGet("", blob_id, blob, 0, ctx);
    } else {
      return SrlBaseGet<T>("", blob_id, blob, ctx);
    }
  }

  /**
   * AsyncGet \a blob_name Blob from the bucket
   * */
  LPointer<labpq::TypedPushTask<GetBlobTask>>
  AsyncGet(const std::string &blob_name,
           Blob &blob,
           Context &ctx) {
    return AsyncBaseGet(blob_name, BlobId::GetNull(), blob, 0, ctx);
  }

  /**
   * AsyncGet \a blob_id Blob from the bucket
   * */
  LPointer<labpq::TypedPushTask<GetBlobTask>>
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
  LPointer<labpq::TypedPushTask<GetBlobTask>>
  AsyncPartialGet(const std::string &blob_name,
                  Blob &blob,
                  size_t blob_off,
                  Context &ctx) {
    return AsyncBaseGet(blob_name, BlobId::GetNull(), blob, blob_off, ctx);
  }

  /**
   * AsyncGet \a blob_id Blob from the bucket
   * */
  LPointer<labpq::TypedPushTask<GetBlobTask>>
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
    BlobId new_blob_id = blob_mdm_->GetBlobIdRoot(id_, hshm::to_charbuf(blob_name));
    return !new_blob_id.IsNull();
  }

  /**
   * Rename \a blob_id blob to \a new_blob_name new name
   * */
  void RenameBlob(const BlobId &blob_id, std::string new_blob_name, Context &ctx) {
    blob_mdm_->RenameBlobRoot(id_, blob_id, hshm::to_charbuf(new_blob_name));
  }

  /**
   * Delete \a blob_id blob
   * */
  void DestroyBlob(const BlobId &blob_id, Context &ctx) {
    // TODO(llogan): Make apart of bkt_mdm_ instead
    blob_mdm_->DestroyBlobRoot(id_, blob_id);
  }

  /**
   * Get the set of blob IDs contained in the bucket
   * */
  std::vector<BlobId> GetContainedBlobIds() {
    // TODO(llogan)
    return {};
  }
};

}  // namespace hermes

#endif  // LABSTOR_TASKS_HERMES_CONF_INCLUDE_HERMES_CONF_BUCKET_H_
