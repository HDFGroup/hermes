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

#ifndef HERMES_SRC_BUFFER_ORGANIZER_H_
#define HERMES_SRC_BUFFER_ORGANIZER_H_

#include "rpc.h"
#include "hermes_types.h"
#include "buffer_pool.h"
#include "trait_manager.h"

/** Forward declaration of Bucket */
namespace hermes::api {
class Bucket;
}  // namespace hermes::api

namespace hermes {

// NOTE(llogan): I may add this back to make flushing more intelligent
/** Calculates the total size of a blob's buffers
static inline size_t SumBufferBlobSizes(std::vector<BufferInfo> &buffers) {
  size_t sum = 0;
  for (BufferInfo &buffer_ref : buffers) {
    sum += buffer_ref.blob_size_;
  }
  return sum;
}*/

/** Information needed by traits called internally by Flush */
struct FlushTraitParams {
  Blob *blob_;
  std::string blob_name_;
  std::shared_ptr<api::Bucket> *bkt_;
};

/** An I/O flushing task spawned by BORG */
struct BorgIoTask {
  TagId bkt_id_;
  BlobId blob_id_;
  size_t blob_size_;
  hipc::Ref<BlobInfo> blob_info_;
  std::vector<Trait*> traits_;

  /** Default constructor */
  BorgIoTask() = default;

  /** Emplace constructor */
  explicit BorgIoTask(TagId bkt_id, BlobId blob_id, size_t blob_size,
                      hipc::Ref<BlobInfo> &blob_info,
                      std::vector<Trait*> &&traits)
    : bkt_id_(bkt_id), blob_id_(blob_id), blob_size_(blob_size),
      blob_info_(blob_info),
      traits_(std::forward<std::vector<Trait*>>(traits)) {}

  /** Copy constructor */
  BorgIoTask(const BorgIoTask &other)
      : bkt_id_(other.bkt_id_),
        blob_id_(other.blob_id_),
        blob_size_(other.blob_size_),
        blob_info_(other.blob_info_),
        traits_(other.traits_) {}

  /** Move constructor */
  BorgIoTask(BorgIoTask &&other)
      : bkt_id_(std::move(other.bkt_id_)),
        blob_id_(std::move(other.blob_id_)),
        blob_size_(std::move(other.blob_size_)),
        blob_info_(std::move(other.blob_info_)),
        traits_(std::move(other.traits_)) {}
};

/** A queue for holding BORG I/O flushing tasks */
struct BorgIoThreadQueue {
  std::queue<BorgIoTask> queue_;  /**< Holds pending tasks*/
  std::atomic<size_t> load_;      /**< Data being processed on queue */
  int id_;                        /**< ID of this worker queue */
  hshm::Mutex lock_;              /**< Synchronize enqueue / flush phases */

  /** Default constructor */
  BorgIoThreadQueue() : load_(0) {}

  /** Copy constructor */
  BorgIoThreadQueue(const BorgIoThreadQueue &other)
  : queue_(other.queue_), load_(other.load_.load()), id_(other.id_) {}
};

/** Macro to simplify thread manager singleton */
#define HERMES_BORG_IO_THREAD_MANAGER \
  hshm::EasySingleton<BorgIoThreadManager>::GetInstance()

/** Manages the I/O flushing threads for BORG */
class BorgIoThreadManager {
 public:
  std::vector<BorgIoThreadQueue> queues_;  /**< The set of worker queues */
  std::atomic<bool> kill_requested_;

 public:
  /** Constructor */
  BorgIoThreadManager() {
    kill_requested_.store(false);
  }

  /** Indicate that no more modifications will take place to blobs */
  void Join() {
    kill_requested_.store(true);
  }

  /** Check if IoThreadManager is still alive */
  bool Alive() {
    return !kill_requested_.load();
  }

  /** Spawn the enqueuing thread */
  void SpawnFlushMonitor(int num_threads);

  /** Spawn the I/O threads */
  void SpawnFlushWorkers(int num_threads);

  /** Wait for flushing to complete */
  void WaitForFlush() {
    while (IsFlushing()) {
      LockAll();
      UnlockAll();
    }
  }

  /** Check if a flush is still happening */
  bool IsFlushing() {
    for (BorgIoThreadQueue &bq : queues_) {
      if (bq.queue_.size() > 0) {
        return true;
      }
    }
    return false;
  }

  /** Lock all queues */
  void LockAll() {
    for (BorgIoThreadQueue &bq : queues_) {
      bq.lock_.Lock();
    }
  }

  /** Unlock all queues */
  void UnlockAll() {
    for (BorgIoThreadQueue &bq : queues_) {
      bq.lock_.Unlock();
    }
  }

  /** Enqueue a flushing task to a worker */
  void Enqueue(TagId bkt_id, BlobId blob_id, size_t blob_size,
               hipc::Ref<BlobInfo> &info, std::vector<Trait*> &&traits) {
    BorgIoThreadQueue& bq = FindLowestQueue();
    bq.load_.fetch_add(blob_size);
    bq.queue_.emplace(bkt_id, blob_id, blob_size,
                      info,
                      std::forward<std::vector<Trait*>>(traits));
  }

  /** Find the queue with the least burden */
  BorgIoThreadQueue& FindLowestQueue() {
    BorgIoThreadQueue *bq(nullptr);
    size_t load = std::numeric_limits<uint64_t>::max();
    for (BorgIoThreadQueue &temp_bq : queues_) {
      if (temp_bq.load_ < load) {
        bq = &temp_bq;
      }
    }
    return *bq;
  }
};

/** A scoped mutex for the BORG I/O thread manager */
struct ScopedBorgIoThreadMutex {
  ScopedBorgIoThreadMutex() {
    HERMES_BORG_IO_THREAD_MANAGER->LockAll();
  }

  ~ScopedBorgIoThreadMutex() {
    HERMES_BORG_IO_THREAD_MANAGER->UnlockAll();
  }
};

/**
 * Any state needed by BORG in SHM
 * */
template<>
struct ShmHeader<BufferOrganizer> {
};

/**
 * Manages the organization of blobs in the hierarchy.
 * */
class BufferOrganizer : public hipc::ShmContainer {
 public:
  SHM_CONTAINER_TEMPLATE(BufferOrganizer,
                         BufferOrganizer,
                         ShmHeader<BufferOrganizer>)
  MetadataManager *mdm_;
  RPC_TYPE *rpc_;

 public:
  /**====================================
   * Default Constructor
   * ===================================*/

  /**
   * Initialize the BORG
   * REQUIRES mdm to be initialized already.
   * */
  explicit BufferOrganizer(ShmHeader<BufferOrganizer> *header,
                           hipc::Allocator *alloc);

  /**====================================
   * SHM Deserialization
   * ===================================*/

  /** Deserialize the BORG from shared memory */
  void shm_deserialize_main();

  /**====================================
   * Destructors
   * ===================================*/

  /** Check if BORG is NULL */
  bool IsNull() const {
    return false;
  }

  /** Set BORG to NULL */
  void SetNull() {}

  /** Finalize the BORG */
  void shm_destroy_main();

  /**====================================
   * BORG Methods
   * ===================================*/

  /** Stores a blob into a set of buffers */
  RPC void LocalPlaceBlobInBuffers(const Blob &blob,
                                   std::vector<BufferInfo> &buffers);
  RPC void GlobalPlaceBlobInBuffers(const Blob &blob,
                                    std::vector<BufferInfo> &buffers);

  /** Stores a blob into a set of buffers */
  RPC void LocalReadBlobFromBuffers(Blob &blob,
                                    std::vector<BufferInfo> &buffers);
  Blob GlobalReadBlobFromBuffers(std::vector<BufferInfo> &buffers);

  /** Re-organize blobs based on a score */
  void GlobalOrganizeBlob(const std::string &bucket_name,
                          const std::string &blob_name,
                          float score);

 public:
  /**====================================
   * BORG Flushing methods
   * ===================================*/

  /** Flush all blobs registered in this daemon */
  static void LocalEnqueueFlushes();

  /** Actually process flush operations */
  static void LocalProcessFlushes(BorgIoThreadQueue &bq);

  /** Barrier for all flushing to complete */
  void LocalWaitForFullFlush();

  /** Barrier for all I/O in Hermes to flush */
  void GlobalWaitForFullFlush();
};

}  // namespace hermes

#endif  // HERMES_SRC_BUFFER_ORGANIZER_H_
