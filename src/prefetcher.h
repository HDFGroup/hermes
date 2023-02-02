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

#ifndef HERMES_SRC_PREFETCHER_H_
#define HERMES_SRC_PREFETCHER_H_

#include "hermes.h"
#include "hermes_types.h"
#include "rpc_thallium.h"
#include <cstdlib>
#include <set>
#include <list>
#include <vector>
#include <unordered_map>

namespace hermes {

using hermes::api::PrefetchContext;
using hermes::api::PrefetchHint;

enum class IoType {
  kNone, kPut, kGet
};

struct GlobalThreadID {
  int rank_;
  int tid_;
  GlobalThreadID() : rank_(0), tid_(0) {}
  explicit GlobalThreadID(int rank) : rank_(rank) {
    ABT_unit_id tid_argo;
    ABT_thread_self_id(&tid_argo);
    tid_ = tid_argo;
  }

  bool operator==(const GlobalThreadID &other) const {
    return (rank_ == other.rank_ && tid_ == other.tid_);
  }
};

struct UniqueBucket {
  VBucketId vbkt_id_;
  BucketId bkt_id_;

  UniqueBucket() = default;
  UniqueBucket(VBucketId vbkt_id, BucketId bkt_id) :
       vbkt_id_(vbkt_id), bkt_id_(bkt_id) {}

  bool operator==(const UniqueBucket &other) const {
    return (vbkt_id_ == other.vbkt_id_ &&
            bkt_id_ == other.bkt_id_);
  }
};

struct IoLogEntry {
  VBucketId vbkt_id_;
  BucketId bkt_id_;
  BlobId blob_id_;
  IoType type_;
  off_t off_;
  size_t size_;
  PrefetchContext pctx_;
  GlobalThreadID tid_;
  bool historical_;
  struct timespec timestamp_;
};

struct PrefetchStat {
  float est_next_time_;
  struct timespec start_;
  float decay_;

  PrefetchStat() : est_next_time_(0), decay_(-1) {}

  explicit PrefetchStat(float est_next_time, struct timespec &start) :
      est_next_time_(est_next_time), start_(start), decay_(-1) {}

  explicit PrefetchStat(float decay) : decay_(decay) {}

  float TimeLeftOnIo(float est_xfer_time, const struct timespec *cur) {
    float diff = DiffTimespec(cur, &start_);
    /*LOG(INFO) << "Time since I/O submitted: " << diff << std::endl;
    LOG(INFO) << "Est I/O time: " << est_xfer_time << std::endl;*/
    return est_xfer_time - diff;
  }

  float TimeToNextIo(const struct timespec *cur) {
    float diff = DiffTimespec(cur, &start_);
    return est_next_time_ - diff;
  }

  static float DiffTimespec(const struct timespec *left,
                            const struct timespec *right) {
    return (left->tv_sec - right->tv_sec)
           + (left->tv_nsec - right->tv_nsec) / 1000000000.0;
  }
};

struct PrefetchDecision {
  BucketId bkt_id_;
  BlobId blob_id_;
  std::string blob_name_;
  std::list<PrefetchStat> stats_;
  bool queue_later_;
  float est_xfer_time_;
  float new_score_;
  bool decay_;

  PrefetchDecision() : queue_later_(false),
                       est_xfer_time_(-1),
                       new_score_(-1),
                       decay_(false) {}
  void AddStat(float est_access_time, struct timespec &start) {
    stats_.emplace_back(est_access_time, start);
  }
  void AddStat(float decay) {
    stats_.emplace_back(decay);
  }
};

class PrefetchSchema {
 private:
  std::unordered_map<BlobId, PrefetchDecision> schema_;

 public:
  void emplace(PrefetchDecision &decision) {
    auto prior_decision_iter = schema_.find(decision.blob_id_);
    if (prior_decision_iter == schema_.end()) {
      schema_.emplace(decision.blob_id_, decision);
      return;
    }
    auto &[blob_id, prior_decision] = (*prior_decision_iter);
    prior_decision.est_xfer_time_ = decision.est_xfer_time_;
    prior_decision.stats_.splice(
        prior_decision.stats_.end(),
        decision.stats_);
  }

  std::unordered_map<BlobId, PrefetchDecision>::iterator begin() {
    return schema_.begin();
  }

  std::unordered_map<BlobId, PrefetchDecision>::iterator end() {
    return schema_.end();
  }
};

class PrefetchAlgorithm {
 public:
  virtual void Process(std::list<IoLogEntry> &log,
                       PrefetchSchema &schema) = 0;
};

class Prefetcher {
 private:
  uint32_t max_length_;
  thallium::mutex lock_;
  std::list<IoLogEntry> log_;
  std::unordered_map<BlobId, PrefetchDecision> queue_later_;

 public:
  float max_wait_xfer_;
  float max_wait_sec_;
  float epsilon_;

 public:
  std::shared_ptr<api::Hermes> hermes_;

  Prefetcher() : max_length_(4096), max_wait_xfer_(10), max_wait_sec_(60),
                 epsilon_(.05) {}
  void SetHermes(std::shared_ptr<api::Hermes> &hermes) { hermes_ = hermes; }
  void SetLogLength(uint32_t max_length) { max_length_ = max_length; }
  void Log(IoLogEntry &entry);
  static bool LogIoStat(api::Hermes *hermes, IoLogEntry &entry);
  void Process();

 private:
  static size_t HashToNode(api::Hermes *hermes, IoLogEntry &entry);
  float EstimateBlobMovementTime(BlobId blob_id);
  void CalculateBlobScore(struct timespec &ts,
                          PrefetchDecision &decision);
};

/** RPC SERIALIZERS */

// IoType
template <typename A>
void save(A &ar, IoType &hint) {
  ar << static_cast<int>(hint);
}
template <typename A>
void load(A &ar, IoType &hint) {
  int hint_i;
  ar >> hint_i;
  hint = static_cast<IoType>(hint_i);
}

// struct timespec
template <typename A>
void save(A &ar, struct timespec &ts) {
  ar << ts.tv_sec;
  ar << ts.tv_nsec;
}
template <typename A>
void load(A &ar, struct timespec &ts) {
  ar >> ts.tv_sec;
  ar >> ts.tv_nsec;
}

// GlobalThreadID
template <typename A>
void serialize(A &ar, GlobalThreadID &tid) {
  ar & tid.rank_;
  ar & tid.tid_;
}

// IoLogEntry
template <typename A>
void serialize(A &ar, IoLogEntry &entry) {
  ar & entry.vbkt_id_;
  ar & entry.bkt_id_;
  ar & entry.blob_id_;
  ar & entry.type_;
  ar & entry.off_;
  ar & entry.size_;
  ar & entry.pctx_;
  ar & entry.tid_;
  // ar & entry.timestamp_;
  ar & entry.historical_;
}

}  // namespace hermes

namespace std {
template <>
struct hash<hermes::GlobalThreadID> {
  std::size_t operator()(const hermes::GlobalThreadID &key) const {
    size_t h1 = std::hash<int>{}(key.rank_);
    size_t h2 = std::hash<int>{}(key.tid_);
    size_t hash = h1^h2;
    return hash;
  }
};

template <>
struct hash<hermes::UniqueBucket> {
  std::size_t operator()(const hermes::UniqueBucket &key) const {
    size_t h1 = std::hash<hermes::VBucketId>{}(key.vbkt_id_);
    size_t h2 = std::hash<hermes::BucketId>{}(key.bkt_id_);
    size_t hash = h1^h2;
    return hash;
  }
};
}  // namespace std

#endif  // HERMES_SRC_PREFETCHER_H_
