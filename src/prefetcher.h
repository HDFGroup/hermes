//
// Created by lukemartinlogan on 10/18/22.
//

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

static bool operator==(const BlobID& first, const BlobID& second) {
  return first.as_int == second.as_int;
}

enum class IoType {
  kNone, kPut, kGet
};

struct GlobalThreadID {
  int rank_;
  int tid_;
  GlobalThreadID() : rank_(0), tid_(0) {}
  GlobalThreadID(int rank, int tid) : rank_(rank), tid_(tid) {}
};

struct IoLogEntry {
  VBucketID vbkt_id_;
  BucketID bkt_id_;
  BlobID blob_id_;
  std::string blob_name_;
  IoType type_;
  off_t off_;
  size_t size_;
  PrefetchContext pctx_;
  GlobalThreadID tid_;
  struct timespec timestamp_;
  bool historical_;
};

struct PrefetchStat {
  float max_time_;
  struct timespec start_;

  explicit PrefetchStat(float max_time, struct timespec &start) :
        max_time_(max_time), start_(start) {}

  float GetRemainingTime(const struct timespec *cur) {
    float diff = DiffTimespec(cur, &start_);
    return max_time_ - diff;
  }

 private:
  static float DiffTimespec(const struct timespec *left,
                            const struct timespec *right) {
    return (left->tv_sec - right->tv_sec)
           + (left->tv_nsec - right->tv_nsec) / 1000000000.0;
  }
};

struct PrefetchDecision {
  BucketID bkt_id_;
  BlobID blob_id_;
  std::string blob_name_;
  std::list<PrefetchStat> stats_;
  bool queue_later_;
  float est_xfer_time_;
  float new_score_;

  PrefetchDecision() : est_xfer_time_(-1), new_score_(-1) {}
  void AddStat(float est_access_time, struct timespec &start) {
    stats_.emplace_back(est_access_time, start);
  }
};

class PrefetchSchema {
 private:
  std::unordered_map<BlobID, PrefetchDecision> schema_;

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

  std::unordered_map<BlobID, PrefetchDecision>::iterator begin() {
    return schema_.begin();
  }

  std::unordered_map<BlobID, PrefetchDecision>::iterator end() {
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
  std::unordered_map<BlobID, PrefetchDecision> queue_later_;
  std::shared_ptr<api::Hermes> hermes_;
  float epsilon_;

 public:
  explicit Prefetcher() : max_length_(4096), epsilon_(.05) {}
  void SetHermes(std::shared_ptr<api::Hermes> &hermes) { hermes_ = hermes; }
  void SetLogLength(uint32_t max_length) { max_length_ = max_length; }
  void Log(IoLogEntry &entry);
  static bool LogIoStat(api::Hermes *hermes, IoLogEntry &entry);
  void Process();

 private:
  static size_t HashToNode(api::Hermes *hermes, IoLogEntry &entry);
  float EstimateBlobMovementTime(BlobID blob_id);
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
  ar & entry.blob_name_;
  ar & entry.type_;
  ar & entry.off_;
  ar & entry.size_;
  ar & entry.pctx_;
  ar & entry.tid_;
  // ar & entry.timestamp_;
  ar & entry.historical_;
}

}  // namespace hermes

#endif  // HERMES_SRC_PREFETCHER_H_
