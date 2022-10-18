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

/* Equal operators */
static bool operator==(const BlobID& first, const BlobID& second) {
  return first.as_int == second.as_int;
}
static bool operator==(const BucketID& first, const BucketID& second) {
  return first.as_int == second.as_int;
}
static bool operator==(const VBucketID& first, const VBucketID& second) {
  return first.as_int == second.as_int;
}

enum class PrefetchHint {
  kNone,
  kNoPrefetch,
  kFileSequential,
  kFileStrided,
  kMachineLearning
};

enum class IoType {
  kNone, kPut, kGet
};

struct PrefetchContext {
  PrefetchHint hint_;
  int read_ahead_;
  PrefetchContext() : hint_(PrefetchHint::kNone) {}
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
  IoType type_;
  off_t off_;
  size_t size_;
  PrefetchContext pctx_;
  GlobalThreadID tid_;
  struct timespec timestamp_;
  bool historical_;
};

class Prefetcher {
 private:
  uint32_t max_length_;
  std::list<IoLogEntry> log_;
  thallium::mutex lock_;
 public:
  Prefetcher() : max_length_(4096) {}
  void SetLogLength(uint32_t max_length);
  void Log(IoLogEntry &entry) {
    lock_.lock();
    if (log_.size() == max_length_) {
      log_.pop_front();
    }
    log_.emplace_back(entry);
    lock_.unlock();
  }
  static void LogIoStat(IoLogEntry &entry) {
  }
  void Process();
};

struct PrefetchDecision {
  BlobID blob_id;
  std::list<float> access_times_;
  float updated_score_;
};

class PrefetchSchema {
 private:
  std::unordered_map<BlobID, PrefetchDecision> schema_;

 public:
  void emplace(PrefetchDecision &decision) {
    schema_.emplace(decision.blob_id, decision);
  }
};

class PrefetchAlgorithm {
 public:
  virtual void Process(std::list<IoLogEntry> &log,
                       PrefetchSchema &schema) = 0;
};

/** RPC SERIALIZERS */

// PrefetchHint
template <typename A>
void save(A &ar, PrefetchHint &hint) {
  ar << static_cast<int>(hint);
}
template <typename A>
void load(A &ar, PrefetchHint &hint) {
  int hint_i;
  ar >> hint_i;
  hint = static_cast<PrefetchHint>(hint_i);
}

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

// PrefetchContext
template <typename A>
void serialize(A &ar, PrefetchContext &pctx) {
  ar & pctx.hint_;
  ar & pctx.read_ahead_;
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

}

#endif  // HERMES_SRC_PREFETCHER_H_
