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

#ifndef HERMES_SRC_BINLOG_H_
#define HERMES_SRC_BINLOG_H_

#include <hermes_shm/types/bitfield.h>
#include "data_structures.h"
#include <vector>
#include <string>
#include <cereal/archives/binary.hpp>
#include <cereal/types/vector.hpp>
#include <sstream>

namespace hermes {

template<typename T>
struct BinaryLogRank {
  std::vector<T> cache_;  /**< Cached log entries */
  size_t off_;  /**< Prefetcher's offset in the cache */
  size_t num_cached_;  /**< The number of entries cached int the log */

  /** Constructor */
  BinaryLogRank() : off_(0), num_cached_(0) {}

  /** Number of elements in the cache */
  size_t size() const {
    return cache_.size();
  }

  /** Number of touched elements / index of first untouched element */
  size_t touched() const {
    return off_;
  }

  /** Number of untouched elements */
  size_t untouched() const {
    return size() - off_;
  }

  /** Number of uncached elements */
  size_t uncached() {
    return size() - num_cached_;
  }

  /** Increment the number of cached elements */
  void increment_cached() {
    num_cached_ += 1;
  }

  /** Get the next untouched cached entry */
  bool next(T &next) {
    if (off_ >= cache_.size()) { return false; }
    next = cache_[off_];
    off_ += 1;
    return true;
  }

  /** Reserve more space */
  void reserve(size_t size) {
    cache_.reserve(size);
  }

  /** Emplace an entry to the back of the cache log */
  void emplace_back(const T &entry) {
    cache_.emplace_back(entry);
  }

  /** Remove touched elements from the cache log */
  size_t clear_touched() {
    size_t num_touched = touched();
    cache_.erase(cache_.begin(), cache_.begin() + num_touched);
    if (touched() <= num_cached_) {
      num_cached_ -= num_touched;
    } else {
      num_cached_ = 0;
    }
    off_ = 0;
    return num_touched;
  }
};

/**
 * A simple file-per-process log format for storing
 * execution traces.
 *
 * This assumes only a single thread modifies or reads
 * from the log. This is intded to be used internally
 * by the prefetcher.
 * */
template<typename T>
class BinaryLog {
 public:
  std::vector<BinaryLogRank<T>> cache_;  /**< The cached log entries */
  size_t max_ingest_;  /**< Max number of elements to cache before flush */
  size_t cur_entry_count_;  /**< Total number of cached entries */
  std::string path_;  /**< Path to the backing log file */

 public:
  /** Constructor. */
  BinaryLog(const std::string &path,
            size_t max_ingest_bytes) :
    max_ingest_(max_ingest_bytes / sizeof(T)),
    cur_entry_count_(0),
    path_(path) {
    // Create + truncate the file
    // This is ok because the Hermes daemons are assumed to be spawned before
    // applications start running.
    std::ofstream output_file(path_);
  }

  /**
   * Appends all entries in the queue to the cache.
   * */
  void Ingest(const hipc::mpsc_queue<T> &queue) {
    T entry;
    while (!queue.pop(entry).IsNull()) {
      AppendEntry(entry);
    }
  }

  /**
   * Appends all entries in the vector to the cache.
   * */
  void Ingest(const std::vector<T> &queue) {
    for (auto &entry : queue) {
      AppendEntry(entry);
    }
  }

  /**
   * Get the next entry corresponding to the rank
   * */
  bool GetNextEntry(int rank, T &entry) {
    while (cache_[rank].untouched() == 0 && Load(max_ingest_)) {}
    return cache_[rank].next(entry);
  }

  /**
   * Flush all entries to the backing log
   * */
  void Flush(bool force = false) {
    if (!force && cur_entry_count_ < max_ingest_) {
      return;
    }

    // Serialize all contents into the log file
    if (path_.size()) {
      std::ofstream output_file(path_, std::ios::out | std::ios::app);
      cereal::BinaryOutputArchive oarch(output_file);
      for (auto &rank_cache : cache_) {
        for (size_t i = rank_cache.uncached(); i < rank_cache.size(); ++i) {
          auto &entry = rank_cache.cache_[i];
          oarch(entry);
          rank_cache.increment_cached();
        }
      }
    }

    // Remove all touched entries from the cache
    for (auto &rank_cache : cache_) {
      cur_entry_count_ -= rank_cache.clear_touched();
    }
  }

 private:
  /** Appends an entry to the cache */
  void AppendEntry(const T &entry) {
    if (entry.rank_ >= (int)cache_.size()) {
      cache_.resize(entry.rank_ + 1);
    }
    if (cache_[entry.rank_].size() == 0) {
      cache_[entry.rank_].reserve(8192);
    }
    cache_[entry.rank_].emplace_back(entry);
    cur_entry_count_ += 1;
  }

  /**
   * Load data from the log into memory
   *
   * @return true when there is still data to load from the file, false
   * otherwise
   * */
  bool Load(size_t num_entries) {
    std::vector<T> buffer;
    buffer.reserve(num_entries);
    std::ifstream input_file(path_, std::ios::in);
    cereal::BinaryInputArchive iarch(input_file);
    while (!input_file.eof()) {
      buffer.emplace_back();
      iarch(buffer.back());
    }
    Ingest(buffer);
    return !input_file.eof();
  }
};

}  // namespace hermes

#endif  // HERMES_SRC_BINLOG_H_
