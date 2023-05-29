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
  std::vector<T> log_;  /**< Cached log entries */
  size_t backend_off_;  /**< Entry offset in the backend file */

  BinaryLogRank() : backend_off_(0) {}
};

/**
 * A simple file-per-process log format for storing
 * execution traces.
 *
 * This assumes only a single thread modifies or reads
 * from the log. Intended for internal use by prefetcher.
 * */
template<typename T>
class BinaryLog {
 public:
  std::vector<BinaryLogRank<T>> cache_;  /**< The cached log entries */
  size_t max_ingest_;  /**< Max number of elements to cache before flush */
  size_t cur_entry_count_;  /**< Total number of cached entries */
  std::string path_;  /**< Path to the backing log file */

 public:
  /** Default Constructor*/
  BinaryLog() : max_ingest_(0), cur_entry_count_(0) {}

  /** Constructor. */
  void Init(const std::string &path,
            size_t max_ingest_bytes) {
    max_ingest_ = max_ingest_bytes / sizeof(T);
    path_ = path;
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
  bool GetEntry(int rank, size_t off, T &entry) {
    auto &cache = cache_[rank];
    if (off < cache.backend_off_ + cache.log_.size()) {
      entry = cache.log_[off];
      return true;
    }
    return false;
  }

  /**
   * Flush all entries to the backing log
   * */
  void Flush(bool force = false) {
    if (!force && cur_entry_count_ < max_ingest_) {
      return;
    }

    // Serialize all contents into the log file
    if (path_.empty()) {
      std::ofstream output_file(path_, std::ios::out | std::ios::app);
      cereal::BinaryOutputArchive oarch(output_file);
      for (auto &cache : cache_) {
        for (size_t i = 0; i < cache.log_.size(); ++i) {
          auto &entry = cache.log_[i];
          oarch(entry);
          cache.backend_off_ += 1;
        }
        cache.log_.clear();
      }
    }
    cur_entry_count_ = 0;
  }

 private:
  /** Appends an entry to the cache */
  void AppendEntry(const T &entry) {
    if (entry.rank_ >= (int)cache_.size()) {
      cache_.resize(entry.rank_ + 1);
    }
    auto &cache = cache_[entry.rank_];
    if (cache.log_.size() == 0) {
      cache.log_.reserve(8192);
    }
    cache.log_.emplace_back(entry);
    cur_entry_count_ += 1;
  }
};

}  // namespace hermes

#endif  // HERMES_SRC_BINLOG_H_
