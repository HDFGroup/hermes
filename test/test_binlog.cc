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

#include <cstdlib>
#include <string>

#include "binlog.h"
#include "hermes_types.h"
#include "basic_test.h"
#include <filesystem>

namespace hapi = hermes::api;
namespace stdfs = std::filesystem;

void MainPretest() {
}

void MainPosttest() {
}

std::vector<hermes::IoStat> create_stats(
  size_t bytes, int num_ranks, size_t &entries_per_rank) {
  std::vector<hermes::IoStat> stats;
  size_t max_entries = bytes / sizeof(hermes::IoStat);
  entries_per_rank = max_entries / num_ranks;
  stats.reserve(num_ranks * entries_per_rank);
  for (int rank = 0; rank < num_ranks; ++rank) {
    for (size_t i = 0; i < entries_per_rank; ++i) {
      stats.emplace_back();
      auto &stat = stats.back();
      stat.rank_ = rank;
      stat.type_ = (i % 2) ? hermes::IoType::kRead : hermes::IoType::kWrite;
      stat.blob_id_ = hermes::BlobId(i, rank);
      stat.blob_size_ = 8 * (i + 1);
    }
  }
  return stats;
}

void verify_log(hermes::BinaryLog<hermes::IoStat> log,
                int num_ranks, size_t entries_per_rank) {
  for (int rank = 0; rank < num_ranks; ++rank) {
    hermes::IoStat stat;
    size_t i = 0;
    while (log.GetNextEntry(rank, stat)) {
      REQUIRE(stat.blob_id_.node_id_ == rank);
      REQUIRE(stat.blob_id_.unique_ == i);
      i += 1;
    }
    REQUIRE(i == entries_per_rank);
  }
}

TEST_CASE("TestBinlog") {
  int num_ranks = 16;
  size_t log_bytes = MEGABYTES(1);
  size_t chunk_bytes = log_bytes / 4;
  size_t entries_per_rank;
  std::string path =  "/tmp/log.bin";

  // Create chunk
  std::vector<hermes::IoStat> stats = create_stats(
    chunk_bytes, num_ranks, entries_per_rank);

  // Attempt flushing the log
  hermes::BinaryLog<hermes::IoStat> log(path, log_bytes);
  log.Ingest(stats);
  verify_log(log, num_ranks, entries_per_rank);
  log.Flush();
  REQUIRE(stdfs::file_size(path) == 0);

  // Actually flush the log when capacity reached
  log.Ingest(stats);
  log.Ingest(stats);
  log.Ingest(stats);
  log.Flush();
  REQUIRE(stdfs::file_size(path) > 0);
}
