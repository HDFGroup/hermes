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

#include <iostream>
#include <random>
#include <map>
#include <chrono>

#include "hermes.h"
#include "utils.h"
#include "test_utils.h"
#include "data_placement_engine.h"

/* example usage: ./bin/dpe_bench -m -s 4096 */

using namespace hermes;  // NOLINT(*)
using std::chrono::time_point;
const auto now = std::chrono::high_resolution_clock::now;

const u64 dpe_total_targets = 10;
const size_t dpe_total_num_blobs = 10;
const size_t dpe_total_blob_size = GIGABYTES(10);
const size_t kDefaultBlobSize = KILOBYTES(4);

void PrintUsage(char *program) {
  fprintf(stderr, "Usage %s [-r]\n", program);
  fprintf(stderr, "  -m\n");
  fprintf(stderr, "     Use Random policy (default).\n");
  fprintf(stderr, "  -n\n");
  fprintf(stderr, "     Use RoundRobin policy.\n");
  fprintf(stderr, "  -o\n");
  fprintf(stderr, "     Use MinimizeIoTime policy.\n");
  fprintf(stderr, "  -r\n");
  fprintf(stderr, "     Chooese blob size range.\n");
  fprintf(stderr, "     s/S: Small blob range (0, 64KB].\n");
  fprintf(stderr, "     m/M: Medium blob range (64KB, 1MB].\n");
  fprintf(stderr, "     l/L: Large blob range (1MB, 4MB].\n");
  fprintf(stderr, "     x/X: Xlarge blob range (4MB, 64MB].\n");
  fprintf(stderr, "     h/H: Huge blob size is 1GB.\n");
  fprintf(stderr, "  -s\n");
  fprintf(stderr, "     Specify exact blob size.\n");
  fprintf(stderr, "     Blob size option: 4KB, 64KB, 1MB, 4MB, 64MB.\n");
}

int main(int argc, char **argv) {
  api::PlacementPolicy policy {api::PlacementPolicy::kRandom};
  bool fixed_total_num_blobs {true}, fixed_total_blob_size {false};
  int option = -1;
  char *rvalue = NULL;
  size_t each_blob_size = kDefaultBlobSize;
  size_t total_placed_size;
  double dpe_seconds;
  api::Status result;

  while ((option = getopt(argc, argv, "mnor:s:")) != -1) {
    switch (option) {
      case 'm':
        policy = api::PlacementPolicy::kRandom;
        break;
      case 'n':
        policy = api::PlacementPolicy::kRoundRobin;
        break;
      case 'o':
        policy = api::PlacementPolicy::kMinimizeIoTime;
        break;
      case 'r':
        fixed_total_blob_size = true;
        if (fixed_total_num_blobs)
          fixed_total_num_blobs =  false;
        rvalue = optarg;
        break;
      case 's':
        fixed_total_num_blobs = true;
        each_blob_size = atoi(optarg);
        break;
      default:
        PrintUsage(argv[0]);
        policy = api::PlacementPolicy::kRandom;
        fixed_total_blob_size = true;
        each_blob_size = kDefaultBlobSize;
        std::cout << "Using Random policy for data placement engine.\n"
                  << "Using fixed number of blobs of size 4KB for test.\n\n";
    }
  }

  if (fixed_total_num_blobs && fixed_total_blob_size) {
    std::cout << "DPE benchmark uses fixed total blob size\n"
              << "or fixed total number of blbs.\n"
              << "Use default fixed total number of blbs now\n\n";
  }

  std::vector<size_t> blob_sizes;
  if (fixed_total_blob_size) {
    testing::BlobSizeRange blob_range;

    if (rvalue[0] == 's' || rvalue[0] == 'S') {
      blob_range = testing::BlobSizeRange::kSmall;
    } else if (rvalue[0] == 'm' || rvalue[0] == 'M') {
      blob_range = testing::BlobSizeRange::kMedium;
    } else if (rvalue[0] == 'l' || rvalue[0] == 'L') {
      blob_range = testing::BlobSizeRange::kLarge;
    } else if (rvalue[0] == 'x' || rvalue[0] == 'X') {
      blob_range = testing::BlobSizeRange::kXLarge;
    } else if (rvalue[0] == 'h' || rvalue[0] == 'H') {
      blob_range = testing::BlobSizeRange::kHuge;
    } else {
      blob_range = testing::BlobSizeRange::kSmall;
      std::cout << "No blob range is setup.\n"
                << "Choose small blob size range (0, 64KB] to test.\n\n";
    }

    blob_sizes = testing::GenFixedTotalBlobSize(dpe_total_blob_size,
                                                blob_range);
    total_placed_size = dpe_total_blob_size;
  } else {
    blob_sizes.resize(dpe_total_blob_size);
    fill(blob_sizes.begin(), blob_sizes.end(), each_blob_size);
    total_placed_size = each_blob_size * dpe_total_num_blobs;
  }

  testing::TargetViewState tgt_state =
                           testing::InitDeviceState(dpe_total_targets);

  assert(tgt_state.num_devices == dpe_total_targets);

  std::vector<PlacementSchema> output_tmp, schemas;
  std::vector<TargetID> targets =
                        testing::GetDefaultTargets(tgt_state.num_devices);

  switch (policy) {
    case api::PlacementPolicy::kRandom: {
      std::multimap<u64, TargetID> ordered_cap;
      for (auto i = 0; i < tgt_state.num_devices; ++i) {
        ordered_cap.insert(std::pair<u64, TargetID>(
                           tgt_state.bytes_available[i], targets[i]));
      }

      std::cout << "DPE benchmark uses Random placement.\n\n";
      time_point start_tm = now();
      result = RandomPlacement(blob_sizes, ordered_cap, output_tmp);
      time_point end_tm = now();
      dpe_seconds = std::chrono::duration<double>(end_tm - start_tm).count();
      break;
    }
    case api::PlacementPolicy::kRoundRobin: {
      time_point start_tm = now();
      result = RoundRobinPlacement(blob_sizes, tgt_state.bytes_available,
                                   output_tmp, targets, false);
      std::cout << "DPE benchmark uses RoundRobin placement.\n\n";
      time_point end_tm = now();
      dpe_seconds = std::chrono::duration<double>(end_tm - start_tm).count();
      break;
    }
    case api::PlacementPolicy::kMinimizeIoTime: {
      std::cout << "DPE benchmark uses MinimizeIoTime placement.\n\n";
      time_point start_tm = now();
      result = MinimizeIoTimePlacement(blob_sizes, tgt_state.bytes_available,
                                       tgt_state.bandwidth, targets,
                                       output_tmp);
      time_point end_tm = now();
      dpe_seconds = std::chrono::duration<double>(end_tm - start_tm).count();
      break;
    }
  }

  u64 placed_size {0};
  for (auto schema : output_tmp) {
    placed_size += testing::UpdateDeviceState(schema, tgt_state);
  }
  Assert(placed_size == total_placed_size);

  // Aggregate placement schemas from the same target
  if (result.Succeeded()) {
    for (auto it = output_tmp.begin(); it != output_tmp.end(); ++it) {
      PlacementSchema schema = AggregateBlobSchema((*it));
      assert(schema.size() > 0);
      schemas.push_back(schema);
    }
  }

  std::cout << "Total DPE time: " << dpe_seconds << '\n';

  return 0;
}
