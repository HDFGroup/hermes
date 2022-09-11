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

#include "hermes.h"
#include "dpe/minimize_io_time.h"
#include "test_utils.h"
#include "utils.h"

using namespace hermes;  // NOLINT(*)

void MinimizeIoTimePlaceBlob(std::vector<size_t> &blob_sizes,
                             std::vector<PlacementSchema> &schemas,
                             testing::TargetViewState &node_state) {
  std::vector<PlacementSchema> schemas_tmp;

  std::cout << "\nMinimizeIoTimePlacement to place blob of size "
            << blob_sizes[0] << " to targets\n" << std::flush;
  std::vector<TargetID> targets =
    testing::GetDefaultTargets(node_state.num_devices);
  api::Context ctx;
  ctx.policy = hermes::api::PlacementPolicy::kMinimizeIoTime;
  ctx.minimize_io_time_options = api::MinimizeIoTimeOptions(0, 0, true);
  Status result = MinimizeIoTime().Placement(blob_sizes,
                                          node_state.bytes_available,
                                          targets,
                                          ctx,
                                          schemas_tmp);
  if (result.Failed()) {
    std::cout << "\nMinimizeIoTimePlacement failed\n" << std::flush;
    exit(1);
  }

  for (auto it = schemas_tmp.begin(); it != schemas_tmp.end(); ++it) {
    PlacementSchema schema = AggregateBlobSchema((*it));
    Assert(schemas.size() <= static_cast<size_t>(node_state.num_devices));
    schemas.push_back(schema);
  }

  u64 placed_size {0};
  for (auto schema : schemas) {
    placed_size += UpdateDeviceState(schema, node_state);
  }

  std::cout << "\nUpdate Device State:\n";
  testing::PrintNodeState(node_state);
  u64 total_sizes = std::accumulate(blob_sizes.begin(), blob_sizes.end(), 0);
  Assert(placed_size == total_sizes);
}

int main() {
  testing::TargetViewState node_state = testing::InitDeviceState();

  Assert(node_state.num_devices == 4);
  std::cout << "Device Initial State:\n";
  testing::PrintNodeState(node_state);

  std::vector<size_t> blob_sizes1(1, MEGABYTES(10));
  std::vector<PlacementSchema> schemas1;
  MinimizeIoTimePlaceBlob(blob_sizes1, schemas1, node_state);
  Assert(schemas1.size() == blob_sizes1.size());

  std::vector<size_t> blob_sizes2(1, MEGABYTES(1));
  std::vector<PlacementSchema> schemas2;
  MinimizeIoTimePlaceBlob(blob_sizes2, schemas2, node_state);
  Assert(schemas2.size() == blob_sizes2.size());

  return 0;
}
