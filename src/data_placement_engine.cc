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

#include "data_placement_engine_factory.h"
//#include "data_placement_engine.h"

#include <assert.h>
#include <glpk.h>
#include <math.h>

#include <utility>
#include <random>
#include <map>

#include "hermes.h"
#include "hermes_types.h"
#include "metadata_management.h"
#include "metadata_management_internal.h"
#include "metadata_storage.h"

namespace hermes {

using hermes::api::Status;

std::vector<int> DPE::GetValidSplitChoices(size_t blob_size) {
  int split_option = 10;
  // Split the blob if size is greater than 64KB
  if (blob_size > KILOBYTES(64) && blob_size <= KILOBYTES(256)) {
    split_option = 2;
  } else if (blob_size > KILOBYTES(256) && blob_size <= MEGABYTES(1)) {
    split_option = 5;
  } else if (blob_size > MEGABYTES(1) && blob_size <= MEGABYTES(4)) {
    split_option = 8;
  }

  constexpr int split_range[] = { 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024 };
  std::vector<int> result(split_range, split_range + split_option - 1);

  return result;
}

bool DPE::SplitBlob(size_t blob_size) {
  bool result = false;
  std::random_device dev;
  std::mt19937 rng(dev());

  if (blob_size > KILOBYTES(64)) {
    std::uniform_int_distribution<std::mt19937::result_type> distribution(0, 1);
    if (distribution(rng) == 1) {
      result = true;
    }
  }

  return result;
}

void DPE::GetSplitSizes(size_t blob_size, std::vector<size_t> &output) {
  std::random_device dev;
  std::mt19937 rng(dev());

  std::vector<int> split_choice = GetValidSplitChoices(blob_size);

  // Random pickup a number from split_choice to split the blob
  std::uniform_int_distribution<std::mt19937::result_type>
    position(0, split_choice.size()-1);
  int split_num = split_choice[position(rng)];

  size_t blob_each_portion {blob_size/split_num};
  for (int j {0}; j < split_num - 1; ++j) {
    output.push_back(blob_each_portion);
  }
  output.push_back(blob_size - blob_each_portion*(split_num-1));
}

PlacementSchema AggregateBlobSchema(PlacementSchema &schema) {
  std::unordered_map<u64, u64> place_size;
  PlacementSchema result;

  for (auto [size, target] : schema) {
    place_size.insert({target.as_int, 0});
    place_size[target.as_int] += size;
  }
  for (auto [target, size] : place_size) {
    TargetID id = {};
    id.as_int = target;
    result.push_back(std::make_pair(size, id));
  }

  return result;
}

enum Topology {
  Topology_Local,
  Topology_Neighborhood,
  Topology_Global,

  Topology_Count
};

Status CalculatePlacement(SharedMemoryContext *context, RpcContext *rpc,
                          const std::vector<size_t> &blob_sizes,
                          std::vector<PlacementSchema> &output,
                          const api::Context &api_context) {
  std::vector<PlacementSchema> output_tmp;
  Status result;

  // NOTE(chogan): Start with local targets and gradually expand the target
  // list until the placement succeeds.
  for (int i = 0; i < Topology_Count; ++i) {
    std::vector<TargetID> targets;

    switch (i) {
      case Topology_Local: {
        // TODO(chogan): @optimization We can avoid the copy here when getting
        // local targets by just getting a pointer and length.
        targets = LocalGetNodeTargets(context);
        break;
      }
      case Topology_Neighborhood: {
        targets = GetNeighborhoodTargets(context, rpc);
        break;
      }
      case Topology_Global: {
        // TODO(chogan): GetGlobalTargets(context, rpc);
        break;
      }
    }

    if (targets.size() == 0) {
      result = DPE_PLACEMENTSCHEMA_EMPTY;
      continue;
    }

    std::vector<u64> node_state =
      GetRemainingTargetCapacities(context, rpc, targets);
    std::vector<f32> bandwidths = GetBandwidths(context, targets);
    auto dpe = DPEFactory().Get(api_context.policy);
    result = dpe->Placement(blob_sizes, node_state, bandwidths,
                   targets, output_tmp, api_context);

    if (!result.Failed()) {
      break;
    }
  }

  // Aggregate placement schemas from the same target
  if (result.Succeeded()) {
    for (auto it = output_tmp.begin(); it != output_tmp.end(); ++it) {
      PlacementSchema schema = AggregateBlobSchema((*it));
      if (schema.size() == 0) {
        result = DPE_PLACEMENTSCHEMA_EMPTY;
        LOG(ERROR) << result.Msg();
        return result;
      }
      output.push_back(schema);
    }
  }

  return result;
}

}  // namespace hermes
