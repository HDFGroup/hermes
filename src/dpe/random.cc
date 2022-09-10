//
// Created by lukemartinlogan on 9/9/22.
//

#include "random.h"

#include <utility>
#include <random>
#include <map>

namespace hermes {

Status Random::AddSchema(std::multimap<u64, TargetID> &ordered_cap,
                       size_t blob_size, PlacementSchema &schema) {
  std::random_device rd;
  std::mt19937 gen(rd());
  Status result;

  auto itlow = ordered_cap.lower_bound(blob_size);
  if (itlow == ordered_cap.end()) {
    result = DPE_RANDOM_FOUND_NO_TGT;
    LOG(ERROR) << result.Msg();
  } else {
    // distance from lower bound to the end
    std::uniform_int_distribution<>
        dst_dist(1, std::distance(itlow, ordered_cap.end()));
    size_t dst_relative = dst_dist(gen);
    std::advance(itlow, dst_relative-1);
    ordered_cap.insert(std::pair<u64, TargetID>((*itlow).first-blob_size,
                                                (*itlow).second));

    schema.push_back(std::make_pair(blob_size, (*itlow).second));
    ordered_cap.erase(itlow);
  }

  return result;
}

void Random::GetOrderedCapacities(const std::vector<u64> &node_state,
                                  const std::vector<TargetID> &targets,
                                  std::multimap<u64, TargetID> &ordered_cap) {
  for (size_t i = 0; i < node_state.size(); ++i) {
    ordered_cap.insert(std::pair<u64, TargetID>(node_state[i], targets[i]));
  }
}

Status Random::Placement(const std::vector<size_t> &blob_sizes,
                         const std::vector<u64> &node_state,
                         const std::vector<f32> &bandwidths,
                         const std::vector<TargetID> &targets,
                         std::vector<PlacementSchema> &output,
                         const api::Context &ctx) {
  Status result;
  std::multimap<u64, TargetID> ordered_cap;

  GetOrderedCapacities(node_state, targets, ordered_cap);

  for (size_t i {0}; i < blob_sizes.size(); ++i) {
    PlacementSchema schema;

    // Split the blob
    if (SplitBlob(blob_sizes[i])) {
      // Construct the vector for the splitted blob
      std::vector<size_t> new_blob_size;
      GetSplitSizes(blob_sizes[i], new_blob_size);

      for (size_t k {0}; k < new_blob_size.size(); ++k) {
        result = AddSchema(ordered_cap, new_blob_size[k], schema);

        if (!result.Succeeded()) {
          break;
        }
      }
    } else {
      // Blob size is less than 64KB or do not split
      result = AddSchema(ordered_cap, blob_sizes[i], schema);
      if (!result.Succeeded()) {
        return result;
      }
    }
    output.push_back(schema);
  }

  return result;
}

}