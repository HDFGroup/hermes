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

#include "apriori_prefetcher.h"
#include "hermes.h"

namespace hermes {

/** Constructor. Parse YAML schema. */
AprioriPrefetcher::AprioriPrefetcher() {
  auto &path = HERMES->server_config_.prefetcher_.apriori_schema_path_;
  auto real_path = hshm::ConfigParse::ExpandPath(path);
  HILOG(kDebug, "Start load apriori schema {}", real_path)
  try {
    YAML::Node yaml_conf = YAML::LoadFile(real_path);
    ParseSchema(yaml_conf);
    HILOG(kDebug, "Complete load of apriori schema {}", real_path)
  } catch (std::exception &e) {
    HELOG(kFatal, e.what())
  }
}

void ParseList(std::vector<std::string> &list, YAML::Node node) {
  list.reserve(node.size());
  for (YAML::Node sub_node : node) {
    list.emplace_back(sub_node.as<std::string>());
  }
}

/** Parse YAML schema. */
void AprioriPrefetcher::ParseSchema(YAML::Node &schema) {
  rank_info_.resize(schema.size());
  for (const auto &rank_node_pair : schema) {
    const YAML::Node& rank_node = rank_node_pair.first;
    const YAML::Node& rank_instrs = rank_node_pair.second;
    int rank = rank_node.as<int>();
    auto &instr_list = rank_info_[rank];
    for (YAML::Node instr_list_node : rank_instrs) {
      instr_list.emplace_back();
      auto &instr = instr_list.back();
      YAML::Node op_count_range_node = instr_list_node["op_count_range"];
      instr.min_op_count_ = op_count_range_node[0].as<size_t>();
      instr.max_op_count_ = op_count_range_node[1].as<size_t>();
      for (YAML::Node instr_node : instr_list_node["prefetch"]) {
        instr.promotes_.emplace_back();
        auto &promote = instr.promotes_.back();
        promote.bkt_name_ = instr_node["bucket_name"].as<std::string>();
        ParseList(promote.promote_, instr_node["promote_blobs"]);
        ParseList(promote.demote_, instr_node["demote_blobs"]);
      }
    }
  }
}

/** Prefetch based on YAML schema */
void AprioriPrefetcher::Prefetch(BufferOrganizer *borg,
                                 BinaryLog<IoStat> &log) {
  for (size_t rank = 0; rank < rank_info_.size(); ++rank) {
    size_t num_ops = log.GetRankLogSize((int)rank);
    auto &instr_list = rank_info_[rank];

    // Find the instruction to execute for this rank
    auto begin = instr_list.begin();
    auto cur = begin;
    for (; cur != instr_list.end(); ++cur) {
      auto &instr = *cur;
      if (instr.min_op_count_ <= num_ops && instr.max_op_count_ <= num_ops) {
        break;
      }
    }

    // First, demote blobs
    if (cur != instr_list.end()) {
      auto &instr = *cur;
      for (auto &promote_instr : instr.promotes_) {
        for (auto &blob_name : promote_instr.demote_) {
          borg->GlobalOrganizeBlob(promote_instr.bkt_name_,
                                   blob_name, 0);
        }
      }

      // Next, promote blobs
      for (auto &promote_instr : instr.promotes_) {
        for (auto &blob_name : promote_instr.promote_) {
          borg->GlobalOrganizeBlob(promote_instr.bkt_name_,
                                   blob_name, 1);
        }
      }
    }

    // Erase unneeded logs
    instr_list.erase(begin, cur);
  }
}

}  // namespace hermes
