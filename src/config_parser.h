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

#ifndef HERMES_CONFIG_PARSER_H_
#define HERMES_CONFIG_PARSER_H_

#include <float.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <yaml-cpp/yaml.h>

#include <ostream>
#include <string>

#include <glog/logging.h>

#include "hermes_types.h"
#include "utils.h"
#include "memory_management.h"

namespace hermes {

void PrintExpectedAndFail(const std::string &expected, u32 line_number = 0) {
  std::ostringstream msg;
  msg << "Configuration parser expected '" << expected << "'";
  if (line_number > 0) {
    msg << " on line " << line_number;
  }
  msg << "\n";

  LOG(FATAL) << msg.str();
}

void RequireNumDevices(Config *config) {
  if (config->num_devices == 0) {
    LOG(FATAL) << "The configuration variable 'num_devices' must be defined "
               << "first" << std::endl;
  }
}

void RequireNumSlabs(Config *config) {
  if (config->num_slabs == 0) {
    LOG(FATAL) << "The configuration variable 'num_slabs' must be defined first"
               << std::endl;
  }
}

void RequireCapacitiesUnset(bool &already_specified) {
  if (already_specified) {
    LOG(FATAL) << "Capacities are specified multiple times in the configuration"
               << " file. Only use one of 'capacities_bytes', 'capacities_kb',"
               << "'capacities_mb', or 'capacities_gb'\n";
  } else {
    already_specified = true;
  }
}

void RequireBlockSizesUnset(bool &already_specified) {
  if (already_specified) {
    LOG(FATAL) << "Block sizes are specified multiple times in the "
               << "configuration file. Only use one of 'block_sizes_bytes',"
               << "'block_sizes_kb', 'block_sizes_mb', or 'block_sizes_gb'\n";
  } else {
    already_specified = true;
  }
}

void ParseCapacities(Config *config, YAML::Node capacities, int unit_conversion, bool &already_specified) {
  int i = 0;
  RequireNumDevices(config);
  RequireCapacitiesUnset(already_specified);
  for(auto val_node : capacities) {
    config->capacities[i++] = val_node.as<size_t>() * unit_conversion;
  }
}

void ParseBlockSizes(Config *config, YAML::Node block_sizes, int unit_conversion, bool &already_specified) {
  int i = 0;
  RequireNumDevices(config);
  RequireBlockSizesUnset(already_specified);
  for(auto val_node : block_sizes) {
    size_t block_size = val_node.as<size_t>() * unit_conversion;
    if (block_size > INT_MAX) {
      LOG(FATAL) << "Max supported block size is " << INT_MAX << " bytes. "
                 << "Config file requested " << block_size << " bytes\n";
    }
    config->block_sizes[i++] = block_size;
  }
}

template<typename T>
void ParseList(YAML::Node list_node, T *list, int list_len) {
  int i = 0;
  for(auto val_node : list_node) {
    list[i++] = val_node.as<T>();
  }
}

void ParseSlabUnitSizes(Config *config, YAML::Node matrix_node, int row_len, int *col_len) {
  int i = 0;
  for(auto row : matrix_node) {
    ParseList<int>(row, config->slab_unit_sizes[i++], col_len[i]);
  }
}

void ParseDesiredSlabPercentages(Config *config, YAML::Node matrix_node, int row_len, int *col_len) {
  int i = 0;
  for(auto row : matrix_node) {
    ParseList<f32>(row, config->desired_slab_percentages[i++], col_len[i]);
  }
}

void ParseRangeList(YAML::Node list_node, std::vector<int> &list) {
  int i = 0, min, max;
  for(auto val_node : list_node) {
    std::string val = val_node.as<std::string>();
    if(val.find('-') == std::string::npos) {
      min = val_node.as<int>();
      max = min;
    }
    else {
      std::stringstream ss(val);
      std::string word;
      std::vector<std::string> words;
      while(std::getline(ss, word, '-')) {
        words.push_back(word);
      }
      if(words.size() != 2) {
        LOG(FATAL) << "Invalid range definition " << val << std::endl;
      }
      min = std::stoi(words[0]);
      max = std::stoi(words[1]);
    }
    for(int i = min; i <= max; ++i) {
      list.emplace_back(i);
    }
  }
}

void ParseConfig(Arena *arena, const char *path, Config *config);
void ParseConfigString(Arena *arena, const std::string &config_string, Config *config);

}  // namespace hermes
#endif  // HERMES_CONFIG_PARSER_H_
