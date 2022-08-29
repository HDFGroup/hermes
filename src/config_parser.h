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

namespace hermes {

void ParseCapacities(Config *config, YAML::Node capacities, int unit_conversion) {
  int i = 0;
  RequireNumDevices(config);
  static bool already_specified = false;
  RequireCapacitiesUnset(already_specified);
  for(auto &val_node : capacities) {
    config->capacities[i++] = val_node.as<size_t>() * unit_conversion;
  }
}

void ParseBlockSizes(Config *config, YAML::Node block_sizes, int unit_conversion) {
  int i = 0;
  static bool already_specified = false;
  RequireNumDevices(config);
  RequireBlockSizesUnset(already_specified);
  for(auto val_node : block_sizes) {
    block_size = val_node.as<size_t>() * unit_conversion;
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

template<typename T>
void ParseMatrix(YAML::Node matrix_node, T *matrix[], int row_len, int col_len) {
  int i = 0;
  for(auto row : matrix_node) {
    ParseList<T>(row, matrix[i++], col_len);
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
      list.append(i);
    }
  }
}

void ParseTokens(TokenList *tokens, Config *config);

}  // namespace hermes
#endif  // HERMES_CONFIG_PARSER_H_
