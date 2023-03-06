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

#include <string.h>
#include <yaml-cpp/yaml.h>
#include <iomanip>
#include <ostream>
#include <vector>
#include <sstream>
#include <limits>
#include "data_structures.h"

#include "hermes_types.h"
#include "constants.h"
#include "utils.h"

namespace hermes::config {

/**
 * Base class for configuration files
 * */
class BaseConfig {
 public:
  /** load configuration from a string */
  void LoadText(const std::string &config_string, bool with_default = true) {
    if (with_default) { LoadDefault(); }
    if (config_string.size() == 0) {
      return;
    }
    YAML::Node yaml_conf = YAML::Load(config_string);
    ParseYAML(yaml_conf);
  }

  /** load configuration from file */
  void LoadFromFile(const std::string &path, bool with_default = true) {
    if (with_default) { LoadDefault(); }
    if (path.size() == 0) {
      return;
    }
    LOG(INFO) << "ParseConfig-LoadFile: " << path << std::endl;
    YAML::Node yaml_conf = YAML::LoadFile(path);
    LOG(INFO) << "ParseConfig-LoadComplete" << std::endl;
    ParseYAML(yaml_conf);
  }

  /** load the default configuration */
  virtual void LoadDefault() = 0;

 private:
  virtual void ParseYAML(YAML::Node &yaml_conf) = 0;
};

/** print \a expected value and fail when an error occurs */
static void PrintExpectedAndFail(const std::string &expected,
                                 u32 line_number = 0) {
  std::ostringstream msg;
  msg << "Configuration parser expected '" << expected << "'";
  if (line_number > 0) {
    msg << " on line " << line_number;
  }
  msg << "\n";

  LOG(FATAL) << msg.str();
}

/** parse \a list_node vector from configuration file in YAML */
template<typename T, typename VEC_TYPE = std::vector<T>>
static void ParseVector(YAML::Node list_node, VEC_TYPE &list) {
  if constexpr(IS_SHM_SMART_POINTER(VEC_TYPE)) {
    for (auto val_node : list_node) {
      list->emplace_back(val_node.as<T>());
    }
  } else {
    for (auto val_node : list_node) {
      list.emplace_back(val_node.as<T>());
    }
  }
}

/** parse range list from configuration file in YAML */
static void ParseRangeList(YAML::Node list_node, std::string var,
                           std::vector<std::string> &list) {
  int min, max, width = 0;
  for (auto val_node : list_node) {
    std::string val = val_node.as<std::string>();
    if (val.find('-') == std::string::npos) {
      min = val_node.as<int>();
      max = min;
    } else {
      std::stringstream ss(val);
      std::string word;
      std::vector<std::string> words;
      while (std::getline(ss, word, '-')) {
        words.push_back(word);
      }
      if (words.size() != 2) {
        LOG(FATAL) << var <<
            " has invalid range definition " << val << std::endl;
        return;
      }
      min = std::stoi(words[0]);
      max = std::stoi(words[1]);
      width = words[0].size();
    }

    if (width > 0) {
      for (int i = min; i <= max; ++i) {
        std::stringstream ss;
        ss << std::setw(width) << std::setfill('0') << i;
        list.emplace_back(ss.str());
      }
    } else {
      for (int i = min; i <= max; ++i) {
        list.emplace_back(std::to_string(i));
      }
    }
  }
}

/** parse the suffix of \a num_text NUMBER text */
static std::string ParseNumberSuffix(const std::string &num_text) {
  int i;
  for (i = 0; i < num_text.size(); ++i) {
    char c = num_text[i];
    if ('0' <= c && c <= '9') continue;
    if (c == ' ' || c == '\t' || c == '\n' || c == '\r') continue;
    break;
  }
  return std::string(num_text.begin() + i, num_text.end());;
}

/** parse the number of \a num_text NUMBER text */
static size_t ParseNumber(const std::string &num_text) {
  size_t size;
  std::stringstream(num_text) >> size;
  return size;
}

/** Converts \a size_text SIZE text into a size_t */
static size_t ParseSize(const std::string &size_text) {
  size_t size = ParseNumber(size_text);
  if (size_text == "inf") {
    return std::numeric_limits<size_t>::max();
  }
  std::string suffix = ParseNumberSuffix(size_text);
  if (suffix.size() == 0) {
    return size;
  } else if (suffix[0] == 'k' || suffix[0] == 'K') {
    return KILOBYTES(size);
  } else if (suffix[0] == 'm' || suffix[0] == 'M') {
    return MEGABYTES(size);
  } else if (suffix[0] == 'g' || suffix[0] == 'G') {
    return GIGABYTES(size);
  } else {
    LOG(FATAL) << "Could not parse the size: " << size_text << std::endl;
  }
}

/** Returns bandwidth (bytes / second) */
static size_t ParseBandwidth(const std::string &size_text) {
  return ParseSize(size_text);
}

/** Returns latency (nanoseconds) */
static size_t ParseLatency(const std::string &latency_text) {
  size_t size = ParseNumber(latency_text);
  std::string suffix = ParseNumberSuffix(latency_text);
  if (suffix.size() == 0) {
    return size;
  } else if (suffix[0] == 'n' || suffix[0] == 'N') {
    return size;
  } else if (suffix[0] == 'u' || suffix[0] == 'U') {
    return KILOBYTES(size);
  } else if (suffix[0] == 'm' || suffix[0] == 'M') {
    return MEGABYTES(size);
  } else if (suffix[0] == 's' || suffix[0] == 'S') {
    return GIGABYTES(size);
  }
  LOG(FATAL) << "Could not parse the latency: " << latency_text << std::endl;
}

}  // namespace hermes::config
#endif  // HERMES_CONFIG_PARSER_H_
