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
#include "hermes_shm/util/path_parser.h"

#include "hermes_types.h"
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
    auto real_path = hshm::path_parser(path);
    VLOG(kDebug) << "ParseConfig-LoadFile: " << real_path << std::endl;
    try {
      YAML::Node yaml_conf = YAML::LoadFile(real_path);
      VLOG(kDebug) << "ParseConfig-LoadComplete" << std::endl;
      ParseYAML(yaml_conf);
    } catch (std::exception &e) {
      LOG(FATAL) << e.what() << std::endl;
    }
  }

  /** load the default configuration */
  virtual void LoadDefault() = 0;

 public:
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

  /**
 * parse a hostfile string
 * [] represents a range to generate
 * ; represents a new host name completely
 *
 * Example: hello[00-09,10]-40g;hello2[11-13]-40g
 * */
  static void ParseHostNameString(std::string hostname_set_str,
                                  std::vector<std::string> &list) {
    // Remove all whitespace characters from host name
    std::remove(hostname_set_str.begin(), hostname_set_str.end(), ' ');
    std::remove(hostname_set_str.begin(), hostname_set_str.end(), '\n');
    std::remove(hostname_set_str.begin(), hostname_set_str.end(), '\r');
    std::remove(hostname_set_str.begin(), hostname_set_str.end(), '\t');
    if (hostname_set_str.size() == 0) {
      return;
    }
    // Expand hostnames
    std::stringstream ss(hostname_set_str);
    while (ss.good()) {
      // Get the current host
      std::string hostname;
      std::getline(ss, hostname, ';');

      // Divide the hostname string into prefix, ranges, and suffix
      auto lbracket = hostname.find_first_of('[');
      auto rbracket = hostname.find_last_of(']');
      std::string prefix, ranges_str, suffix;
      if (lbracket != std::string::npos && rbracket != std::string::npos) {
        /*
       * For example, hello[00-09]-40g
       * lbracket = 5
       * rbracket = 11
       * prefix = hello (length: 5)
       * range = 00-09 (length: 5)
       * suffix = -40g (length: 4)
       * */
        prefix = hostname.substr(0, lbracket);
        ranges_str = hostname.substr(lbracket + 1, rbracket - lbracket - 1);
        suffix = hostname.substr(rbracket + 1);
      } else {
        list.emplace_back(hostname);
        continue;
      }

      // Parse the range list into a tuple of (min, max, num_width)
      std::stringstream ss_ranges(ranges_str);
      std::vector<std::tuple<int, int, int>> ranges;
      while (ss_ranges.good()) {
        // Parse ',' and remove spaces
        std::string range_str;
        std::getline(ss_ranges, range_str, ',');
        std::remove(range_str.begin(), range_str.end(), ' ');

        // Divide the range by '-'
        auto dash = range_str.find_first_of('-');
        if (dash != std::string::npos) {
          int min, max;
          // Get the minimum and maximum value
          std::string min_str = range_str.substr(0, dash);
          std::string max_str = range_str.substr(dash + 1);
          std::stringstream(min_str) >> min;
          std::stringstream(max_str) >> max;

          // Check for leading 0s
          int num_width = 0;
          if (min_str.size() == max_str.size()) {
            num_width = min_str.size();
          }

          // Place the range with width
          ranges.emplace_back(min, max, num_width);
        } else if (range_str.size()) {
          int val;
          std::stringstream(range_str) >> val;
          ranges.emplace_back(val, val, range_str.size());
        }
      }

      // Expand the host names by each range
      for (auto &range : ranges) {
        int min = std::get<0>(range);
        int max = std::get<1>(range);
        int num_width = std::get<2>(range);

        for (int i = min; i <= max; ++i) {
          std::stringstream host_ss;
          host_ss << prefix;
          host_ss << std::setw(num_width) << std::setfill('0') << i;
          host_ss << suffix;
          list.emplace_back(host_ss.str());
        }
      }
    }
  }

  /** parse the suffix of \a num_text NUMBER text */
  static std::string ParseNumberSuffix(const std::string &num_text) {
    size_t i;
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
      exit(1);
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
    exit(1);
  }

 private:
  virtual void ParseYAML(YAML::Node &yaml_conf) = 0;
};

}  // namespace hermes::config
#endif  // HERMES_CONFIG_PARSER_H_
