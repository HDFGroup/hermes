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

#ifndef HERMES_SRC_CONFIG_CLIENT_H_
#define HERMES_SRC_CONFIG_CLIENT_H_

#include <filesystem>
#include "config.h"
#include "adapter/adapter_types.h"

namespace stdfs = std::filesystem;

namespace hermes::config {

using hermes::adapter::AdapterType;
using hermes::adapter::AdapterMode;
using hermes::adapter::AdapterModeConv;
using hermes::adapter::AdapterObjectConfig;

/**< A path is included */
static inline const bool do_include = true;
/**< A path is excluded */
static inline const bool do_exclude = false;

/** Stores information about path inclusions and exclusions */
struct UserPathInfo {
  std::string path_;   /**< The path the user specified */
  bool include_;       /**< Whether to track path. */
  bool is_directory_;  /**< Whether the path is a file or directory */

  /** Default constructor */
  UserPathInfo() = default;

  /** Emplace Constructor */
  UserPathInfo(const std::string &path, bool include, bool is_directory)
  : path_(path), include_(include), is_directory_(is_directory) {}
};

/**
 * Configuration used to intialize client
 * */
class ClientConfig : public BaseConfig {
 public:
  /** Whether or not to stop daemon at exit */
  bool stop_daemon_;
  /** The flushing mode to use */
  FlushingMode flushing_mode_;
  /** The set of paths to monitor or exclude, ordered by length */
  std::vector<UserPathInfo> path_list_;
  /** The default adapter config */
  AdapterObjectConfig base_adapter_config_;
  /** Per-object (e.g., file) adapter configuration */
  std::unordered_map<std::string, AdapterObjectConfig> adapter_config_;

 public:
  ClientConfig() = default;
  void LoadDefault() override;

  void SetBaseAdapterMode(AdapterMode mode) {
    base_adapter_config_.mode_ = mode;
  }

  AdapterMode GetBaseAdapterMode() {
    return base_adapter_config_.mode_;
  }

  AdapterObjectConfig& GetAdapterConfig(const std::string &path) {
    auto iter = adapter_config_.find(path);
    if (iter == adapter_config_.end()) {
      return base_adapter_config_;
    }
    return iter->second;
  }

  void SetAdapterConfig(const std::string &path, AdapterObjectConfig &conf) {
    adapter_config_.emplace(path, conf);
  }

  void CreateAdapterPathTracking(const std::string &path, bool include) {
    bool is_dir = stdfs::is_directory(path);
    path_list_.emplace_back(
        stdfs::absolute(path).string(), include, is_dir);
    std::sort(path_list_.begin(),
              path_list_.end(),
              [](const UserPathInfo &a,
                 const UserPathInfo &b) {
                return a.path_.size() > b.path_.size();
              });
  }

  void SetAdapterPathTracking(const std::string &path, bool include) {
    for (auto &pth : path_list_) {
      if (pth.path_ == path) {
        pth.include_ = include;
        pth.is_directory_ = stdfs::is_directory(path);
        return;
      }
    }
    CreateAdapterPathTracking(path, include);
  }

  bool GetAdapterPathTracking(const std::string &path) {
    for (auto &pth : path_list_) {
      if (pth.path_ == path) {
        return pth.include_;
      }
    }
    return false;
  }

 private:
  void ParseYAML(YAML::Node &yaml_conf) override;

  void ParseAdapterConfig(YAML::Node &yaml_conf,
                          AdapterObjectConfig &conf);
};

}  // namespace hermes::config

namespace hermes {
using config::ClientConfig;
}  // namespace hermes

#endif  // HERMES_SRC_CONFIG_CLIENT_H_
