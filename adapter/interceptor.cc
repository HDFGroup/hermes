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

/**
 * Internal headers
 */
#include "interceptor.h"
#include <unistd.h>
#include <regex>
#include <experimental/filesystem>
#include "adapter_utils.h"
#include "config_parser.h"

namespace stdfs = std::experimental::filesystem;
const char* kPathExclusions[15] = {"/bin/", "/boot/", "/dev/",  "/etc/",
                                 "/lib/", "/opt/",  "/proc/", "/sbin/",
                                 "/sys/", "/usr/",  "/var/",  "/run/",
                                 "pipe", "socket:", "anon_inode:"};

namespace hermes::adapter {
/**
 * is exit by program called.
 */
bool exit = false;

bool populated = false;

void PopulateBufferingPath() {
  char* hermes_config = getenv(kHermesConf);

  if (stdfs::exists(hermes_config)) {
    std::string hermes_conf_abs_path =
        WeaklyCanonical(stdfs::path(hermes_config)).string();
    INTERCEPTOR_LIST->hermes_paths_exclusion.push_back(hermes_conf_abs_path);
  }
  hermes::Config config_stack = {};
  hermes::Config *config = &config_stack;
  hermes::InitConfig(config, hermes_config);
  for (const auto& item : config->mount_points) {
    if (!item.empty()) {
      std::string abs_path = WeaklyCanonical(item).string();
      INTERCEPTOR_LIST->hermes_paths_exclusion.push_back(abs_path);
    }
  }
  for (const auto& item : config->path_exclusions) {
    if (!item.empty()) {
      std::string abs_path = WeaklyCanonical(item).string();
      INTERCEPTOR_LIST->hermes_paths_exclusion.push_back(abs_path);
    }
  }
  for (const auto& item : config->path_inclusions) {
    if (!item.empty()) {
      std::string abs_path = WeaklyCanonical(item).string();
      INTERCEPTOR_LIST->hermes_paths_inclusion.push_back(abs_path);
    }
  }
  INTERCEPTOR_LIST->hermes_paths_exclusion.push_back(
      config->buffer_pool_shmem_name);
  INTERCEPTOR_LIST->hermes_paths_exclusion.push_back(kHermesExtension);

  // NOTE(chogan): Logging before setting up hermes_paths_exclusion results in
  // deadlocks in GLOG
  LOG(INFO) << "Adapter page size: " << kPageSize << "\n";
  populated = true;
}

bool IsTracked(const std::string& path) {
  if (hermes::adapter::exit) {
    return false;
  }
  atexit(OnExit);

  std::string abs_path = WeaklyCanonical(path).string();

  for (const auto& pth : INTERCEPTOR_LIST->hermes_flush_exclusion) {
    if (abs_path.find(pth) != std::string::npos) {
      return false;
    }
  }

  if (INTERCEPTOR_LIST->hermes_paths_exclusion.empty()) {
    PopulateBufferingPath();
  }

  for (const auto& pth : INTERCEPTOR_LIST->hermes_paths_inclusion) {
    if (abs_path.find(pth) != std::string::npos) {
      return true;
    }
  }

  for (auto &pth : kPathExclusions) {
    if (abs_path.find(pth) == 0) {
      return false;
    }
  }

  for (const auto& pth : INTERCEPTOR_LIST->hermes_paths_exclusion) {
    if (abs_path.find(pth) != std::string::npos ||
        pth.find(abs_path) != std::string::npos) {
      return false;
    }
  }

  auto list = INTERCEPTOR_LIST;
  auto buffer_mode = INTERCEPTOR_LIST->adapter_mode;
  if (buffer_mode == AdapterMode::kBypass) {
    if (list->adapter_paths.empty()) {
      return false;
    } else {
      for (const auto& pth : list->adapter_paths) {
        if (path.find(pth) == 0) {
          return false;
        }
      }
      return true;
    }
  } else {
    return true;
  }
}

bool IsTracked(FILE* fh) {
  if (hermes::adapter::exit) return false;
  atexit(OnExit);
  return IsTracked(GetFilenameFromFP(fh));
}

bool IsTracked(int fd) {
  if (hermes::adapter::exit) return false;
  atexit(OnExit);
  return IsTracked(GetFilenameFromFD(fd));
}

void OnExit(void) { hermes::adapter::exit = true; }

}  // namespace hermes::adapter
