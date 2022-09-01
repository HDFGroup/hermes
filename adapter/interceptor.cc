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

namespace fs = std::experimental::filesystem;

namespace hermes::adapter {
/**
 * is exit by program called.
 */
bool exit = false;

void PopulateBufferingPath() {
  char* hermes_config = getenv(kHermesConf);

  auto config = Singleton<hermes::Config>::GetInstance().get();
  const size_t kConfigMemorySize = KILOBYTES(16);
  hermes::u8 config_memory[kConfigMemorySize];
  if (fs::exists(hermes_config)) {
    std::string hermes_conf_abs_path =
      WeaklyCanonical(fs::path(hermes_config)).string();
    INTERCEPTOR_LIST->hermes_paths_exclusion.push_back(hermes_conf_abs_path);
    hermes::Arena config_arena = {};
    hermes::InitArena(&config_arena, kConfigMemorySize, config_memory);
    hermes::ParseConfig(&config_arena, hermes_conf_abs_path.c_str(), config);
  } else {
    InitDefaultConfig(config);
  }

  for (const auto& item : config->mount_points) {
    if (!item.empty()) {
      std::string abs_path = WeaklyCanonical(item).string();
      INTERCEPTOR_LIST->hermes_paths_exclusion.push_back(abs_path);
    }
  }
  INTERCEPTOR_LIST->hermes_paths_exclusion.push_back(
      config->buffer_pool_shmem_name);
  INTERCEPTOR_LIST->hermes_paths_exclusion.push_back(kHermesExtension);

  // NOTE(chogan): Logging before setting up hermes_paths_exclusion results in
  // deadlocks in GLOG
  LOG(INFO) << "Adapter page size: " << kPageSize << "\n";
}

bool IsTracked(const std::string& path) {
  if (hermes::adapter::exit) {
    return false;
  }
  atexit(OnExit);

  // NOTE(llogan): This may be a performance issue
  auto config = Singleton<hermes::Config>::GetInstance();
  std::string abs_path = WeaklyCanonical(path).string();

  for (const auto& pth : INTERCEPTOR_LIST->hermes_flush_exclusion) {
    if (abs_path.find(pth) != std::string::npos) {
      return false;
    }
  }

  if (INTERCEPTOR_LIST->hermes_paths_exclusion.empty()) {
    PopulateBufferingPath();
  }

  for (const auto& pth : config->path_exclusions) {
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

  for (const auto& pth : config->path_inclusions) {
    if (abs_path.find(pth) == 0) {
      return true;
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
