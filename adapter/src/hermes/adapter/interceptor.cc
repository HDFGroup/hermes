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

namespace hermes::adapter {
/**
 * is exit by program called.
 */
bool exit = false;

void PopulateBufferingPath() {
  char* hermes_config = getenv(kHermesConf);
  hermes::Config config = {};
  const size_t kConfigMemorySize = KILOBYTES(16);
  hermes::u8 config_memory[kConfigMemorySize];
  if (hermes_config && strlen(hermes_config) > 0) {
    INTERCEPTOR_LIST->hermes_paths_exclusion.push_back(hermes_config);
    hermes::Arena config_arena = {};
    hermes::InitArena(&config_arena, kConfigMemorySize, config_memory);
    hermes::ParseConfig(&config_arena, hermes_config, &config);
  } else {
    InitDefaultConfig(&config);
  }

  for (const auto& item : config.mount_points) {
    if (!item.empty()) {
      INTERCEPTOR_LIST->hermes_paths_exclusion.push_back(item);
    }
  }
  INTERCEPTOR_LIST->hermes_paths_exclusion.push_back(
      config.buffer_pool_shmem_name);
  INTERCEPTOR_LIST->hermes_paths_exclusion.push_back(kHermesExtension);
}
bool IsTracked(const std::string& path) {
  if (hermes::adapter::exit) return false;
  atexit(OnExit);
  if (INTERCEPTOR_LIST->hermes_paths_exclusion.empty()) {
    PopulateBufferingPath();
  }
  for (const auto& pth : INTERCEPTOR_LIST->hermes_flush_exclusion) {
    if (path.find(pth) != std::string::npos) {
      return false;
    }
  }
  for (const auto& pth : INTERCEPTOR_LIST->hermes_paths_exclusion) {
    if (path.find(pth) != std::string::npos ||
        pth.find(path) != std::string::npos) {
      return false;
    }
  }
  if (INTERCEPTOR_LIST->user_path_exclusions.empty()) {
    for (const auto& pth : kPathInclusions) {
      if (path.find(pth) == 0) {
        return true;
      }
    }
    for (const auto& pth : kPathExclusions) {
      if (path.find(pth) == 0) {
        return false;
      }
    }
  } else {
    for (const auto& pth : INTERCEPTOR_LIST->user_path_exclusions) {
      if (path.find(pth) == 0) {
        return false;
      }
    }
  }
  return true;
}
bool IsTracked(FILE* fh) {
  if (hermes::adapter::exit) return false;
  atexit(OnExit);
  const int kMaxSize = 0xFFF;
  char proclnk[kMaxSize];
  char filename[kMaxSize];
  int fno = fileno(fh);
  snprintf(proclnk, kMaxSize, "/proc/self/fd/%d", fno);
  size_t r = readlink(proclnk, filename, kMaxSize);
  filename[r] = '\0';
  if (r > 0) {
    std::string file_str(filename);
    return IsTracked(file_str);
  }
  return false;
}

void OnExit(void) { hermes::adapter::exit = true; }

}  // namespace hermes::adapter
