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

namespace hermes::adapter {
/**
 * is exit by program called.
 */
bool exit = false;

void PopulateBufferingPath() {
  char* hermes_config = getenv(kHermesConf);
  if (IsSymLink(hermes_config))
    LOG(FATAL) << "Hermes Config file: " << hermes_config 
               << "\nis symbolic link. It is not supported yet";
  if (IsRelativePath(hermes_config))
    LOG(FATAL) << "Hermes Config file: " << hermes_config
               << "\nis relative. It is not supported yet";

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
  if (hermes::adapter::exit) {
    return false;
  }
  atexit(OnExit);
  for (const auto& pth : kPathExclusions) {
    if (path.find(pth) == 0) {
      return false;
    }
  }
  for (const auto& pth : INTERCEPTOR_LIST->hermes_flush_exclusion) {
    if (path.find(pth) != std::string::npos) {
      return false;
    }
  }
  if (INTERCEPTOR_LIST->hermes_paths_exclusion.empty()) {
    PopulateBufferingPath();
  }
  for (const auto& pth : INTERCEPTOR_LIST->hermes_paths_exclusion) {
    if (path.find(pth) != std::string::npos ||
        pth.find(path) != std::string::npos) {
      return false;
    }
  }
  for (const auto& pth : kPathInclusions) {
    if (path.find(pth) == 0) {
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

bool IsRelativePath(const std::string& path) {
  std::regex e1 ("^/.*");
  std::regex e2 ("(.*)(\\./)(.*)");
  // Capture path not starting with "/" or containing "./"
  return !std::regex_match(path, e1) 
         && std::regex_match(path, e2);
}

bool IsSymLink(const std::string& path) {
  std::string cmd = "readlink -f " + path;
  std::array<char, PATH_MAX> buffer;
  std::string result;
  std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd.c_str(), "r"), pclose);
  if (!pipe) {
    LOG(FATAL) << "popen() failed!";
  }
  while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
    result += buffer.data();
  }
  return result!= path;
}

void OnExit(void) { hermes::adapter::exit = true; }

}  // namespace hermes::adapter
