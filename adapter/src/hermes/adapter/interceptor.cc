/**
 * Internal headers
 */
#include "interceptor.h"

/**
 * Define Interceptor list for adapter.
 */
hermes::adapter::InterceptorList list;

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
    list.hermes_paths_exclusion.push_back(hermes_config);
    hermes::Arena config_arena = {};
    hermes::InitArena(&config_arena, kConfigMemorySize, config_memory);
    hermes::ParseConfig(&config_arena, hermes_config, &config);
  } else {
    InitDefaultConfig(&config);
  }

  for (const auto& item : config.mount_points) {
    if (!item.empty()) {
      list.hermes_paths_exclusion.push_back(item);
    }
  }
  list.hermes_paths_exclusion.push_back(config.buffer_pool_shmem_name);
  list.hermes_paths_exclusion.push_back(kHermesExtension);
}
bool IsTracked(const std::string& path) {
  if (hermes::adapter::exit) return false;
  atexit(OnExit);
  if (list.hermes_paths_exclusion.empty()) {
    PopulateBufferingPath();
  }
  for (const auto& pth : list.hermes_flush_exclusion) {
    if (path.find(pth) != std::string::npos) {
      return false;
    }
  }
  for (const auto& pth : list.hermes_paths_exclusion) {
    if (path.find(pth) != std::string::npos ||
        pth.find(path) != std::string::npos) {
      return false;
    }
  }
  if (list.user_path_exclusions.empty()) {
    for (const auto& pth : list.path_inclusions) {
      if (path.find(pth) == 0) {
        return true;
      }
    }
    for (const auto& pth : list.path_exclusions) {
      if (path.find(pth) == 0) {
        return false;
      }
    }
  } else {
    for (const auto& pth : list.user_path_exclusions) {
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
  if (r > 0) {
    std::string file_str(filename);
    return IsTracked(file_str);
  }
  return false;
}

void OnExit(void) { hermes::adapter::exit = true; }

}  // namespace hermes::adapter
