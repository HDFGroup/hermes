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
}  // namespace hermes::adapter

void PopulateBufferingPath() {
  LOG(INFO) << "Populating Buffering Path " << std::endl;
  char* hermes_config = getenv(HERMES_CONF);
  hermes::Config config = {};
  const size_t kConfigMemorySize = KILOBYTES(16);
  hermes::u8 config_memory[kConfigMemorySize];
  if (hermes_config && strlen(hermes_config) > 0) {
    LOG(INFO) << "Found hermes config " << hermes_config << std::endl;
    list.hermes_paths_exclusion.push_back(hermes_config);
    hermes::Arena config_arena = {};
    hermes::InitArena(&config_arena, kConfigMemorySize, config_memory);
    hermes::ParseConfig(&config_arena, hermes_config, &config);
  } else {
    LOG(INFO) << "Hermes config not found in " << HERMES_CONF << std::endl;
    InitDefaultConfig(&config);
  }
  if (config.mount_points->empty()) {
    LOG(INFO) << "Adding hermes mount points to exclusion list." << std::endl;
    for (const auto& item : config.mount_points) {
      if (!item.empty()) {
        list.hermes_paths_exclusion.push_back(item);
      }
    }
  }
  LOG(INFO) << "Adding hermes shared memory and ext to exclusion list."
            << std::endl;
  list.hermes_paths_exclusion.push_back(config.buffer_pool_shmem_name);
  list.hermes_paths_exclusion.push_back(HERMES_EXT);
}
bool IsTracked(const std::string& path) {
  LOG(INFO) << "Check if " << path << " should be tracked." << std::endl;
  if (hermes::adapter::exit) return false;
  atexit(OnExit);
  if (list.hermes_paths_exclusion.empty()) {
    PopulateBufferingPath();
  }
  for (const auto& pth : list.hermes_flush_exclusion) {
    if (path.find(pth) != std::string::npos) {
      LOG(INFO) << "Exclude as file is getting flushed." << std::endl;
      return false;
    }
  }
  for (const auto& pth : list.hermes_paths_exclusion) {
    if (path.find(pth) != std::string::npos ||
        pth.find(path) != std::string::npos) {
      LOG(INFO) << "Exclude as files created and written by hermes."
                << std::endl;
      return false;
    }
  }
  if (list.user_path_exclusions.empty()) {
    LOG(INFO) << "Include as user specified paths." << std::endl;
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
    LOG(INFO) << "Exclude as user specified paths." << std::endl;
    for (const auto& pth : list.user_path_exclusions) {
      if (path.find(pth) == 0) {
        return false;
      }
    }
  }
  return true;
}
bool IsTracked(FILE* fh) {
  LOG(INFO) << "Check if file handler should be tracked." << std::endl;
  if (hermes::adapter::exit) return false;
  atexit(OnExit);
  int MAXSIZE = 0xFFF;
  char proclnk[0xFFF];
  char filename[0xFFF];
  int fno = fileno(fh);
  snprintf(proclnk, MAXSIZE, "/proc/self/fd/%d", fno);
  size_t r = readlink(proclnk, filename, MAXSIZE);
  if (r > 0) {
    std::string file_str(filename);
    return IsTracked(file_str);
  }
  return false;
}
void OnExit(void) {
  LOG(INFO) << "Program exiting." << std::endl;
  hermes::adapter::exit = true;
}
