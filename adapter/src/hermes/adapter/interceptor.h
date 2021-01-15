//
// Created by manihariharan on 12/23/20.
//

#ifndef HERMES_INTERCEPTOR_H
#define HERMES_INTERCEPTOR_H

#include <utils.h>

#include <fstream>
#include <string>
#include <vector>

namespace hermes::adapter {
/**
 * Exclusion lists
 */
// Paths prefixed with the following directories are not tracked in Hermes
// Exclusion list used by darshan at
// darshan/darshan-runtime/lib/darshan-core.c
static std::vector<std::string> path_exclusions = {
    "/bin/",  "/boot/", "/dev/", "/etc/", "/lib/", "/opt/",
    "/proc/", "/sbin/", "/sys/", "/usr/", "/var/",
};

// paths prefixed with the following directories are tracked by Hermes even if
// they share a root with a path listed in path_exclusions
static std::vector<std::string> path_inclusions = {"/var/opt/cray/dws/mounts/"};

// allow users to override the path exclusions
std::vector<std::string> user_path_exclusions;

// allow users to override the path exclusions
std::vector<std::string> hermes_paths_exclusion;

}  // namespace hermes::adapter

#ifdef HERMES_PRELOAD
#include <buffer_pool_internal.h>
#include <dlfcn.h>
#include <stdlib.h>

#include "constants.h"
#include "singleton.h"
#define HERMES_FORWARD_DECL(__func, __ret, __args) \
  typedef __ret(*__real_t_##__func) __args;        \
  __ret(*__real_##__func) __args = NULL;

#define HERMES_DECL(__func) __func
#define MAP_OR_FAIL(__func)                                          \
  if (!(__real_##__func)) {                                          \
    __real_##__func = (__real_t_##__func)dlsym(RTLD_NEXT, #__func);  \
    if (!(__real_##__func)) {                                        \
      fprintf(stderr, "HERMES failed to map symbol: %s\n", #__func); \
      exit(1);                                                       \
    }                                                                \
  }

bool PopulateBufferingPath() {
  char* hermes_config = getenv(HERMES_CONF);
  hermes::Config config = {};
  const size_t kConfigMemorySize = KILOBYTES(16);
  hermes::u8 config_memory[kConfigMemorySize];
  if (hermes_config && strlen(hermes_config) > 0) {
    hermes::adapter::hermes_paths_exclusion.push_back(hermes_config);
    hermes::Arena config_arena = {};
    hermes::InitArena(&config_arena, kConfigMemorySize, config_memory);
    hermes::ParseConfig(&config_arena, hermes_config, &config);
  } else {
    InitDefaultConfig(&config);
  }
  for (const auto& item : config.mount_points) {
    if (!item.empty()) {
      hermes::adapter::hermes_paths_exclusion.push_back(item);
    }
  }
  hermes::adapter::hermes_paths_exclusion.push_back(
      config.buffer_pool_shmem_name);
  hermes::adapter::hermes_paths_exclusion.push_back(HERMES_EXT);
}

bool IsTracked(const std::string& path) {
  if (hermes::adapter::hermes_paths_exclusion.empty()) {
    PopulateBufferingPath();
  }
  for (const auto& pth : hermes::adapter::hermes_paths_exclusion) {
    if (path.find(pth) != std::string::npos ||
        pth.find(path) != std::string::npos) {
      return false;
    }
  }
  if (hermes::adapter::user_path_exclusions.empty()) {
    for (const auto& pth : hermes::adapter::path_inclusions) {
      if (path.find(pth) == 0) {
        return true;
      }
    }
    for (const auto& pth : hermes::adapter::path_exclusions) {
      if (path.find(pth) == 0) {
        return false;
      }
    }
  } else {
    for (const auto& pth : hermes::adapter::user_path_exclusions) {
      if (path.find(pth) == 0) {
        return false;
      }
    }
  }
  return true;
}

bool IsTracked(FILE* fh) {
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

#else
#define HERMES_FORWARD_DECL(__name, __ret, __args) \
  extern __ret __real_##__name __args;
#define HERMES_DECL(__name) __wrap_##__name
#define MAP_OR_FAIL(__func)
#endif

#endif  // HERMES_INTERCEPTOR_H
