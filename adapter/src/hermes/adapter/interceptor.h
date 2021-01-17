//
// Created by manihariharan on 12/23/20.
//

#ifndef HERMES_INTERCEPTOR_H
#define HERMES_INTERCEPTOR_H

#include <utils.h>

#include <fstream>
#include <string>
#include <unordered_set>
#include <vector>

namespace hermes::adapter {
/**
 * Exclusion lists
 */
struct InterceptorList {
  // Paths prefixed with the following directories are not tracked in Hermes
  // Exclusion list used by darshan at
  // darshan/darshan-runtime/lib/darshan-core.c
  std::vector<std::string> path_exclusions;
  // paths prefixed with the following directories are tracked by Hermes even if
  // they share a root with a path listed in path_exclusions
  std::vector<std::string> path_inclusions;
  // allow users to override the path exclusions
  std::vector<std::string> user_path_exclusions;
  // allow users to override the path exclusions
  std::vector<std::string> hermes_paths_exclusion;
  // allow users to override the flush operations
  std::unordered_set<std::string> hermes_flush_exclusion;
  InterceptorList()
      : path_exclusions({
            "/bin/",
            "/boot/",
            "/dev/",
            "/etc/",
            "/lib/",
            "/opt/",
            "/proc/",
            "/sbin/",
            "/sys/",
            "/usr/",
            "/var/",
        }),
        path_inclusions({"/var/opt/cray/dws/mounts/"}),
        user_path_exclusions(),
        hermes_paths_exclusion(),
        hermes_flush_exclusion() {}
};
}  // namespace hermes::adapter

void OnExit(void);

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

void PopulateBufferingPath();

bool IsTracked(const std::string& path);

bool IsTracked(FILE* fh);

#else
#define HERMES_FORWARD_DECL(__name, __ret, __args) \
  extern __ret __real_##__name __args;
#define HERMES_DECL(__name) __wrap_##__name
#define MAP_OR_FAIL(__func)
#endif

#endif  // HERMES_INTERCEPTOR_H
