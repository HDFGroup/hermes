#ifndef HERMES_INTERCEPTOR_H
#define HERMES_INTERCEPTOR_H

/**
 * Standard headers
 */
#include <dlfcn.h>
#include <stdlib.h>
#include <utils.h>

#include <fstream>
#include <string>
#include <unordered_set>
#include <vector>

/**
 * Library headers
 */

/**
 * Internal headers
 */
#include <buffer_pool_internal.h>
#include <hermes/adapter/constants.h>
#include <hermes/adapter/singleton.h>

namespace hermes::adapter {
/**
 * Interceptor list defines files and directory that should be either excluded
 * or included for interceptions.
 */
struct InterceptorList {
  /**
   * Paths prefixed with the following directories are not tracked in Hermes
   * Exclusion list used by darshan at
   * darshan/darshan-runtime/lib/darshan-core.c
   */
  std::vector<std::string> path_exclusions;
  /**
   * Paths prefixed with the following directories are tracked by Hermes even if
   * they share a root with a path listed in path_exclusions
   */
  std::vector<std::string> path_inclusions;
  /**
   * Allow users to override the path exclusions
   */
  std::vector<std::string> user_path_exclusions;
  /**
   * Allow adapter to exclude hermes specific files.
   */
  std::vector<std::string> hermes_paths_exclusion;
  /**
   * Allow adapter to exclude files which are currently flushed.
   */
  std::unordered_set<std::string> hermes_flush_exclusion;

  /**
   * Default constructor
   */
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

/**
 * This method is bind in the runtime of the application. It is called during
 * __exit of the program. This function avoid any internal c++ runtime call to
 * get intercepted. On invocation, we set the adaptor to not intercept any
 * calls. If we do not have this method, the program might try to access
 * deallocated structures within adapter.
 */
void OnExit(void);

/**
 * HERMES_PRELOAD variable defines if adapter layer should intercept.
 */
#ifdef HERMES_PRELOAD
/**
 * Typedef and function declaration for intercepted functions.
 */
#define HERMES_FORWARD_DECL(func_, ret_, args_) \
  typedef ret_(*real_t_##func_##_) args_;       \
  ret_(*real_##func_##_) args_ = NULL;

#define HERMES_DECL(func_) func_
/**
 * The input function is remained as __real_<func_name>. And a ptr to function
 * is obtained using dlsym.
 */
#define MAP_OR_FAIL(func_)                                                  \
  if (!(real_##func_##_)) {                                                 \
    real_##func_##_ = (real_t_##func_##_)dlsym(RTLD_NEXT, #func_);          \
    if (!(real_##func_##_)) {                                               \
      fprintf(stderr, "HERMES Adapter failed to map symbol: %s\n", #func_); \
      exit(1);                                                              \
    }                                                                       \
  }

/**
 * Loads the buffering paths for Hermes Adapter to exclude. It inserts the
 * buffering mount points in the InclusionsList.hermes_paths_exclusion.
 */
void PopulateBufferingPath();

/**
 * Check if path should be tracked. In this method, the path is compared against
 * multiple inclusion and exclusion lists.
 *
 * @param path, const std::string&, path to be compared with exclusion and
 * inclusion.
 * @return true, if file should be tracked.
 *         false, if file should not be intercepted.
 */
bool IsTracked(const std::string& path);

/**
 * Check if fh should be tracked. In this method, first Convert fh to path.
 * Then, call IsTracked(path).
 *
 * @param fh, file handlers to be compared for exclusion or inclusion.
 * @return true, if file should be tracked.
 *         false, if file should not be intercepted.
 */
bool IsTracked(FILE* fh);

#else
/**
 * Do not intercept if HERMES_PRELOAD is not defined.
 */
#define HERMES_FORWARD_DECL(name_, ret_, args_) \
  extern ret_ real_##name_##_ args_;
#define HERMES_DECL(name_) wrap_##name_##_
#define MAP_OR_FAIL(func_)
#endif
#endif  // HERMES_INTERCEPTOR_H
