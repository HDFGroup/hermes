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

#ifndef HERMES_INTERCEPTOR_H
#define HERMES_INTERCEPTOR_H

#include <dlfcn.h>
#include <stdlib.h>

#include <fstream>
#include <string>
#include <unordered_set>
#include <vector>
#include <regex>

#include "buffer_pool_internal.h"
#include "constants.h"
#include "enumerations.h"
#include "singleton.h"
#include "adapter_utils.h"

/**
 * Define Interceptor list for adapter.
 */
#define INTERCEPTOR_LIST \
  hermes::adapter::Singleton<hermes::adapter::InterceptorList>::GetInstance<>()

#define HERMES_CONF \
  hermes::adapter::Singleton<hermes::Config>::GetInstance()

// Path lengths are up to 4096 bytes
const int kMaxPathLen = 4096;

namespace hermes::adapter {

/**
 * Splits a string given a delimiter
 */
inline std::vector<std::string> StringSplit(char* str, char delimiter) {
  std::stringstream ss(str);
  std::vector<std::string> v;
  while (ss.good()) {
    std::string substr;
    getline(ss, substr, delimiter);
    v.push_back(substr);
  }
  return v;
}
inline std::string GetFilenameFromFP(FILE* fh) {
  char proclnk[kMaxPathLen];
  char filename[kMaxPathLen];
  int fno = fileno(fh);
  snprintf(proclnk, kMaxPathLen, "/proc/self/fd/%d", fno);
  size_t r = readlink(proclnk, filename, kMaxPathLen);
  filename[r] = '\0';
  return filename;
}
inline std::string GetFilenameFromFD(int fd) {
  char proclnk[kMaxPathLen];
  char filename[kMaxPathLen];
  snprintf(proclnk, kMaxPathLen, "/proc/self/fd/%d", fd);
  size_t r = readlink(proclnk, filename, kMaxPathLen);
  filename[r] = '\0';
  return filename;
}

/**
 * Interceptor list defines files and directory that should be either excluded
 * or included for interceptions.
 */
struct InterceptorList {
  /**

   * hermes buffering mode.
   */
  AdapterMode adapter_mode;
  /**
   * Scratch paths
   */
  std::vector<std::string> adapter_paths;
  /**
   * Allow adapter to include hermes specific files.
   */
  std::vector<std::string> hermes_paths_inclusion;
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
      : adapter_mode(AdapterMode::kDefault),
        adapter_paths(),
        hermes_paths_exclusion(),
        hermes_flush_exclusion() {}
  void SetupAdapterMode() {
    char* adapter_mode_str = getenv(kAdapterMode);
    if (adapter_mode_str == nullptr) {
      // default is Persistent mode
      adapter_mode = AdapterMode::kDefault;
    } else {
      if (strcmp(kAdapterDefaultMode, adapter_mode_str) == 0) {
        adapter_mode = AdapterMode::kDefault;
      } else if (strcmp(kAdapterBypassMode, adapter_mode_str) == 0) {
        adapter_mode = AdapterMode::kBypass;
      } else if (strcmp(kAdapterScratchMode, adapter_mode_str) == 0) {
        adapter_mode = AdapterMode::kScratch;
      } else {
        // TODO(hari): @errorhandling throw error.
        return;
      }
    }
    char* adapter_paths_str = getenv(kAdapterModeInfo);
    std::vector<std::string> paths_local;
    if (adapter_paths_str) {
      paths_local = StringSplit(adapter_paths_str, kPathDelimiter);
    }
    adapter_paths = paths_local;
  }

  bool Persists(FILE* fh) { return Persists(GetFilenameFromFP(fh)); }
  bool Persists(int fd) { return Persists(GetFilenameFromFD(fd)); }

  bool Persists(std::string path) {
    if (adapter_mode == AdapterMode::kDefault) {
      if (adapter_paths.empty()) {
        return true;
      } else {
        for (const auto& pth : adapter_paths) {
          if (path.find(pth) == 0) {
            return true;
          }
        }
        return false;
      }
    } else if (adapter_mode == AdapterMode::kScratch) {
      if (adapter_paths.empty()) {
        return false;
      } else {
        for (const auto& pth : adapter_paths) {
          if (path.find(pth) == 0) {
            return false;
          }
        }
        return true;
      }
    }
    return true;
  }
};

/**
 * This method is bind in the runtime of the application. It is called during
 * __exit of the program. This function avoid any internal c++ runtime call to
 * get intercepted. On invocation, we set the adaptor to not intercept any
 * calls. If we do not have this method, the program might try to access
 * deallocated structures within adapter.
 */
void OnExit(void);

}  // namespace hermes::adapter

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
 * The input function is renamed as real_<func_name>_. And a ptr to function
 * is obtained using dlsym.
 */
#define MAP_OR_FAIL(func_)                                            \
  if (!(real_##func_##_)) {                                           \
    real_##func_##_ = (real_t_##func_##_)dlsym(RTLD_NEXT, #func_);    \
    if (!(real_##func_##_)) {                                         \
      LOG(FATAL) << "HERMES Adapter failed to map symbol: " << #func_ \
                 << std::endl;                                        \
    }                                                                 \
  }

namespace hermes::adapter {
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
}  // namespace hermes::adapter
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
