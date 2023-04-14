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

#ifndef HERMES_SHM_INCLUDE_HERMES_SHM_UTIL_LOGGING_H_
#define HERMES_SHM_INCLUDE_HERMES_SHM_UTIL_LOGGING_H_

#include <unistd.h>
#include <climits>

#include <vector>
#include <iostream>
#include <fstream>
#include <iomanip>
#include <filesystem>
#include "formatter.h"
#include "singleton.h"

namespace hshm {

/**
 * A macro to indicate the verbosity of the logger.
 * Verbosity indicates how much data will be printed.
 * The higher the verbosity, the more data will be printed.
 * */
#ifndef HERMES_LOG_VERBOSITY
#define HERMES_LOG_VERBOSITY 10
#endif
#if HERMES_LOG_VERBOSITY < 0
#error "HERMES_LOG_VERBOSITY cannot be less than 0"
#endif

/** Prints log verbosity at compile time */
#define XSTR(s) STR(s)
#define STR(s) #s
// #pragma message XSTR(HERMES_LOG_VERBOSITY)

/** Simplify access to Logger singleton */
#define HERMES_LOG hshm::EasySingleton<hshm::Logger>::GetInstance()

/** Information Logging levels */
#define kDebug 10  /**< Low-priority debugging information*/
// ... may want to add more levels here
#define kInfo 1    /**< Useful information the user should know */

/** Error Logging Levels */
#define kFatal 0   /**< A fatal error has occurred */
#define kError 1   /**< A non-fatal error has occurred */
#define kWarning 2   /**< Something might be wrong */

/**
 * Hermes Print. Like printf, except types are inferred
 * */
#define HIPRINT(...) \
  HERMES_LOG->Print(__VA_ARGS__);

/**
 * Hermes Info (HI) Log
 * LOG_LEVEL indicates the priority of the log.
 * LOG_LEVEL 0 is considered required
 * LOG_LEVEL 10 is considered debugging priority.
 * */
#define HILOG(LOG_LEVEL, ...) \
  if constexpr(LOG_LEVEL <= HERMES_LOG_VERBOSITY) { \
    HERMES_LOG->InfoLog(LOG_LEVEL, __FILE__,        \
    __func__, __LINE__, __VA_ARGS__); \
  }

/**
 * Hermes Error (HE) Log
 * LOG_LEVEL indicates the priority of the log.
 * LOG_LEVEL 0 is considered required
 * LOG_LEVEL 10 is considered debugging priority.
 * */
#define HELOG(LOG_LEVEL, ...) \
  if constexpr(LOG_LEVEL <= HERMES_LOG_VERBOSITY) { \
    HERMES_LOG->ErrorLog(LOG_LEVEL, __FILE__,       \
    __func__, __LINE__, __VA_ARGS__); \
  }

class Logger {
 public:
  int verbosity_;
  FILE *fout_;

 public:
  Logger() {
    // exe_name_ = std::filesystem::path(exe_path_).filename().string();
    int verbosity = kDebug;
    auto verbosity_env = getenv("HERMES_LOG_VERBOSITY");
    if (verbosity_env && strlen(verbosity_env)) {
      try {
        verbosity = std::stoi(verbosity_env);
      } catch (...) {
        verbosity = kDebug;
      }
    }
    SetVerbosity(verbosity);

    auto env = getenv("HERMES_LOG_OUT");
    if (env == nullptr) {
      fout_ = nullptr;
    } else {
      fout_ = fopen(env, "w");
    }
  }

  void SetVerbosity(int LOG_LEVEL) {
    verbosity_ = LOG_LEVEL;
    if (verbosity_ < 0) {
      verbosity_ = 0;
    }
  }

  template<typename ...Args>
  void Print(const char *fmt,
             Args&& ...args) {
    std::string out =
        hshm::Formatter::format(fmt, std::forward<Args>(args)...);
    std::cout << out;
    fwrite(out.data(), 1, out.size(), fout_);
  }

  template<typename ...Args>
  void InfoLog(int LOG_LEVEL,
               const char *path,
               const char *func,
               int line,
               const char *fmt,
               Args&& ...args) {
    if (LOG_LEVEL > verbosity_) { return; }
    std::string msg =
        hshm::Formatter::format(fmt, std::forward<Args>(args)...);
    int tid = gettid();
    std::string out = hshm::Formatter::format(
        "{}:{} {} {} {}\n",
        path, line, tid, func, msg);
    std::cerr << out;
    if (fout_) {
      fwrite(out.data(), 1, out.size(), fout_);
    }
  }

  template<typename ...Args>
  void ErrorLog(int LOG_LEVEL,
                const char *path,
                const char *func,
                int line,
                const char *fmt,
                Args&& ...args) {
    if (LOG_LEVEL > verbosity_) { return; }
    std::string level;
    switch (LOG_LEVEL) {
      case kWarning: {
        level = "Warning";
        break;
      }
      case kError: {
        level = "ERROR";
        break;
      }
      case kFatal: {
        level = "FATAL";
        break;
      }
      default: {
        level = "WARNING";
        break;
      }
    }

    std::string msg =
        hshm::Formatter::format(fmt, std::forward<Args>(args)...);
    int tid = gettid();
    std::string out = hshm::Formatter::format(
        "{}:{} {} {} {} {}\n",
        path, line, level, tid, func, msg);
    std::cerr << out;
    if (fout_) {
      fwrite(out.data(), 1, out.size(), fout_);
    }
    if (LOG_LEVEL == kFatal) {
      exit(1);
    }
  }
};

}  // namespace hshm

#endif  // HERMES_SHM_INCLUDE_HERMES_SHM_UTIL_LOGGING_H_
