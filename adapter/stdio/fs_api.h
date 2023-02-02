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

#ifndef HERMES_ADAPTER_STDIO_NATIVE_H_
#define HERMES_ADAPTER_STDIO_NATIVE_H_

#include <memory>

#include "filesystem/filesystem.cc"
#include "filesystem/filesystem.h"
#include "filesystem/metadata_manager.cc"
#include "filesystem/metadata_manager.h"
#include "posix/real_api.h"
#include "real_api.h"

using hermes::Singleton;
using hermes::adapter::fs::AdapterStat;
using hermes::adapter::fs::File;
using hermes::adapter::fs::IoOptions;
using hermes::adapter::fs::IoStatus;
using hermes::adapter::stdio::API;

namespace hermes::adapter::stdio {
/**
   A class to represent standard IO file system
*/
class StdioFS : public hermes::adapter::fs::Filesystem {
 private:
  API *real_api;                          /**< pointer to real APIs */
  hermes::adapter::fs::API *posix_api; /**< pointer to POSIX APIs */

 public:
  StdioFS() {
    real_api = Singleton<API>::GetInstance();
    posix_api = Singleton<hermes::adapter::fs::API>::GetInstance();
  }
  ~StdioFS() = default;

  void InitFile(File &f) override;

 private:
  void OpenInitStat(File &f, AdapterStat &stat) override;
  File RealOpen(AdapterStat &stat, const std::string &path) override;
  size_t _RealWrite(const std::string &filename, off_t offset, size_t size,
                    const u8 *data_ptr, IoStatus &io_status,
                    IoOptions &opts) override;
  size_t _RealRead(const std::string &filename, off_t offset, size_t size,
                   u8 *data_ptr, IoStatus &io_status, IoOptions &opts) override;
  int RealSync(File &f) override;
  int RealClose(File &f) override;
};

}  // namespace hermes::adapter::stdio

#endif  // HERMES_ADAPTER_STDIO_NATIVE_H_
