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

#ifndef HERMES_ADAPTER_POSIX_NATIVE_H_
#define HERMES_ADAPTER_POSIX_NATIVE_H_

#include <memory>
#include "filesystem/filesystem.h"
#include "filesystem/metadata_manager.h"
#include "real_api.h"

using hermes::adapter::fs::AdapterStat;
using hermes::adapter::fs::File;
using hermes::Singleton;
using hermes::adapter::posix::API;
using hermes::adapter::fs::IoOptions;
using hermes::adapter::fs::IoStatus;

namespace hermes::adapter::posix {

class PosixFS : public hermes::adapter::fs::Filesystem {
 private:
  API* real_api;
 public:
  PosixFS() {
    real_api = Singleton<API>::GetInstance();
  }
  ~PosixFS() = default;

  void _InitFile(File &f) override;

 private:
  void _OpenInitStats(File &f, AdapterStat &stat) override;
  File _RealOpen(AdapterStat &stat, const std::string &path) override;
  size_t _RealWrite(const std::string &filename, off_t offset, size_t size,
                    const u8 *data_ptr,
                    IoStatus &io_status, IoOptions &opts) override;
  size_t _RealRead(const std::string &filename, off_t offset, size_t size,
                   u8 *data_ptr,
                   IoStatus &io_status, IoOptions &opts) override;
  int _RealSync(File &f) override;
  int _RealClose(File &f) override;
};

}  // namespace hermes::adapter::posix

#endif  // HERMES_ADAPTER_POSIX_NATIVE_H_
