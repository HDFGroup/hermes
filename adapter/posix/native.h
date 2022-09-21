//
// Created by lukemartinlogan on 9/20/22.
//

#ifndef HERMES_ADAPTER_POSIX_NATIVE_H_
#define HERMES_ADAPTER_POSIX_NATIVE_H_

#include <memory>
#include "filesystem/filesystem.h"
#include "filesystem/filesystem.cc"
#include "filesystem/metadata_manager.h"
#include "filesystem/metadata_manager.cc"
#include "posix.h"

using hermes::adapter::fs::AdapterStat;
using hermes::adapter::fs::File;
using hermes::adapter::Singleton;
using hermes::adapter::posix::API;

namespace hermes::adapter::posix {

class PosixFS : public hermes::adapter::fs::Filesystem {
 private:
  std::shared_ptr<API> real_api;
 public:
  PosixFS() {
    real_api = Singleton<API>::GetInstance();
  }
  ~PosixFS() = default;

  void _InitFile(File &f) override;

 private:
  void _OpenInitStats(File &f, AdapterStat &stat, bool bucket_exists) override;
  File _RealOpen(AdapterStat &stat, const std::string &path) override;
  size_t _RealWrite(const std::string &filename, off_t offset, size_t size,
                    u8 *data_ptr) override;
  size_t _RealRead(const std::string &filename, off_t offset, size_t size,
                   u8 *data_ptr) override;
};

}  // namespace hermes::adapter::posix

#endif  // HERMES_ADAPTER_POSIX_NATIVE_H_
