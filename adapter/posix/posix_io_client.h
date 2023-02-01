//
// Created by lukemartinlogan on 1/31/23.
//

#ifndef HERMES_ADAPTER_POSIX_POSIX_IO_CLIENT_H_
#define HERMES_ADAPTER_POSIX_POSIX_IO_CLIENT_H_

#include <memory>

#include "filesystem/filesystem_io_client.h"
#include "posix_singleton_macros.h"
#include "posix_api.h"

using hermes::Singleton;
using hermes::adapter::IoClientStat;
using hermes::adapter::IoClientOptions;
using hermes::adapter::IoStatus;
using hermes::adapter::posix::PosixApi;

namespace hermes::adapter::posix {

/** A class to represent POSIX IO file system */
class PosixIoClient : public hermes::adapter::fs::FilesystemIoClient {
 private:
  HERMES_POSIX_API_T real_api; /**< pointer to real APIs */

 public:
  /** Default constructor */
  PosixIoClient() { real_api = HERMES_POSIX_API; }

  /** Virtual destructor */
  virtual ~PosixIoClient() = default;

 public:
  /** Initialize a file whose fd has already been allocated */
  void _InitFile(IoClientContext &f) override;

  /** Write blob to backend */
  void WriteBlob(const Blob &full_blob,
                 size_t backend_off,
                 size_t backend_size,
                 const IoClientContext &io_ctx,
                 const IoClientOptions &opts,
                 IoStatus &status) override;

  /** Read blob from the backend */
  void ReadBlob(Blob &full_blob,
                size_t backend_off,
                size_t backend_size,
                const IoClientContext &io_ctx,
                const IoClientOptions &opts,
                IoStatus &status) override;

 private:
  void _OpenInitStats(IoClientContext &f, IoClientStat &stat) override;
  IoClientContext _RealOpen(IoClientStat &stat,
                            const std::string &path) override;
  int _RealSync(IoClientContext &f) override;
  int _RealClose(IoClientContext &f) override;
};

}  // namespace hermes::adapter::posix

/** Simplify access to the stateless PosixFs Singleton */
#define HERMES_POSIX_FS hermes::EasySingleton<hermes::adapter::posix::PosixFS>::GetInstance()
#define HERMES_POSIX_FS_T hermes::adapter::posix::PosixFs*

#endif  // HERMES_ADAPTER_POSIX_POSIX_IO_CLIENT_H_
