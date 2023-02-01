//
// Created by lukemartinlogan on 2/1/23.
//

#ifndef HERMES_ADAPTER_FILESYSTEM_FILESYSTEM_IO_CLIENT_H_
#define HERMES_ADAPTER_FILESYSTEM_FILESYSTEM_IO_CLIENT_H_

#include "io_client/io_client.h"

namespace hermes::adapter::fs {

class FilesystemIoClient : public IoClient {
 public:
  virtual ~FilesystemIoClient() = default;

 public:
  /** initialize file */
  virtual void _InitFile(IoClientContext &f) = 0;

 private:
  /** Initialize an opened file's statistics */
  virtual void _OpenInitStats(IoClientContext &f, IoClientStat &stat) = 0;

  /** real open */
  virtual IoClientContext _RealOpen(IoClientStat &stat,
                                    const std::string &path) = 0;

  /** real sync */
  virtual int _RealSync(IoClientContext &f) = 0;

  /** real close */
  virtual int _RealClose(IoClientContext &f) = 0;
};

}  // namespace hermes::adapter::fs

#endif  // HERMES_ADAPTER_FILESYSTEM_FILESYSTEM_IO_CLIENT_H_
