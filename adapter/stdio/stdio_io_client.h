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

#ifndef HERMES_ADAPTER_STDIO_STDIO_IO_CLIENT_H_
#define HERMES_ADAPTER_STDIO_STDIO_IO_CLIENT_H_

#include <memory>

#include "adapter/filesystem/filesystem_io_client.h"
#include "stdio_api.h"

using hermes::adapter::IoClientStats;
using hermes::adapter::IoClientContext;
using hermes::adapter::IoStatus;
using hermes::adapter::fs::StdioApi;

namespace hermes::adapter::fs {

/** A class to represent STDIO IO file system */
class StdioIoClient : public hermes::adapter::fs::FilesystemIoClient {
 private:
  HERMES_STDIO_API_T real_api; /**< pointer to real APIs */

 public:
  /** Default constructor */
  StdioIoClient() { real_api = HERMES_STDIO_API; }

  /** Virtual destructor */
  virtual ~StdioIoClient() = default;

 public:
  /** Allocate an fd for the file f */
  void RealOpen(IoClientObject &f,
                IoClientStats &stat,
                const std::string &path) override;

  /**
   * Called after real open. Allocates the Hermes representation of
   * identifying file information, such as a hermes file descriptor
   * and hermes file handler. These are not the same as STDIO file
   * descriptor and STDIO file handler.
   * */
  void HermesOpen(IoClientObject &f,
                  const IoClientStats &stat,
                  FilesystemIoClientObject &fs_mdm) override;

  /** Synchronize \a file FILE f */
  int RealSync(const IoClientObject &f,
               const IoClientStats &stat) override;

  /** Close \a file FILE f */
  int RealClose(const IoClientObject &f,
                IoClientStats &stat) override;

  /**
   * Called before RealClose. Releases information provisioned during
   * the allocation phase.
   * */
  void HermesClose(IoClientObject &f,
                   const IoClientStats &stat,
                   FilesystemIoClientObject &fs_mdm) override;

  /** Get initial statistics from the backend */
  void InitBucketState(const lipc::charbuf &bkt_name,
                       const IoClientContext &opts,
                       GlobalIoClientState &stat) override;

  /** Write blob to backend */
  void WriteBlob(const lipc::charbuf &bkt_name,
                 const Blob &full_blob,
                 const IoClientContext &opts,
                 IoStatus &status) override;

  /** Read blob from the backend */
  void ReadBlob(const lipc::charbuf &bkt_name,
                Blob &full_blob,
                const IoClientContext &opts,
                IoStatus &status) override;
};

}  // namespace hermes::adapter::fs

/** Simplify access to the stateless StdioIoClient Singleton */
#define HERMES_STDIO_IO_CLIENT \
  hermes::EasySingleton<hermes::adapter::fs::StdioIoClient>::GetInstance()
#define HERMES_STDIO_IO_CLIENT_T hermes::adapter::fs::StdioIoClient*

#endif  // HERMES_ADAPTER_STDIO_STDIO_IO_CLIENT_H_
