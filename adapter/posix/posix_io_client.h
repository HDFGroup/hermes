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

#ifndef HERMES_ADAPTER_POSIX_POSIX_IO_CLIENT_H_
#define HERMES_ADAPTER_POSIX_POSIX_IO_CLIENT_H_

#include <memory>

#include "adapter/filesystem/filesystem_io_client.h"
#include "posix_api.h"

using hshm::Singleton;
using hermes::adapter::fs::AdapterStat;
using hermes::adapter::fs::FsIoOptions;
using hermes::adapter::fs::IoStatus;
using hermes::adapter::fs::PosixApi;

namespace hermes::adapter::fs {

/** State for the POSIX I/O trait */
struct PosixIoClientHeader : public TraitHeader {
  explicit PosixIoClientHeader(const std::string &trait_uuid,
                               const std::string &trait_name)
      : TraitHeader(trait_uuid, trait_name,
                    HERMES_TRAIT_FLUSH) {}
};

/** A class to represent POSIX IO file system */
class PosixIoClient : public hermes::adapter::fs::FilesystemIoClient {
 public:
  HERMES_TRAIT_H(PosixIoClient, "posix_io_client")

 private:
  HERMES_POSIX_API_T real_api; /**< pointer to real APIs */

 public:
  /** Default constructor */
  PosixIoClient() {
    real_api = HERMES_POSIX_API;
    CreateHeader<PosixIoClientHeader>("posix_io_client_", trait_name_);
  }

  /** Trait deserialization constructor */
  explicit PosixIoClient(hshm::charbuf &params) {
    (void) params;
    real_api = HERMES_POSIX_API;
    CreateHeader<PosixIoClientHeader>("posix_io_client_", trait_name_);
  }

  /** Virtual destructor */
  virtual ~PosixIoClient() = default;

 public:
  /** Allocate an fd for the file f */
  void RealOpen(File &f,
                AdapterStat &stat,
                const std::string &path) override;

  /**
   * Called after real open. Allocates the Hermes representation of
   * identifying file information, such as a hermes file descriptor
   * and hermes file handler. These are not the same as POSIX file
   * descriptor and STDIO file handler.
   * */
  void HermesOpen(File &f,
                  const AdapterStat &stat,
                  FilesystemIoClientState &fs_mdm) override;

  /** Synchronize \a file FILE f */
  int RealSync(const File &f,
               const AdapterStat &stat) override;

  /** Close \a file FILE f */
  int RealClose(const File &f,
                AdapterStat &stat) override;

  /**
   * Called before RealClose. Releases information provisioned during
   * the allocation phase.
   * */
  void HermesClose(File &f,
                   const AdapterStat &stat,
                   FilesystemIoClientState &fs_mdm) override;

  /** Remove \a file FILE f */
  int RealRemove(const std::string &path) override;

  /** Get initial statistics from the backend */
  size_t GetSize(const hipc::charbuf &bkt_name) override;

  /** Write blob to backend */
  void WriteBlob(const std::string &bkt_name,
                 const Blob &full_blob,
                 const FsIoOptions &opts,
                 IoStatus &status) override;

  /** Read blob from the backend */
  void ReadBlob(const std::string &bkt_name,
                Blob &full_blob,
                const FsIoOptions &opts,
                IoStatus &status) override;
};

}  // namespace hermes::adapter::fs

/** Simplify access to the stateless PosixIoClient Singleton */
#define HERMES_POSIX_IO_CLIENT \
  hshm::EasySingleton<hermes::adapter::fs::PosixIoClient>::GetInstance()
#define HERMES_POSIX_IO_CLIENT_T hermes::adapter::fs::PosixIoClient*

#endif  // HERMES_ADAPTER_POSIX_POSIX_IO_CLIENT_H_
