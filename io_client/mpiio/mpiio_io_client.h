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

#ifndef HERMES_ADAPTER_MPIIO_MPIIO_IO_CLIENT_H_
#define HERMES_ADAPTER_MPIIO_MPIIO_IO_CLIENT_H_

#include <memory>

#include "io_client/filesystem/filesystem_io_client.h"
#include "io_client/mpiio/mpiio_api.h"

using hermes::adapter::fs::AdapterStat;
using hermes::adapter::fs::FsIoOptions;
using hermes::adapter::fs::IoStatus;
using hermes::adapter::fs::MpiioApi;

namespace hermes::adapter::fs {

/** State for the MPI I/O trait */
struct MpiioIoClientHeader : public TraitHeader {
  explicit MpiioIoClientHeader(const std::string &trait_uuid,
                               const std::string &trait_name)
      : TraitHeader(trait_uuid, trait_name,
                    HERMES_TRAIT_FLUSH) {}
};

/** A class to represent STDIO IO file system */
class MpiioIoClient : public hermes::adapter::fs::FilesystemIoClient {
public:
  HERMES_TRAIT_H(MpiioIoClient, "mpiio_io_client")

 private:
  HERMES_MPIIO_API_T real_api; /**< pointer to real APIs */

 public:
  /** Default constructor */
  MpiioIoClient() {
    real_api = HERMES_MPIIO_API;
    CreateHeader<MpiioIoClientHeader>("mpiio_io_client_", trait_name_);
  }

  /** Trait deserialization constructor */
  explicit MpiioIoClient(hshm::charbuf &params) {
    (void) params;
    CreateHeader<MpiioIoClientHeader>("mpiio_io_client_", trait_name_);
  }

  /** Virtual destructor */
  virtual ~MpiioIoClient() = default;

 public:
  /** Allocate an fd for the file f */
  void RealOpen(File &f,
                AdapterStat &stat,
                const std::string &path) override;

  /**
   * Called after real open. Allocates the Hermes representation of
   * identifying file information, such as a hermes file descriptor
   * and hermes file handler. These are not the same as STDIO file
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

  /** Initialize I/O context using count + datatype */
  static size_t IoSizeFromCount(int count,
                                MPI_Datatype datatype,
                                FsIoOptions &opts);

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

  /** Update the I/O status after a ReadBlob or WriteBlob */
  void UpdateIoStatus(size_t count, IoStatus &status);
};

}  // namespace hermes::adapter::fs

/** Simplify access to the stateless StdioIoClient Singleton */
#define HERMES_MPIIO_IO_CLIENT \
  hshm::EasySingleton<hermes::adapter::fs::MpiioIoClient>::GetInstance()
#define HERMES_MPIIO_IO_CLIENT_T hermes::adapter::fs::MpiioIoClient*

#endif  // HERMES_ADAPTER_MPIIO_MPIIO_IO_CLIENT_H_
