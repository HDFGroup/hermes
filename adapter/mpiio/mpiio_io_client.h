//
// Created by lukemartinlogan on 2/4/23.
//

#ifndef HERMES_ADAPTER_MPIIO_MPIIO_IO_CLIENT_H_
#define HERMES_ADAPTER_MPIIO_MPIIO_IO_CLIENT_H_

#include <memory>

#include "adapter/filesystem/filesystem_io_client.h"
#include "mpiio_api.h"

using hermes::adapter::IoClientStats;
using hermes::adapter::IoClientContext;
using hermes::adapter::IoStatus;
using hermes::adapter::fs::MpiioApi;

namespace hermes::adapter::fs {

/** A class to represent STDIO IO file system */
class MpiioIoClient : public hermes::adapter::fs::FilesystemIoClient {
 private:
  HERMES_MPIIO_API_T real_api; /**< pointer to real APIs */

 public:
  /** Default constructor */
  MpiioIoClient() { real_api = HERMES_MPIIO_API; }

  /** Virtual destructor */
  virtual ~MpiioIoClient() = default;

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

  /** Initialize I/O context using count + datatype */
  static size_t IoSizeFromCount(int count,
                                MPI_Datatype datatype,
                                IoClientContext &opts);

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

  /** Update the I/O status after a ReadBlob or WriteBlob */
  void UpdateIoStatus(size_t count, IoStatus &status);
};

}  // namespace hermes::adapter::fs

/** Simplify access to the stateless StdioIoClient Singleton */
#define HERMES_MPIIO_IO_CLIENT \
  hermes::EasySingleton<hermes::adapter::fs::MpiioIoClient>::GetInstance()
#define HERMES_MPIIO_IO_CLIENT_T hermes::adapter::fs::MpiioIoClient*

#endif  // HERMES_ADAPTER_MPIIO_MPIIO_IO_CLIENT_H_
