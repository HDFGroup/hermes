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

#ifndef HERMES_ABSTRACT_ADAPTER_H
#define HERMES_ABSTRACT_ADAPTER_H

#include <mpi.h>
#include "hermes_types.h"
#include "adapter/adapter_types.h"
#include <future>

namespace hapi = hermes::api;

namespace hermes::adapter {

/** Represents an object in the I/O client (e.g., a file) */
struct IoClientObject {
  AdapterType type_;     /**< Client to forward I/O request to */
  std::string filename_;  /**< Filename to read from */

  int hermes_fd_;          /**< fake file descriptor (SCRATCH MODE) */
  FILE *hermes_fh_;        /**< fake file handler (SCRATCH MODE) */
  MPI_File hermes_mpi_fh_; /**< fake MPI file handler (SCRATCH MODE) */

  bool status_;    /**< status */
  int mpi_status_; /**< MPI status */

  /** Default constructor */
  IoClientObject()
      : type_(AdapterType::kNone),
        filename_(),
        hermes_fd_(-1),
        hermes_fh_(nullptr),
        hermes_mpi_fh_(nullptr),
        status_(true),
        mpi_status_(MPI_SUCCESS) {}
};

/** Put or get data directly from I/O client */
#define HERMES_IO_CLIENT_BYPASS (1 << 0)
/** Only put or get data from a Hermes buffer; no fallback to I/O client */
#define HERMES_IO_CLIENT_NO_FALLBACK (1 << 1)
/** Whether to perform seek */
#define HERMES_FS_SEEK (1 << 2)
/** Whether to perform truncate */
#define HERMES_FS_TRUNC (1 << 3)
/** The number of I/O client flags (used for extending flag field) */
#define HERMES_IO_CLIENT_FLAGS_COUNT 4

/** Represents any relevant settings for an I/O client operation */
struct IoClientContext {
  // TODO(llogan): We should use an std::variant or union instead of large set
  AdapterType type_;              /**< Client to forward I/O request to */
  AdapterMode adapter_mode_;      /**< Current adapter mode for this obj */
  hapi::PlacementPolicy dpe_;     /**< data placement policy */
  bitfield32_t flags_;            /**< various I/O flags */
  MPI_Datatype mpi_type_; /**< MPI data type */
  int mpi_count_;         /**< The number of types */
  size_t backend_off_;    /**< Offset in the backend to begin I/O */
  size_t backend_size_;   /**< Size of I/O to perform at backend */

  /** Default constructor */
  IoClientContext() : type_(AdapterType::kNone),
                      dpe_(hapi::PlacementPolicy::kNone),
                      flags_(),
                      mpi_type_(MPI_CHAR),
                      mpi_count_(0),
                      backend_off_(0),
                      backend_size_(0) {}

  /** Contains only the AdapterType */
  explicit IoClientContext(AdapterType type)
      : type_(type),
        dpe_(hapi::PlacementPolicy::kNone),
        flags_(),
        mpi_type_(MPI_CHAR),
        mpi_count_(0),
        backend_off_(0),
        backend_size_(0) {}

  /** Enable seek for this I/O */
  void SetSeek() {
    flags_.SetBits(HERMES_FS_SEEK);
  }

  /** Disable seek for this I/O */
  void UnsetSeek() {
    flags_.UnsetBits(HERMES_FS_SEEK);
  }

  /** Whether or not to perform seek in FS adapter */
  bool DoSeek() const {
    return flags_.OrBits(HERMES_FS_SEEK);
  }

  /** Marks the file as truncated */
  void MarkTruncated() {
    flags_.SetBits(HERMES_FS_TRUNC);
  }

  /** Whether a file is marked truncated */
  bool IsTruncated() const {
    return flags_.OrBits(HERMES_FS_TRUNC);
  }
};

/** Any relevant statistics from the I/O client */
struct IoClientStats {
  std::string path_;     /**< The URL of this file */
  int flags_;            /**< open() flags for POSIX */
  mode_t st_mode_;       /**< protection */
  size_t backend_size_;  /**< size of the object in the backend */
  uid_t st_uid_;         /**< user ID of owner */
  gid_t st_gid_;         /**< group ID of owner */
  off64_t st_ptr_;       /**< current ptr of FILE */
  timespec st_atim_;     /**< time of last access */
  timespec st_mtim_;     /**< time of last modification */
  timespec st_ctim_;     /**< time of last status change */
  std::string mode_str_; /**< mode used for fopen() */
  AdapterMode adapter_mode_;  /**< Mode used for adapter */

  int fd_;          /**< real file descriptor */
  FILE *fh_;        /**< real STDIO file handler */
  MPI_File mpi_fh_; /**< real MPI file handler */

  bool is_trunc_;  /**< File is truncated */
  bool is_append_; /**< File is in append mode */
  int amode_;      /**< access mode (MPI) */
  MPI_Info info_;  /**< Info object (handle) */
  MPI_Comm comm_;  /**< Communicator for the file.*/
  bool atomicity_; /**< Consistency semantics for data-access */

  /** Default constructor */
  IoClientStats()
      : flags_(0),
        st_mode_(),
        st_ptr_(0),
        st_atim_(),
        st_mtim_(),
        st_ctim_(),
        is_trunc_(false),
        is_append_(false),
        amode_(0),
        comm_(MPI_COMM_SELF),
        atomicity_(false) {}

  /** Update to the current time */
  void UpdateTime() {
    struct timespec ts;
    timespec_get(&ts, TIME_UTC);
    st_mtim_ = ts;
    st_ctim_ = ts;
  }
};

/** Any statistics which need to be globally maintained across ranks */
struct GlobalIoClientState {
  size_t true_size_;
};

/** A structure to represent IO status */
struct IoStatus {
  size_t size_;           /**< POSIX/STDIO return value */
  int mpi_ret_;                /**< MPI return value */
  MPI_Status mpi_status_;      /**< MPI status */
  MPI_Status *mpi_status_ptr_; /**< MPI status pointer */
  bool success_;               /**< Whether the I/O succeeded */

  /** Default constructor */
  IoStatus() : size_(0),
               mpi_ret_(MPI_SUCCESS),
               mpi_status_ptr_(&mpi_status_),
               success_(true) {}
};

/** A structure to represent Hermes request */
struct HermesRequest {
  std::future<size_t> return_future; /**< future result of async op. */
  IoStatus io_status;                /**< IO status */
};

/**
 * A class to represent abstract I/O client.
 * Used internally by BORG and certain adapter classes.
 * */
class IoClient {
 public:
  /** Default constructor */
  IoClient() = default;

  /** Virtual destructor */
  virtual ~IoClient() = default;

  /** Get initial statistics from the backend */
  virtual void InitBucketState(const hipc::charbuf &bkt_name,
                               const IoClientContext &opts,
                               GlobalIoClientState &stat) = 0;

  /**
   * How to update the backend when registering a blob
   * */
  virtual void RegisterBlob(const IoClientContext &opts,
                            GlobalIoClientState &stat) = 0;

  /**
   * How to update the backend when unregistering a blob
   * */
  virtual void UnregisterBlob(const IoClientContext &opts,
                              GlobalIoClientState &stat) = 0;

  /** Write blob to backend */
  virtual void WriteBlob(const hipc::charbuf &bkt_name,
                         const Blob &full_blob,
                         const IoClientContext &opts,
                         IoStatus &status) = 0;

  /** Read blob from the backend */
  virtual void ReadBlob(const hipc::charbuf &bkt_name,
                        Blob &full_blob,
                        const IoClientContext &opts,
                        IoStatus &status) = 0;

  /** Decode I/O client context from the blob name */
  virtual IoClientContext DecodeBlobName(const IoClientContext &opts,
                                         const std::string &blob_name) = 0;
};

}  // namespace hermes::adapter

#endif  // HERMES_ABSTRACT_ADAPTER_H
