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

#ifndef HERMES_MPIIO_ADAPTER_DATASTRUCTURES_H
#define HERMES_MPIIO_ADAPTER_DATASTRUCTURES_H

/**
 * Standard header
 */
#include <ftw.h>

#include <string>

/**
 * Dependent library header
 */

/**
 * Internal header
 */
#include <bucket.h>
#include <buffer_pool.h>
#include <hermes_types.h>
#include <mpi.h>

/**
 * Namespace simplification.
 */
namespace hapi = hermes::api;

namespace hermes::adapter::mpiio {

/**
 * Structure MPIIO adapter uses to define a file state.
 */
struct FileStruct {
  /**
   * attributes
   */
  MPI_File* file_id_;  // fileID to identify a file uniquely.
  size_t offset_;     // file pointer within the file.
  size_t size_;       // size of data refered in file.
  /**
   * Constructor
   */
  FileStruct() : file_id_(), offset_(0), size_(0) {} /* default constructor */
  FileStruct(MPI_File* file_id, size_t offset, size_t size)
      : file_id_(file_id),
        offset_(offset),
        size_(size) {} /* parameterized constructor */
  FileStruct(const FileStruct &other)
      : file_id_(other.file_id_),
        offset_(other.offset_),
        size_(other.size_) {} /* copy constructor*/
  FileStruct(FileStruct &&other)
      : file_id_(other.file_id_),
        offset_(other.offset_),
        size_(other.size_) {} /* move constructor*/
  /**
   * Operators defined
   */
  /* Assignment operator. */
  FileStruct &operator=(const FileStruct &other) {
    file_id_ = other.file_id_;
    offset_ = other.offset_;
    size_ = other.size_;
    return *this;
  }
};

/**
 * Structure MPIIO adapter uses to define Hermes blob.
 */
struct HermesStruct {
  /**
   * attributes
   */
  std::string blob_name_;
  size_t offset_;
  size_t size_;
  /**
   * Constructor
   */
  HermesStruct()
      : blob_name_(), offset_(0), size_(0) {} /* default constructor */
  HermesStruct(const HermesStruct &other)
      : blob_name_(other.blob_name_),
        offset_(other.offset_),
        size_(other.size_) {} /* copy constructor*/
  HermesStruct(HermesStruct &&other)
      : blob_name_(other.blob_name_),
        offset_(other.offset_),
        size_(other.size_) {} /* move constructor*/
  /**
   * Operators defined
   */
  /* Assignment operator. */
  HermesStruct &operator=(const HermesStruct &other) {
    blob_name_ = other.blob_name_;
    offset_ = other.offset_;
    size_ = other.size_;
    return *this;
  }
};

typedef std::set<std::string,
                 bool (*)(const std::string &, const std::string &)>
    BlobSet_t;

/**
 * Stat which defines File within MPIIO Adapter.
 */
struct AdapterStat {
  /**
   * attributes
   */
  std::shared_ptr<hapi::Bucket> st_bkid; /* bucket associated with the file */
  BlobSet_t st_blobs;                    /* Blobs access in the bucket */
  i32 ref_count;                         /* # of time process opens a file */
  int a_mode;                            /* access mode */
  MPI_Info info;                         /* Info object (handle) */
  MPI_Comm comm;                         /* Communicator for the file.*/
  MPI_Offset size;                       /* total size, in bytes */
  MPI_Offset ptr;                        /* Current ptr of FILE */
  bool atomicity; /* Consistency semantics for data-access */
  /**
   * Constructor
   */
  AdapterStat()
      : st_bkid(),
        st_blobs(CompareBlobs),
        ref_count(),
        a_mode(),
        info(),
        comm(),
        size(0),
        ptr(0),
        atomicity(true) {} /* default constructor */
  /**
   * Comparator for comparing two blobs.
   */
  static bool CompareBlobs(const std::string &a, const std::string &b) {
    return std::stol(a) < std::stol(b);
  }
};

}  // namespace hermes::adapter::mpiio
#endif  // HERMES_MPIIO_ADAPTER_DATASTRUCTURES_H
