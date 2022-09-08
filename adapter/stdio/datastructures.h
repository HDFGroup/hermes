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

#ifndef HERMES_STDIO_ADAPTER_DATASTRUCTURES_H
#define HERMES_STDIO_ADAPTER_DATASTRUCTURES_H

/**
 * Standard header
 */
#include <ftw.h>
#include <set>
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
#include <traits.h>

/**
 * Namespace simplification.
 */
namespace hapi = hermes::api;

namespace hermes::adapter::stdio {

/**
 * FileID structure used as an identifier in STDIO adapter.
 */
struct FileID {
  /**
   * attributes
   */
  dev_t dev_id_;     // device id to place the file.
  ino_t inode_num_;  // inode number refering to the file.
  /**
   * Constructor
   */
  FileID() : dev_id_(), inode_num_() {} /* default constructor */
  FileID(dev_t dev_id, ino_t inode_num)
      : dev_id_(dev_id),
        inode_num_(inode_num) {} /* parameterized constructor */
  FileID(const FileID &other)
      : dev_id_(other.dev_id_),
        inode_num_(other.inode_num_) {} /* copy constructor*/
  FileID(FileID &&other)
      : dev_id_(other.dev_id_),
        inode_num_(other.inode_num_) {} /* move constructor*/

  /**
   * Operators defined
   */
  /* Assignment operator. */
  FileID &operator=(const FileID &other) {
    dev_id_ = other.dev_id_;
    inode_num_ = other.inode_num_;
    return *this;
  }

  /* Equal operator. */
  bool operator==(const FileID &o) const {
    return dev_id_ == o.dev_id_ && inode_num_ == o.inode_num_;
  }
};

/**
 * Structure STDIO adapter uses to define a file state.
 */
struct FileStruct {
  /**
   * attributes
   */
  FileID file_id_;  // fileID to identify a file uniquely.
  size_t offset_;   // file pointer within the file.
  size_t size_;     // size of data refered in file.
  /**
   * Constructor
   */
  FileStruct() : file_id_(), offset_(0), size_(0) {} /* default constructor */
  FileStruct(FileID file_id, size_t offset, size_t size)
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
 * Structure STDIO adapter uses to define Hermes blob.
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

/**
 * Stat which defines File within STDIO Adapter.
 */
struct AdapterStat {
  /**
   * attributes
   */
  std::shared_ptr<hapi::Bucket> st_bkid; /* bucket associated with the file */
  /** VBucket for persisting data asynchronously. */
  std::shared_ptr<hapi::VBucket> st_vbkt;
  /** Used for async flushing. */
  std::shared_ptr<hapi::PersistTrait> st_persist;
  std::set<std::string, bool (*)(const std::string &, const std::string &)>
      st_blobs;         /* Blobs access in the bucket */
  i32 ref_count;        /* # of time process opens a file */
  mode_t st_mode;       /* protection */
  uid_t st_uid;         /* user ID of owner */
  gid_t st_gid;         /* group ID of owner */
  off_t st_size;        /* total size, in bytes */
  off_t st_ptr;         /* Current ptr of FILE */
  blksize_t st_blksize; /* blocksize for blob within bucket */
  timespec st_atim;     /* time of last access */
  timespec st_mtim;     /* time of last modification */
  timespec st_ctim;     /* time of last status change */
  /**
   * Constructor
   */
  AdapterStat()
      : st_bkid(),
        st_vbkt(),
        st_persist(),
        st_blobs(CompareBlobs),
        ref_count(),
        st_mode(),
        st_uid(),
        st_gid(),
        st_size(0),
        st_ptr(0),
        st_blksize(4096),
        st_atim(),
        st_mtim(),
        st_ctim() {} /* default constructor */
  explicit AdapterStat(const struct stat &st)
      : st_bkid(),
        st_vbkt(),
        st_persist(),
        st_blobs(CompareBlobs),
        ref_count(1),
        st_mode(st.st_mode),
        st_uid(st.st_uid),
        st_gid(st.st_gid),
        st_size(st.st_size),
        st_ptr(0),
        st_blksize(st.st_blksize),
        st_atim(st.st_atim),
        st_mtim(st.st_mtim),
        st_ctim(st.st_ctim) {} /* parameterized constructor */

  /**
   * Comparator for comparing two blobs.
   */
  static bool CompareBlobs(const std::string &a, const std::string &b) {
    return std::stol(a) < std::stol(b);
  }
};

}  // namespace hermes::adapter::stdio

/**
 * Define hash functions for STDIO Adapter.
 */
namespace std {

/**
 * hash for FileID.
 */
template <>
struct hash<hermes::adapter::stdio::FileID> {
  std::size_t operator()(const hermes::adapter::stdio::FileID &key) const {
    std::size_t result = 0;
    std::size_t h1 = std::hash<dev_t>{}(key.dev_id_);
    std::size_t h2 = std::hash<ino_t>{}(key.inode_num_);
    result = h1 ^ (h2 << 1);

    return result;
  }
};
}  // namespace std

#endif  // HERMES_STDIO_ADAPTER_DATASTRUCTURES_H
