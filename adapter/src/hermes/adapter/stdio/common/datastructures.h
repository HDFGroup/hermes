//
// Created by manihariharan on 12/23/20.
//

#ifndef HERMES_DATASTRUCTURES_H
#define HERMES_DATASTRUCTURES_H
#include <bucket.h>
#include <buffer_pool.h>
#include <ftw.h>
#include <hermes_types.h>

#include <string>

namespace hapi = hermes::api;

namespace hermes::adapter::stdio {
struct FileStruct {
  size_t offset_;
  size_t size_;
  FileStruct() : offset_(0), size_(0) {}
  FileStruct(size_t offset, size_t size) : offset_(offset), size_(size) {}
  FileStruct(const FileStruct &other)
      : offset_(other.offset_), size_(other.size_) {} /* copy constructor*/
  FileStruct(FileStruct &&other)
      : offset_(other.offset_), size_(other.size_) {} /* move constructor*/
  FileStruct &operator=(const FileStruct &other) {
    offset_ = other.offset_;
    size_ = other.size_;
    return *this;
  }
};

struct HermesStruct {
  std::string blob_name_;
  size_t offset_;
  size_t size_;
  HermesStruct() : blob_name_(), offset_(0), size_(0) {}
  HermesStruct(const HermesStruct &other)
      : blob_name_(other.blob_name_),
        offset_(other.offset_),
        size_(other.size_) {} /* copy constructor*/
  HermesStruct(HermesStruct &&other)
      : blob_name_(other.blob_name_),
        offset_(other.offset_),
        size_(other.size_) {} /* move constructor*/
  HermesStruct &operator=(const HermesStruct &other) {
    blob_name_ = other.blob_name_;
    offset_ = other.offset_;
    size_ = other.size_;
    return *this;
  }
};

struct FileID {
  dev_t dev_id_;
  ino_t inode_num_;
  FileID() : dev_id_(), inode_num_() {}
  FileID(dev_t dev_id, ino_t inode_num)
      : dev_id_(dev_id), inode_num_(inode_num) {}
  bool operator==(const FileID &o) const {
    return dev_id_ == o.dev_id_ && inode_num_ == o.inode_num_;
  }
};

struct AdapterStat {
  std::shared_ptr<hapi::Bucket> st_bkid; /* bucket associated with the file */
  i32 ref_count;                         /* # of time process opens a file */
  mode_t st_mode;                        /* protection */
  uid_t st_uid;                          /* user ID of owner */
  gid_t st_gid;                          /* group ID of owner */
  off_t st_size;                         /* total size, in bytes */
  off_t st_ptr;                          /* Current ptr of FILE */
  blksize_t st_blksize;                  /* blocksize for blob within bucket */
  timespec st_atim;                      /* time of last access */
  timespec st_mtim;                      /* time of last modification */
  timespec st_ctim;                      /* time of last status change */
  AdapterStat()
      : st_bkid(),
        ref_count(),
        st_mode(),
        st_uid(),
        st_gid(),
        st_size(0),
        st_ptr(0),
        st_blksize(4096),
        st_atim(),
        st_mtim(),
        st_ctim() {}
  AdapterStat(const struct stat &st)
      : st_bkid(),
        ref_count(1),
        st_mode(st.st_mode),
        st_uid(st.st_uid),
        st_gid(st.st_gid),
        st_size(st.st_size),
        st_ptr(0),
        st_blksize(st.st_blksize),
        st_atim(st.st_atim),
        st_mtim(st.st_mtim),
        st_ctim(st.st_ctim) {}
};

}  // namespace hermes::adapter::stdio

namespace std {

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

#endif  // HERMES_DATASTRUCTURES_H
