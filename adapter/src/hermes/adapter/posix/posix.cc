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

#include <hermes/adapter/posix.h>

#include <experimental/filesystem>
#include <hermes/adapter/interceptor.cc>
#include <hermes/adapter/posix/mapper/balanced_mapper.cc>
#include <hermes/adapter/posix/metadata_manager.cc>

using hermes::adapter::posix::AdapterStat;
using hermes::adapter::posix::FileStruct;
using hermes::adapter::posix::MapperFactory;
using hermes::adapter::posix::MetadataManager;

namespace hapi = hermes::api;
namespace fs = std::experimental::filesystem;
/**
 * Internal Functions
 */
size_t perform_file_write(std::string &filename, off_t offset, size_t size,
                          unsigned char *data_ptr) {
  LOG(INFO) << "Writing to file: " << filename << " offset: " << offset
            << " of size:" << size << "." << std::endl;
  INTERCEPTOR_LIST->hermes_flush_exclusion.insert(filename);
  int fd = open(filename.c_str(), O_RDWR | O_CREAT);
  size_t write_size = 0;
  if (fd != -1) {
    auto status = lseek(fd, offset, SEEK_SET);
    if (status == offset) {
      write_size = write(fd, data_ptr, size);
      status = close(fd);
    }
  }
  INTERCEPTOR_LIST->hermes_flush_exclusion.erase(filename);
  return write_size;
}

size_t perform_file_read(const char *filename, off_t file_offset, void *ptr,
                         size_t ptr_offset, size_t size) {
  LOG(INFO) << "Read called for filename from destination: " << filename
            << " on offset: " << file_offset << " and size: " << size
            << std::endl;
  INTERCEPTOR_LIST->hermes_flush_exclusion.insert(filename);
  int fd = open(filename, O_RDONLY);
  size_t read_size = 0;
  if (fd != -1) {
    int status = lseek(fd, file_offset, SEEK_SET);
    if (status == file_offset) {
      read_size = read(fd, (char *)ptr + ptr_offset, size);
    }
    status = close(fd);
  }
  INTERCEPTOR_LIST->hermes_flush_exclusion.erase(filename);
  return read_size;
}
int simple_open(int ret, const std::string &path_str, int flags) {
  LOG(INFO) << "Open file for filename " << path_str << " with flags " << flags
            << std::endl;
  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
  if (!ret) {
    return ret;
  } else {
    auto existing = mdm->Find(ret);
    if (!existing.second) {
      LOG(INFO) << "File not opened before by adapter" << std::endl;
      struct stat st;
      int status = fstat(ret, &st);
      if (status == 0) {
        AdapterStat stat(st);
        stat.ref_count = 1;
        struct timespec ts;
        timespec_get(&ts, TIME_UTC);
        stat.st_atim = ts;
        stat.st_mtim = ts;
        stat.st_ctim = ts;
        if (flags & O_APPEND) {
          /* FIXME: get current size of bucket from Hermes*/
          stat.st_ptr = stat.st_size;
        }
        /* FIXME(hari) check if this initialization is correct. */
        mdm->InitializeHermes();
        // TODO(hari) how to pass to hermes to make a private bucket
        stat.st_bkid =
          std::make_shared<hapi::Bucket>(path_str, mdm->GetHermes());
        mdm->Create(ret, stat);
      } else {
        // TODO(hari): @error_handling invalid fh.
        ret = -1;
      }
    } else {
      LOG(INFO) << "File opened before by adapter" << std::endl;
      existing.first.ref_count++;
      struct timespec ts;
      timespec_get(&ts, TIME_UTC);
      existing.first.st_atim = ts;
      existing.first.st_ctim = ts;
      mdm->Update(ret, existing.first);
    }
  }
  return ret;
}

int open_internal(const std::string &path_str, int flags, int mode) {
  int ret;
  MAP_OR_FAIL(open);
  if (flags & O_CREAT || flags & O_TMPFILE) {
    ret = real_open_(path_str.c_str(), flags, mode);
  } else {
    ret = real_open_(path_str.c_str(), flags);
  }
  ret = simple_open(ret, path_str.c_str(), flags);
  return ret;
}

size_t write_internal(std::pair<AdapterStat, bool> &existing, const void *ptr,
                      size_t total_size, int fp) {
  LOG(INFO) << "Write called for filename: "
            << existing.first.st_bkid->GetName()
            << " on offset: " << existing.first.st_ptr
            << " and size: " << total_size << std::endl;
  size_t ret;
  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
  auto mapper = MapperFactory().Get(kMapperType);
  auto mapping = mapper->map(
      FileStruct(mdm->Convert(fp), existing.first.st_ptr, total_size));
  size_t data_offset = 0;
  auto filename = existing.first.st_bkid->GetName();
  LOG(INFO) << "Mapping for write has " << mapping.size() << " mapping."
            << std::endl;
  for (const auto &item : mapping) {
    hapi::Context ctx;
    auto index = std::stol(item.second.blob_name_) - 1;
    auto blob_exists =
        existing.first.st_bkid->ContainsBlob(item.second.blob_name_);
    unsigned char *put_data_ptr = (unsigned char *)ptr + data_offset;
    size_t put_data_ptr_size = item.first.size_;

    if (!blob_exists || item.second.size_ == kPageSize) {
      LOG(INFO) << "Create or Overwrite blob " << item.second.blob_name_
                << " of size:" << item.second.size_ << "." << std::endl;
      if (item.second.size_ == kPageSize) {
        auto status = existing.first.st_bkid->Put(
            item.second.blob_name_, put_data_ptr, put_data_ptr_size, ctx);
        if (status.Failed()) {
          perform_file_write(filename, item.first.offset_, put_data_ptr_size,
                             put_data_ptr);
        } else {
          existing.first.st_blobs.emplace(item.second.blob_name_);
        }
      } else if (item.second.offset_ == 0) {
        auto status = existing.first.st_bkid->Put(
            item.second.blob_name_, put_data_ptr, put_data_ptr_size, ctx);
        if (status.Failed()) {
          perform_file_write(filename, index * kPageSize, put_data_ptr_size,
                             put_data_ptr);
        } else {
          existing.first.st_blobs.emplace(item.second.blob_name_);
        }
      } else {
        hapi::Blob final_data(item.second.offset_ + item.second.size_);
        if (fs::exists(filename) &&
            fs::file_size(filename) >= item.second.offset_) {
          LOG(INFO) << "Blob has a gap in write. read gap from original file."
                    << std::endl;
          INTERCEPTOR_LIST->hermes_flush_exclusion.insert(filename);
          FILE *fh = fopen(filename.c_str(), "r");
          if (fh != nullptr) {
            if (fseek(fh, index * kPageSize, SEEK_SET) == 0) {
              size_t items_read = fread(final_data.data(), item.second.offset_,
                                        sizeof(char), fh);
              if (items_read != 1) {
                // TODO(hari) @errorhandling read failed.
              }
              if (fclose(fh) != 0) {
                // TODO(hari) @errorhandling fclose failed.
              }
            } else {
              // TODO(hari) @errorhandling fseek failed.
            }
          } else {
            // TODO(hari) @errorhandling FILE cannot be opened
          }
          INTERCEPTOR_LIST->hermes_flush_exclusion.erase(filename);
        }
        memcpy(final_data.data() + item.second.offset_, put_data_ptr,
               put_data_ptr_size);
        auto status = existing.first.st_bkid->Put(item.second.blob_name_,
                                                  final_data, ctx);
        if (status.Failed()) {
          perform_file_write(filename, index * kPageSize, final_data.size(),
                             final_data.data());
        } else {
          existing.first.st_blobs.emplace(item.second.blob_name_);
        }
      }

    } else {
      LOG(INFO) << "Blob " << item.second.blob_name_
                << " of size:" << item.second.size_ << " exists." << std::endl;
      hapi::Blob temp(0);
      auto existing_blob_size =
          existing.first.st_bkid->Get(item.second.blob_name_, temp, ctx);
      if (item.second.offset_ == 0) {
        LOG(INFO) << "Blob offset is 0" << std::endl;
        if (item.second.size_ >= existing_blob_size) {
          LOG(INFO) << "Overwrite blob " << item.second.blob_name_
                    << " of size:" << item.second.size_ << "." << std::endl;
          auto status = existing.first.st_bkid->Put(
              item.second.blob_name_, put_data_ptr, put_data_ptr_size, ctx);
          if (status.Failed()) {
            perform_file_write(filename, index * kPageSize, put_data_ptr_size,
                               put_data_ptr);
          } else {
            existing.first.st_blobs.emplace(item.second.blob_name_);
          }
        } else {
          LOG(INFO) << "Update blob " << item.second.blob_name_
                    << " of size:" << existing_blob_size << "." << std::endl;
          hapi::Blob existing_data(existing_blob_size);
          existing.first.st_bkid->Get(item.second.blob_name_, existing_data,
                                      ctx);
          memcpy(existing_data.data(), put_data_ptr, put_data_ptr_size);
          auto status = existing.first.st_bkid->Put(item.second.blob_name_,
                                                    existing_data, ctx);
          if (status.Failed()) {
            perform_file_write(filename, index * kPageSize, existing_blob_size,
                               existing_data.data());
          } else {
            existing.first.st_blobs.emplace(item.second.blob_name_);
          }
        }
      } else {
        LOG(INFO) << "Blob offset: " << item.second.offset_ << "." << std::endl;
        auto new_size = item.second.offset_ + item.second.size_;
        hapi::Blob existing_data(existing_blob_size);
        existing.first.st_bkid->Get(item.second.blob_name_, existing_data, ctx);
        existing.first.st_bkid->DeleteBlob(item.second.blob_name_, ctx);
        if (new_size < existing_blob_size) {
          new_size = existing_blob_size;
        }
        hapi::Blob final_data(new_size);
        auto existing_data_cp_size = existing_data.size() >= item.second.offset_
                                         ? item.second.offset_
                                         : existing_data.size();
        memcpy(final_data.data(), existing_data.data(), existing_data_cp_size);

        if (existing_blob_size < item.second.offset_ + 1 &&
            fs::exists(filename) &&
            fs::file_size(filename) >=
                item.second.offset_ + item.second.size_) {
          size_t size_to_read = item.second.offset_ - existing_blob_size;
          LOG(INFO) << "Blob has a gap in update read gap from original file."
                    << std::endl;
          INTERCEPTOR_LIST->hermes_flush_exclusion.insert(filename);
          FILE *fh = fopen(filename.c_str(), "r");
          if (fh != nullptr) {
            if (fseek(fh, index * kPageSize + existing_data_cp_size,
                      SEEK_SET) == 0) {
              size_t items_read =
                  fread(final_data.data() + existing_data_cp_size, size_to_read,
                        sizeof(char), fh);
              if (items_read != 1) {
                // TODO(hari) @errorhandling read failed.
              }
              if (fclose(fh) != 0) {
                // TODO(hari) @errorhandling fclose failed.
              }
            } else {
              // TODO(hari) @errorhandling fseek failed.
            }
          } else {
            // TODO(hari) @errorhandling FILE cannot be opened
          }
          INTERCEPTOR_LIST->hermes_flush_exclusion.erase(filename);
        }
        memcpy(final_data.data() + item.second.offset_, put_data_ptr,
               put_data_ptr_size);
        if (item.second.offset_ + item.second.size_ < existing_blob_size) {
          LOG(INFO) << "Retain last portion of blob as Blob is bigger than the "
                       "update."
                    << std::endl;
          auto off_t = item.second.offset_ + item.second.size_;
          memcpy(final_data.data() + off_t, existing_data.data() + off_t,
                 existing_blob_size - off_t);
        }
        auto status = existing.first.st_bkid->Put(item.second.blob_name_,
                                                  final_data, ctx);
        if (status.Failed()) {
          perform_file_write(filename, index * kPageSize, new_size,
                             final_data.data());
        } else {
          existing.first.st_blobs.emplace(item.second.blob_name_);
        }
      }
    }
    data_offset += item.first.size_;
  }
  existing.first.st_ptr += data_offset;
  existing.first.st_size = existing.first.st_size >= existing.first.st_ptr
                               ? existing.first.st_size
                               : existing.first.st_ptr;
  struct timespec ts;
  timespec_get(&ts, TIME_UTC);
  existing.first.st_mtim = ts;
  existing.first.st_ctim = ts;
  mdm->Update(fp, existing.first);
  ret = data_offset;
  return ret;
}

size_t read_internal(std::pair<AdapterStat, bool> &existing, void *ptr,
                     size_t total_size, int fd) {
  LOG(INFO) << "Read called for filename: " << existing.first.st_bkid->GetName()
            << " on offset: " << existing.first.st_ptr
            << " and size: " << total_size << std::endl;
  if (existing.first.st_ptr >= existing.first.st_size) return 0;
  size_t ret;
  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
  auto mapper = MapperFactory().Get(kMapperType);
  auto mapping = mapper->map(
      FileStruct(mdm->Convert(fd), existing.first.st_ptr, total_size));
  size_t total_read_size = 0;
  auto filename = existing.first.st_bkid->GetName();
  LOG(INFO) << "Mapping for read has " << mapping.size() << " mapping."
            << std::endl;
  for (const auto &item : mapping) {
    hapi::Context ctx;
    auto blob_exists =
        existing.first.st_bkid->ContainsBlob(item.second.blob_name_);
    hapi::Blob read_data(0);
    size_t read_size = 0;
    if (blob_exists) {
      LOG(INFO) << "Blob exists and need to read from Hermes from blob: "
                << item.second.blob_name_ << "." << std::endl;
      auto exiting_blob_size =
          existing.first.st_bkid->Get(item.second.blob_name_, read_data, ctx);

      read_data.resize(exiting_blob_size);
      existing.first.st_bkid->Get(item.second.blob_name_, read_data, ctx);
      bool contains_blob = exiting_blob_size > item.second.offset_;
      if (contains_blob) {
        read_size = read_data.size() < item.second.offset_ + item.second.size_
                        ? exiting_blob_size - item.second.offset_
                        : item.second.size_;
        LOG(INFO) << "Blob have data and need to read from hemes "
                     "blob: "
                  << item.second.blob_name_ << " offset:" << item.second.offset_
                  << " size:" << read_size << "." << std::endl;
        memcpy((char *)ptr + total_read_size,
               read_data.data() + item.second.offset_, read_size);
        if (read_size < item.second.size_) {
          contains_blob = true;
        } else {
          contains_blob = false;
        }
      } else {
        LOG(INFO) << "Blob does not have data and need to read from original "
                     "filename: "
                  << filename << " offset:" << item.first.offset_
                  << " size:" << item.first.size_ << "." << std::endl;
        auto file_read_size =
            perform_file_read(filename.c_str(), item.first.offset_, ptr,
                              total_read_size, item.second.size_);
        read_size += file_read_size;
      }
      if (contains_blob && fs::exists(filename) &&
          fs::file_size(filename) >= item.first.offset_ + item.first.size_) {
        LOG(INFO) << "Blob does not have data and need to read from original "
                     "filename: "
                  << filename << " offset:" << item.first.offset_ + read_size
                  << " size:" << item.second.size_ - read_size << "."
                  << std::endl;
        auto new_read_size = perform_file_read(
            filename.c_str(), item.first.offset_, ptr,
            total_read_size + read_size, item.second.size_ - read_size);
        read_size += new_read_size;
      }
    } else if (fs::exists(filename) &&
               fs::file_size(filename) >=
                   item.first.offset_ + item.first.size_) {
      LOG(INFO)
          << "Blob does not exists and need to read from original filename: "
          << filename << " offset:" << item.first.offset_
          << " size:" << item.first.size_ << "." << std::endl;
      read_size = perform_file_read(filename.c_str(), item.first.offset_, ptr,
                                    total_read_size, item.first.size_);
    }
    if (read_size > 0) {
      total_read_size += read_size;
    }
  }
  existing.first.st_ptr += total_read_size;
  struct timespec ts;
  timespec_get(&ts, TIME_UTC);
  existing.first.st_atim = ts;
  existing.first.st_ctim = ts;
  mdm->Update(fd, existing.first);
  ret = total_read_size;
  return ret;
}
/**
 * MPI
 */
int HERMES_DECL(MPI_Init)(int *argc, char ***argv) {
  MAP_OR_FAIL(MPI_Init);
  int status = real_MPI_Init_(argc, argv);
  if (status == 0) {
    LOG(INFO) << "MPI Init intercepted." << std::endl;
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    mdm->InitializeHermes(true);
  }
  return status;
}

int HERMES_DECL(MPI_Finalize)(void) {
  LOG(INFO) << "MPI Finalize intercepted." << std::endl;
  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
  mdm->FinalizeHermes();
  MAP_OR_FAIL(MPI_Finalize);
  int status = real_MPI_Finalize_();
  return status;
}
/**
 * POSIX
 */
int HERMES_DECL(open)(const char *path, int flags, ...) {
  int ret;
  int mode = 0;
  if (flags & O_CREAT || flags & O_TMPFILE) {
    va_list arg;
    va_start(arg, flags);
    mode = va_arg(arg, int);
    va_end(arg);
  }
  if (hermes::adapter::IsTracked(path)) {
    if (hermes::adapter::IsRelativePath(path))
      LOG(FATAL) << "File: " << path
                 << "\nis relative. It is not supported yet";
    if (hermes::adapter::IsSymLink(path))
      LOG(FATAL) << "File: " << path
                 << "\nis symbolic link. It is not supported yet";

    LOG(INFO) << "Intercept open for filename: " << path
              << " and mode: " << flags << " is tracked." << std::endl;
    ret = open_internal(path, flags, mode);
  } else {
    MAP_OR_FAIL(open);
    if (flags & O_CREAT || flags & O_TMPFILE) {
      ret = real_open_(path, flags, mode);
    } else {
      ret = real_open_(path, flags);
    }
  }
  return (ret);
}
int HERMES_DECL(open64)(const char *path, int flags, ...) {
  int ret;
  int mode = 0;
  if (flags & O_CREAT) {
    va_list arg;
    va_start(arg, flags);
    mode = va_arg(arg, int);
    va_end(arg);
  }
  if (hermes::adapter::IsTracked(path)) {
    if (hermes::adapter::IsRelativePath(path))
      LOG(FATAL) << "File: " << path
                 << "\nis relative. It is not supported yet";
    if (hermes::adapter::IsSymLink(path))
      LOG(FATAL) << "File: " << path
                 << "\nis symbolic link. It is not supported yet";

    LOG(INFO) << "Intercept open for filename: " << path
              << " and mode: " << flags << " is tracked." << std::endl;
    ret = open_internal(path, flags, mode);
  } else {
    MAP_OR_FAIL(open64);
    if (flags & O_CREAT) {
      ret = real_open64_(path, flags, mode);
    } else {
      ret = real_open64_(path, flags);
    }
  }
  return (ret);
}
int HERMES_DECL(__open_2)(const char *path, int oflag) {
  int ret;
  if (hermes::adapter::IsTracked(path)) {
    if (hermes::adapter::IsRelativePath(path))
      LOG(FATAL) << "File: " << path
                 << "\nis relative. It is not supported yet";
    if (hermes::adapter::IsSymLink(path))
      LOG(FATAL) << "File: " << path
                 << "\nis symbolic link. It is not supported yet";

    LOG(INFO) << "Intercept __open_2 for filename: " << path
              << " and mode: " << oflag << " is tracked." << std::endl;
    ret = open_internal(path, oflag, 0);
  } else {
    MAP_OR_FAIL(__open_2);
    ret = real___open_2_(path, oflag);
  }
  return (ret);
}
int HERMES_DECL(creat)(const char *path, mode_t mode) {
  int ret;
  std::string path_str(path);
  if (hermes::adapter::IsTracked(path)) {
    LOG(INFO) << "Intercept creat for filename: " << path
              << " and mode: " << mode << " is tracked." << std::endl;
    ret = open_internal(path, O_CREAT, mode);
  } else {
    MAP_OR_FAIL(creat);
    ret = real_creat_(path, mode);
  }
  return (ret);
}
int HERMES_DECL(creat64)(const char *path, mode_t mode) {
  int ret;
  std::string path_str(path);
  if (hermes::adapter::IsTracked(path)) {
    LOG(INFO) << "Intercept creat64 for filename: " << path
              << " and mode: " << mode << " is tracked." << std::endl;
    ret = open_internal(path, O_CREAT, mode);
  } else {
    MAP_OR_FAIL(creat64);
    ret = real_creat64_(path, mode);
  }
  return (ret);
}
ssize_t HERMES_DECL(read)(int fd, void *buf, size_t count) {
  size_t ret;
  if (hermes::adapter::IsTracked(fd)) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(fd);
    if (existing.second) {
      LOG(INFO) << "Intercept read." << std::endl;
      ret = read_internal(existing, buf, count, fd);

    } else {
      MAP_OR_FAIL(pread);
      ret = real_read_(fd, buf, count);
    }
  } else {
    MAP_OR_FAIL(read);
    ret = real_read_(fd, buf, count);
  }
  return (ret);
}
ssize_t HERMES_DECL(write)(int fd, const void *buf, size_t count) {
  size_t ret;
  if (hermes::adapter::IsTracked(fd)) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(fd);
    if (existing.second) {
      LOG(INFO) << "Intercept write." << std::endl;
      ret = write_internal(existing, buf, count, fd);
    } else {
      MAP_OR_FAIL(write);
      ret = real_write_(fd, buf, count);
    }
  } else {
    MAP_OR_FAIL(write);
    ret = real_write_(fd, buf, count);
  }
  return (ret);
}
ssize_t HERMES_DECL(pread)(int fd, void *buf, size_t count, off_t offset) {
  size_t ret;
  if (hermes::adapter::IsTracked(fd)) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(fd);
    if (existing.second) {
      LOG(INFO) << "Intercept pread." << std::endl;
      int status = lseek(fd, offset, SEEK_SET);
      if (status == 0) {
        ret = read_internal(existing, buf, count, fd);
      }
    } else {
      MAP_OR_FAIL(pread);
      ret = real_pread_(fd, buf, count, offset);
    }
  } else {
    MAP_OR_FAIL(pread);
    ret = real_pread_(fd, buf, count, offset);
  }
  return (ret);
}
ssize_t HERMES_DECL(pwrite)(int fd, const void *buf, size_t count,
                            off_t offset) {
  size_t ret;
  if (hermes::adapter::IsTracked(fd)) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(fd);
    if (existing.second) {
      LOG(INFO) << "Intercept pwrite." << std::endl;
      int status = lseek(fd, offset, SEEK_SET);
      if (status == 0) {
        ret = write_internal(existing, buf, count, fd);
      }
    } else {
      MAP_OR_FAIL(pwrite);
      ret = real_pwrite_(fd, buf, count, offset);
    }
  } else {
    MAP_OR_FAIL(pwrite);
    ret = real_pwrite_(fd, buf, count, offset);
  }
  return (ret);
}
ssize_t HERMES_DECL(pread64)(int fd, void *buf, size_t count, off64_t offset) {
  size_t ret;
  if (hermes::adapter::IsTracked(fd)) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(fd);
    if (existing.second) {
      LOG(INFO) << "Intercept pread64." << std::endl;
      int status = lseek(fd, offset, SEEK_SET);
      if (status == 0) {
        ret = read_internal(existing, buf, count, fd);
      }
    } else {
      MAP_OR_FAIL(pread);
      ret = real_pread_(fd, buf, count, offset);
    }
  } else {
    MAP_OR_FAIL(pread);
    ret = real_pread_(fd, buf, count, offset);
  }
  return (ret);
}
ssize_t HERMES_DECL(pwrite64)(int fd, const void *buf, size_t count,
                              off64_t offset) {
  size_t ret;
  if (hermes::adapter::IsTracked(fd)) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(fd);
    if (existing.second) {
      LOG(INFO) << "Intercept pwrite." << std::endl;
      int status = lseek(fd, offset, SEEK_SET);
      if (status == 0) {
        ret = write_internal(existing, buf, count, fd);
      }
    } else {
      MAP_OR_FAIL(pwrite);
      ret = real_pwrite_(fd, buf, count, offset);
    }
  } else {
    MAP_OR_FAIL(pwrite);
    ret = real_pwrite_(fd, buf, count, offset);
  }
  return (ret);
}
off_t HERMES_DECL(lseek)(int fd, off_t offset, int whence) {
  int ret = -1;
  if (hermes::adapter::IsTracked(fd)) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(fd);
    if (existing.second) {
      LOG(INFO) << "Intercept lseek offset:" << offset << " whence:" << whence
                << "." << std::endl;
      if (!(existing.first.st_mode & O_APPEND)) {
        switch (whence) {
          case SEEK_SET: {
            existing.first.st_ptr = offset;
            break;
          }
          case SEEK_CUR: {
            existing.first.st_ptr += offset;
            break;
          }
          case SEEK_END: {
            existing.first.st_ptr = existing.first.st_size + offset;
            break;
          }
          default: {
            // TODO(hari): throw not implemented error.
          }
        }
        mdm->Update(fd, existing.first);
        ret = existing.first.st_ptr;
      } else {
        LOG(INFO)
            << "File pointer not updating as file was opened in append mode."
            << std::endl;
        ret = -1;
      }
    } else {
      MAP_OR_FAIL(lseek);
      ret = real_lseek_(fd, offset, whence);
    }
  } else {
    MAP_OR_FAIL(lseek);
    ret = real_lseek_(fd, offset, whence);
  }
  return (ret);
}
off64_t HERMES_DECL(lseek64)(int fd, off64_t offset, int whence) {
  int ret = -1;
  if (hermes::adapter::IsTracked(fd)) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(fd);
    if (existing.second) {
      LOG(INFO) << "Intercept lseek64 offset:" << offset << " whence:" << whence
                << "." << std::endl;
      if (!(existing.first.st_mode & O_APPEND)) {
        switch (whence) {
          case SEEK_SET: {
            existing.first.st_ptr = offset;
            break;
          }
          case SEEK_CUR: {
            existing.first.st_ptr += offset;
            break;
          }
          case SEEK_END: {
            existing.first.st_ptr = existing.first.st_size + offset;
            break;
          }
          default: {
            // TODO(hari): throw not implemented error.
          }
        }
        mdm->Update(fd, existing.first);
        ret = 0;
      } else {
        LOG(INFO)
            << "File pointer not updating as file was opened in append mode."
            << std::endl;
        ret = -1;
      }
    } else {
      MAP_OR_FAIL(lseek);
      ret = real_lseek_(fd, offset, whence);
    }
  } else {
    MAP_OR_FAIL(lseek);
    ret = real_lseek_(fd, offset, whence);
  }
  return (ret);
}
int HERMES_DECL(fsync)(int fd) {
  int ret;
  if (hermes::adapter::IsTracked(fd)) {
    LOG(INFO) << "Intercept fsync." << std::endl;
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(fd);
    if (existing.second) {
      LOG(INFO) << "File handler is opened by adapter." << std::endl;
      hapi::Context ctx;
      if (existing.first.ref_count == 1) {
        auto persist = INTERCEPTOR_LIST->Persists(fd);
        auto filename = existing.first.st_bkid->GetName();
        const auto &blob_names = existing.first.st_blobs;
        if (!blob_names.empty() && persist) {
          LOG(INFO) << "Adapter flushes " << blob_names.size()
                    << " blobs to filename:" << filename << "." << std::endl;
          INTERCEPTOR_LIST->hermes_flush_exclusion.insert(filename);
          hapi::VBucket file_vbucket(filename, mdm->GetHermes(), ctx);
          auto offset_map = std::unordered_map<std::string, hermes::u64>();

          for (const auto &blob_name : blob_names) {
            auto status = file_vbucket.Link(blob_name, filename, ctx);
            if (!status.Failed()) {
              auto page_index = std::stol(blob_name) - 1;
              offset_map.emplace(blob_name, page_index * kPageSize);
            }
          }
          auto file_mapping = hapi::FileMappingTrait(filename, offset_map,
                                                     nullptr, NULL, NULL);
          bool flush_synchronously = true;
          hapi::PersistTrait persist_trait(file_mapping, flush_synchronously);
          file_vbucket.Attach(&persist_trait, ctx);
          file_vbucket.Destroy(ctx);
          existing.first.st_blobs.clear();
          INTERCEPTOR_LIST->hermes_flush_exclusion.erase(filename);
          mdm->Update(fd, existing.first);
        }
      } else {
        LOG(INFO) << "File handler is opened by more than one fopen."
                  << std::endl;
        struct timespec ts;
        timespec_get(&ts, TIME_UTC);
        existing.first.st_atim = ts;
        existing.first.st_ctim = ts;
        mdm->Update(fd, existing.first);
      }
    }
  }

  MAP_OR_FAIL(fsync);
  ret = real_fsync_(fd);
  return (ret);
}

int HERMES_DECL(close)(int fd) {
  int ret;
  if (hermes::adapter::IsTracked(fd)) {
    LOG(INFO) << "Intercept close(" << std::to_string(fd) << ")";
    DLOG(INFO) << " -> " << hermes::adapter::GetFilenameFromFD(fd);
    LOG(INFO) << std::endl;
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(fd);
    if (existing.second) {
      LOG(INFO) << "File handler is opened by adapter." << std::endl;
      hapi::Context ctx;
      if (existing.first.ref_count == 1) {
        auto persist = INTERCEPTOR_LIST->Persists(fd);
        auto filename = existing.first.st_bkid->GetName();
        mdm->Delete(fd);
        const auto &blob_names = existing.first.st_blobs;
        if (!blob_names.empty() && persist) {
          LOG(INFO) << "Adapter flushes " << blob_names.size()
                    << " blobs to filename:" << filename << "." << std::endl;
          INTERCEPTOR_LIST->hermes_flush_exclusion.insert(filename);
          hapi::VBucket file_vbucket(filename, mdm->GetHermes(), ctx);
          auto offset_map = std::unordered_map<std::string, hermes::u64>();

          for (const auto &blob_name : blob_names) {
            auto status = file_vbucket.Link(blob_name, filename, ctx);
            if (!status.Failed()) {
              auto page_index = std::stol(blob_name) - 1;
              offset_map.emplace(blob_name, page_index * kPageSize);
            }
          }
          auto file_mapping =
            hapi::FileMappingTrait(filename, offset_map, nullptr, NULL,
                                          NULL);
          bool flush_synchronously = true;
          hapi::PersistTrait persist_trait(file_mapping, flush_synchronously);
          file_vbucket.Attach(&persist_trait, ctx);
          file_vbucket.Destroy(ctx);
          existing.first.st_blobs.clear();
          INTERCEPTOR_LIST->hermes_flush_exclusion.erase(filename);
        }
        existing.first.st_bkid->Destroy(ctx);
        mdm->FinalizeHermes();
      } else {
        LOG(INFO) << "File handler is opened by more than one fopen."
                  << std::endl;
        existing.first.ref_count--;
        struct timespec ts;
        timespec_get(&ts, TIME_UTC);
        existing.first.st_atim = ts;
        existing.first.st_ctim = ts;
        mdm->Update(fd, existing.first);
        existing.first.st_bkid->Release(ctx);
      }
    }
  }

  MAP_OR_FAIL(close);
  ret = real_close_(fd);
  return (ret);
}
