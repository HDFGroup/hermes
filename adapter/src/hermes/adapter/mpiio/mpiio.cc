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

/**
 * Internal headers
 */
#include <hermes/adapter/mpiio.h>
#include <hermes/adapter/thread_pool.h>

#include <hermes/adapter/mpiio/mapper/balanced_mapper.cc>
/**
 * Namespace declarations
 */
using hermes::adapter::ThreadPool;
using hermes::adapter::mpiio::AdapterStat;
using hermes::adapter::mpiio::FileStruct;
using hermes::adapter::mpiio::HermesRequest;
using hermes::adapter::mpiio::MapperFactory;
using hermes::adapter::mpiio::MetadataManager;

namespace hapi = hermes::api;
namespace fs = std::experimental::filesystem;
/**
 * Internal Functions.
 */
inline std::string GetFilenameFromFP(MPI_File *fh) {
  MPI_Info info_out;
  int status = MPI_File_get_info(*fh, &info_out);
  if (status != MPI_SUCCESS) {
    LOG(ERROR) << "MPI_File_get_info on file handler failed." << std::endl;
  }
  const int kMaxSize = 0xFFF;
  int flag;
  char filename[kMaxSize];
  MPI_Info_get(info_out, "filename", kMaxSize, filename, &flag);
  return filename;
}

inline bool IsTracked(MPI_File *fh) {
  if (hermes::adapter::exit) return false;
  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
  auto existing = mdm->Find(fh);
  return existing.second;
}

int simple_open(MPI_Comm &comm, const char *path, int &amode, MPI_Info &info,
                MPI_File *fh) {
  LOG(INFO) << "Open file for filename " << path << " in mode " << amode
            << std::endl;
  int ret = MPI_SUCCESS;
  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
  auto existing = mdm->Find(fh);
  if (!existing.second) {
    LOG(INFO) << "File not opened before by adapter" << std::endl;
    AdapterStat stat;
    stat.ref_count = 1;
    stat.a_mode = amode;
    MPI_File_get_size(*fh, &stat.size);
    if (amode & MPI_MODE_APPEND) {
      stat.ptr = stat.size;
    }
    stat.info = info;
    stat.comm = comm;
    hapi::Context ctx;
    stat.st_bkid = std::make_shared<hapi::Bucket>(path, mdm->GetHermes(), ctx);
    mdm->Create(fh, stat);
  } else {
    LOG(INFO) << "File opened before by adapter" << std::endl;
    existing.first.ref_count++;
    mdm->Update(fh, existing.first);
  }
  return ret;
}

int open_internal(MPI_Comm &comm, const char *path, int &amode, MPI_Info &info,
                  MPI_File *fh) {
  int ret;
  MAP_OR_FAIL(MPI_File_open);
  ret = real_MPI_File_open_(comm, path, amode, info, fh);
  if (ret == MPI_SUCCESS) {
    ret = simple_open(comm, path, amode, info, fh);
  }
  return ret;
}
size_t perform_file_read(const char *filename, size_t file_offset, void *ptr,
                         size_t ptr_offset, int count, MPI_Datatype datatype) {
  LOG(INFO) << "Read called for filename from destination: " << filename
            << " on offset: " << file_offset << " and count: " << count
            << std::endl;
  INTERCEPTOR_LIST->hermes_flush_exclusion.insert(filename);
  MPI_File fh;
  int status = MPI_File_open(MPI_COMM_SELF, filename, MPI_MODE_RDONLY,
                             MPI_INFO_NULL, &fh);
  int read_size = 0;
  if (status == MPI_SUCCESS) {
    status = MPI_File_seek(fh, file_offset, MPI_SEEK_SET);
    if (status == MPI_SUCCESS) {
      MPI_Status read_status;
      status = MPI_File_read(fh, (char *)ptr + ptr_offset, count, datatype,
                             &read_status);
      MPI_Get_count(&read_status, datatype, &read_size);
      if (read_size != count) {
        LOG(ERROR) << "reading failed: read " << read_size << " of " << count
                   << "." << std::endl;
      }
    }
    status = MPI_File_close(&fh);
  }
  INTERCEPTOR_LIST->hermes_flush_exclusion.erase(filename);
  return read_size;
}

size_t perform_file_write(std::string &filename, int offset, int count,
                          MPI_Datatype datatype, unsigned char *data_ptr) {
  LOG(INFO) << "Writing to file: " << filename << " offset: " << offset
            << " of count:" << count << " datatype:" << datatype << "."
            << std::endl;
  INTERCEPTOR_LIST->hermes_flush_exclusion.insert(filename);
  MPI_File fh;
  int status = MPI_File_open(MPI_COMM_SELF, filename.c_str(), MPI_MODE_RDWR,
                             MPI_INFO_NULL, &fh);
  int write_size = 0;
  if (fh != nullptr) {
    status = MPI_File_seek(fh, offset, MPI_SEEK_SET);
    if (status == 0) {
      MPI_Status write_status;
      status = MPI_File_write(fh, data_ptr, count, datatype, &write_status);
      MPI_Get_count(&write_status, datatype, &write_size);
      if (write_size != count) {
        LOG(ERROR) << "writing failed: wrote " << write_size << " of " << count
                   << "." << std::endl;
      }
      status = MPI_File_close(&fh);
    }
  }
  INTERCEPTOR_LIST->hermes_flush_exclusion.erase(filename);
  return write_size;
}

std::pair<int, size_t> write_internal(std::pair<AdapterStat, bool> &existing,
                                      const void *ptr, int count,
                                      MPI_Datatype datatype, MPI_File *fp,
                                      MPI_Status *mpi_status,
                                      bool is_collective = false) {
  (void)is_collective;
  LOG(INFO) << "Write called for filename: "
            << existing.first.st_bkid->GetName()
            << " on offset: " << existing.first.ptr << " and count: " << count
            << std::endl;
  size_t ret;
  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
  auto mapper = MapperFactory().Get(kMapperType);
  int datatype_size;
  MPI_Type_size(datatype, &datatype_size);
  size_t total_size = datatype_size * count;
  auto mapping = mapper->map(FileStruct(fp, existing.first.ptr, total_size));
  size_t data_offset = 0;
  auto filename = existing.first.st_bkid->GetName();
  LOG(INFO) << "Mapping for write has " << mapping.size() << " mapping."
            << std::endl;
  for (const auto &item : mapping) {
    hapi::Context ctx;
    auto index = std::stol(item.second.blob_name_) - 1;
    unsigned char *put_data_ptr = (unsigned char *)ptr + data_offset;
    size_t put_data_ptr_size = item.first.size_;

    if (item.second.size_ == kPageSize) {
      LOG(INFO) << "Create or Overwrite blob " << item.second.blob_name_
                << " of size:" << item.second.size_ << "." << std::endl;
      if (item.second.size_ == kPageSize) {
        auto status = existing.first.st_bkid->Put(
            item.second.blob_name_, put_data_ptr, put_data_ptr_size, ctx);
        if (status.Failed()) {
          perform_file_write(filename, item.first.offset_, put_data_ptr_size,
                             MPI_CHAR, put_data_ptr);
        } else {
          existing.first.st_blobs.emplace(item.second.blob_name_);
        }
      // TODO(chogan): The commented out branches are unreachable. Hari needs to
      // take a look at this
      }
#if 0
      } else if (item.second.offset_ == 0) {
        auto status = existing.first.st_bkid->Put(
            item.second.blob_name_, put_data_ptr, put_data_ptr_size, ctx);
        if (status.Failed()) {
          perform_file_write(filename, index * kPageSize, put_data_ptr_size,
                             MPI_CHAR, put_data_ptr);
        } else {
          existing.first.st_blobs.emplace(item.second.blob_name_);
        }
      } else {
        hapi::Blob final_data(item.second.offset_ + item.second.size_);
        if (fs::exists(filename) &&
            fs::file_size(filename) >= item.second.offset_) {
          LOG(INFO) << "Blob has a gap in write. read gap from original file."
                    << std::endl;
          perform_file_read(filename.c_str(), index * kPageSize,
                            final_data.data(), 0, item.second.offset_,
                            MPI_CHAR);
        }
        memcpy(final_data.data() + item.second.offset_, put_data_ptr,
               put_data_ptr_size);
        auto status = existing.first.st_bkid->Put(item.second.blob_name_,
                                                  final_data, ctx);
        if (status.Failed()) {
          perform_file_write(filename, index * kPageSize, final_data.size(),
                             MPI_CHAR, final_data.data());
        } else {
          existing.first.st_blobs.emplace(item.second.blob_name_);
        }
      }
#endif
      /* TODO(hari): Check if vbucket exists. if so delete it.*/
    } else {
      LOG(INFO) << "Writing blob " << item.second.blob_name_
                << " of size:" << item.second.size_ << "." << std::endl;
      auto blob_exists =
          existing.first.st_bkid->ContainsBlob(item.second.blob_name_);
      hapi::Blob temp(0);
      auto existing_blob_size =
          existing.first.st_bkid->Get(item.second.blob_name_, temp, ctx);
      if (blob_exists) {
        LOG(INFO) << "blob " << item.second.blob_name_
                  << " of size:" << existing_blob_size << "." << std::endl;
        if (existing_blob_size != kPageSize) {
          LOG(ERROR) << "blob " << item.second.blob_name_
                     << " has of size:" << existing_blob_size
                     << " of the overall page." << std::endl;
        }
        hapi::Blob existing_data(existing_blob_size);
        existing.first.st_bkid->Get(item.second.blob_name_, existing_data, ctx);
        memcpy(existing_data.data() + item.second.offset_, put_data_ptr,
               put_data_ptr_size);
        auto status = existing.first.st_bkid->Put(item.second.blob_name_,
                                                  existing_data, ctx);
        if (status.Failed()) {
          perform_file_write(filename, index * kPageSize, existing_blob_size,
                             MPI_CHAR, existing_data.data());
        } else {
          existing.first.st_blobs.emplace(item.second.blob_name_);
        }
      } else {
        std::string process_local_blob_name =
            mdm->EncodeBlobNameLocal(item.second);
        auto vbucket =
            hapi::VBucket(item.second.blob_name_, mdm->GetHermes(), false);
        existing.first.st_vbuckets.emplace(item.second.blob_name_);
        auto blob_names = vbucket.GetLinks(ctx);
        LOG(INFO) << "vbucket with blobname " << item.second.blob_name_
                  << " does not exists." << std::endl;
        auto status = existing.first.st_bkid->Put(
            process_local_blob_name, put_data_ptr, put_data_ptr_size, ctx);
        if (status.Failed()) {
          perform_file_write(filename, item.first.offset_, put_data_ptr_size,
                             MPI_CHAR, put_data_ptr);
        } else {
          existing.first.st_blobs.emplace(process_local_blob_name);
          vbucket.Link(process_local_blob_name,
                       existing.first.st_bkid->GetName());
        }
        if (!blob_names.empty()) {
          LOG(INFO) << "vbucket with blobname " << item.second.blob_name_
                    << " exists." << std::endl;
          for (auto &blob_name : blob_names) {
            auto hermes_struct = mdm->DecodeBlobNameLocal(blob_name);
            if (((hermes_struct.second.offset_ < item.second.offset_) &&
                 (hermes_struct.second.offset_ + hermes_struct.second.size_ >
                  item.second.offset_))) {
              // partially contained second half
              hapi::Blob existing_data(item.second.offset_ -
                                       hermes_struct.second.offset_);
              existing.first.st_bkid->Get(item.second.blob_name_, existing_data,
                                          ctx);
              status = existing.first.st_bkid->Put(item.second.blob_name_,
                                                   existing_data, ctx);
              if (status.Failed()) {
                LOG(ERROR) << "Put Failed on adapter." << std::endl;
              }
            } else if (item.second.offset_ < hermes_struct.second.offset_ &&
                       item.second.offset_ + item.second.size_ >
                           hermes_struct.second.offset_) {
              // partially contained first half
              hapi::Blob existing_data(hermes_struct.second.size_);
              existing.first.st_bkid->Get(item.second.blob_name_, existing_data,
                                          ctx);
              existing_data.erase(
                  existing_data.begin(),
                  existing_data.begin() +
                      (hermes_struct.second.offset_ - item.second.offset_));
              status = existing.first.st_bkid->Put(item.second.blob_name_,
                                                   existing_data, ctx);
              if (status.Failed()) {
                LOG(ERROR) << "Put Failed on adapter." << std::endl;
              }
            } else if (hermes_struct.second.offset_ > item.second.offset_ &&
                       hermes_struct.second.offset_ +
                               hermes_struct.second.size_ <
                           item.second.offset_ + item.second.size_) {
              // fully contained
              status =
                  existing.first.st_bkid->DeleteBlob(item.second.blob_name_);
              if (status.Failed()) {
                LOG(ERROR) << "Delete blob Failed on adapter." << std::endl;
              }
              existing.first.st_blobs.erase(item.second.blob_name_);
            } else {
              // no overlap
            }
          }
        }
        vbucket.Release();
      }
    }
    data_offset += item.first.size_;
  }
  existing.first.ptr += data_offset;
  existing.first.size = existing.first.size >= existing.first.ptr
                            ? existing.first.size
                            : existing.first.ptr;
  mdm->Update(fp, existing.first);
  ret = data_offset;
  mpi_status->count_hi_and_cancelled = 0;
  mpi_status->count_lo = ret;
  return std::pair<int, size_t>(MPI_SUCCESS, ret);
}

std::pair<int, size_t> read_internal(std::pair<AdapterStat, bool> &existing,
                                     void *ptr, int count,
                                     MPI_Datatype datatype, MPI_File *fp,
                                     MPI_Status *mpi_status,
                                     bool is_collective = false) {
  (void)is_collective;
  LOG(INFO) << "Read called for filename: " << existing.first.st_bkid->GetName()
            << " on offset: " << existing.first.ptr << " and size: " << count
            << std::endl;
  if (existing.first.ptr >= existing.first.size) {
    mpi_status->count_hi_and_cancelled = 0;
    mpi_status->count_lo = 0;
    return std::pair<int, size_t>(MPI_SUCCESS, 0);
  }
  int datatype_size;
  MPI_Type_size(datatype, &datatype_size);
  size_t total_size = datatype_size * count;
  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
  auto mapper = MapperFactory().Get(kMapperType);
  auto mapping = mapper->map(FileStruct(fp, existing.first.ptr, total_size));
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
        auto file_read_size = perform_file_read(
            filename.c_str(), item.first.offset_, ptr, total_read_size,
            (int)item.second.size_, MPI_CHAR);
        read_size += file_read_size;
      }
      if (contains_blob && fs::exists(filename) &&
          fs::file_size(filename) >= item.first.offset_ + item.first.size_) {
        LOG(INFO) << "Blob does not have data and need to read from original "
                     "filename: "
                  << filename << " offset:" << item.first.offset_ + read_size
                  << " size:" << item.second.size_ - read_size << "."
                  << std::endl;
        auto new_read_size =
            perform_file_read(filename.c_str(), item.first.offset_, ptr,
                              total_read_size + read_size,
                              item.second.size_ - read_size, MPI_CHAR);
        read_size += new_read_size;
      }
    } else {
      hapi::VBucket vbucket(item.second.blob_name_, mdm->GetHermes(), false);
      auto blob_names = vbucket.GetLinks(ctx);
      if (!blob_names.empty()) {
        LOG(INFO) << "vbucket with blobname " << item.second.blob_name_
                  << " exists." << std::endl;
        for (auto &blob_name : blob_names) {
          auto hermes_struct = mdm->DecodeBlobNameLocal(blob_name);
          if (((hermes_struct.second.offset_ <= item.second.offset_) &&
               (hermes_struct.second.offset_ + hermes_struct.second.size_ >=
                item.second.offset_) &&
               (hermes_struct.second.size_ - item.second.offset_ -
                    hermes_struct.second.offset_ >
                0))) {
            // partially contained second half
            hapi::Blob existing_data(hermes_struct.second.size_);
            existing.first.st_bkid->Get(hermes_struct.second.encoded_blob_name_,
                                        existing_data, ctx);
            auto offset_to_cp =
                item.second.offset_ - hermes_struct.second.offset_;
            memcpy((char *)ptr + (item.first.offset_ - existing.first.ptr),
                   existing_data.data() + offset_to_cp,
                   hermes_struct.second.size_ - offset_to_cp);
            read_size += hermes_struct.second.size_ - offset_to_cp;
          } else if (item.second.offset_ < hermes_struct.second.offset_ &&
                     item.second.offset_ + item.second.size_ >
                         hermes_struct.second.offset_) {
            // partially contained first half
            hapi::Blob existing_data(hermes_struct.second.size_);
            existing.first.st_bkid->Get(hermes_struct.second.encoded_blob_name_,
                                        existing_data, ctx);
            memcpy((char *)ptr + (item.first.offset_ - existing.first.ptr),
                   existing_data.data(),
                   (hermes_struct.second.offset_ - item.second.offset_));
            read_size += hermes_struct.second.offset_ - item.second.offset_;
          } else if (hermes_struct.second.offset_ > item.second.offset_ &&
                     hermes_struct.second.offset_ + hermes_struct.second.size_ <
                         item.second.offset_ + item.second.size_) {
            // fully contained
            hapi::Blob existing_data(hermes_struct.second.size_);
            existing.first.st_bkid->Get(hermes_struct.second.encoded_blob_name_,
                                        existing_data, ctx);
            memcpy((char *)ptr + (item.first.offset_ - existing.first.ptr),
                   existing_data.data(), hermes_struct.second.size_);
            read_size += hermes_struct.second.size_;
          } else {
            // no overlap
          }
          if (read_size == item.second.size_) break;
        }
      } else if (fs::exists(filename) &&
                 fs::file_size(filename) >=
                     item.first.offset_ + item.first.size_) {
        LOG(INFO) << "Blob does not exists and need to read from original "
                     "filename: "
                  << filename << " offset:" << item.first.offset_
                  << " size:" << item.first.size_ << "." << std::endl;
        read_size =
            perform_file_read(filename.c_str(), item.first.offset_, ptr,
                              total_read_size, item.first.size_, MPI_CHAR);
      }
      vbucket.Release();
    }
    if (read_size > 0) {
      total_read_size += read_size;
    }
  }
  existing.first.ptr += total_read_size;
  mdm->Update(fp, existing.first);
  mpi_status->count_hi_and_cancelled = 0;
  mpi_status->count_lo = total_read_size;
  return std::pair<int, size_t>(MPI_SUCCESS, total_read_size);
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
    mdm->InitializeHermes();
    auto pool =
        hermes::adapter::Singleton<ThreadPool>::GetInstance(kNumThreads);
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

int HERMES_DECL(MPI_Wait)(MPI_Request *req, MPI_Status *status) {
  int ret;
  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
  auto iter = mdm->request_map.find(req);
  if (iter != mdm->request_map.end()) {
    ret = iter->second->return_future.get();
    memcpy(status, &iter->second->status, sizeof(MPI_Status));
    auto h_req = iter->second;
    mdm->request_map.erase(iter);
    delete (h_req);
  } else {
    MAP_OR_FAIL(MPI_Wait);
    ret = real_MPI_Wait_(req, status);
  }
  return ret;
}

int HERMES_DECL(MPI_Waitall)(int count, MPI_Request *req, MPI_Status *status) {
  int ret;
  for (int i = 0; i < count; i++) {
    auto sub_ret = MPI_Wait(&req[i], &status[i]);
    if (sub_ret != MPI_SUCCESS) {
      ret = sub_ret;
    }
  }
  return ret;
}
/**
 * Metadata functions
 */
int HERMES_DECL(MPI_File_open)(MPI_Comm comm, const char *filename, int amode,
                               MPI_Info info, MPI_File *fh) {
  int status;
  if (hermes::adapter::IsTracked(filename)) {
    LOG(INFO) << "Intercept MPI_File_open for filename: " << filename
              << " and mode: " << amode << " is tracked." << std::endl;
    status = open_internal(comm, filename, amode, info, fh);
  } else {
    MAP_OR_FAIL(MPI_File_open);
    status = real_MPI_File_open_(comm, filename, amode, info, fh);
  }
  return (status);
}

int HERMES_DECL(MPI_File_close)(MPI_File *fh) {
  int ret;
  if (IsTracked(fh)) {
    LOG(INFO) << "Intercept MPI_File_close." << std::endl;
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(fh);
    if (existing.second) {
      MPI_Barrier(existing.first.comm);
      LOG(INFO) << "File handler is opened by adapter." << std::endl;
      hapi::Context ctx;
      if (existing.first.ref_count == 1) {
        auto filename = existing.first.st_bkid->GetName();
        auto persist = INTERCEPTOR_LIST->Persists(filename);
        mdm->Delete(fh);
        const auto &blob_names = existing.first.st_blobs;
        if (!blob_names.empty() && persist) {
          LOG(INFO) << "Adapter flushes " << blob_names.size()
                    << " blobs to filename:" << filename << "." << std::endl;
          INTERCEPTOR_LIST->hermes_flush_exclusion.insert(filename);
          hermes::api::VBucket file_vbucket(filename, mdm->GetHermes(), true,
                                            ctx);
          auto offset_map = std::unordered_map<std::string, hermes::u64>();

          for (auto blob_name : blob_names) {
            auto h_struct = mdm->DecodeBlobNameLocal(blob_name);
            auto status = file_vbucket.Link(blob_name, filename, ctx);
            if (!status.Failed()) {
              if (h_struct.first == -1) {
                auto page_index = std::stol(blob_name) - 1;
                offset_map.emplace(blob_name, page_index * kPageSize);
              } else {
                auto page_index = std::stol(h_struct.second.blob_name_) - 1;
                offset_map.emplace(blob_name, page_index * kPageSize +
                                                  h_struct.second.offset_);
              }
            }
          }
          auto trait = hermes::api::FileMappingTrait(filename, offset_map,
                                                     nullptr, NULL, NULL);
          file_vbucket.Attach(&trait, ctx);
          file_vbucket.Destroy(ctx);
          for (const auto &vbucket : existing.first.st_vbuckets) {
            hermes::api::VBucket blob_vbucket(vbucket, mdm->GetHermes(), false,
                                              ctx);
            auto blob_names_v = blob_vbucket.GetLinks(ctx);
            for (auto &blob_name : blob_names_v) {
              blob_vbucket.Unlink(blob_name, existing.first.st_bkid->GetName());
            }
            auto blob_names_temp = blob_vbucket.GetLinks(ctx);
            blob_vbucket.Destroy(ctx);
          }
          for (auto &blob_name : existing.first.st_blobs) {
            existing.first.st_bkid->DeleteBlob(blob_name);
          }
          existing.first.st_blobs.clear();
          INTERCEPTOR_LIST->hermes_flush_exclusion.erase(filename);
        }
        existing.first.st_bkid->Destroy(ctx);
        if (existing.first.a_mode & MPI_MODE_DELETE_ON_CLOSE) {
          fs::remove(filename);
        }

      } else {
        LOG(INFO) << "File handler is opened by more than one open."
                  << std::endl;
        existing.first.ref_count--;
        mdm->Update(fh, existing.first);
        existing.first.st_bkid->Release(ctx);
      }
      MPI_Barrier(existing.first.comm);
      ret = MPI_SUCCESS;
    } else {
      MAP_OR_FAIL(MPI_File_close);
      ret = real_MPI_File_close_(fh);
    }
  } else {
    MAP_OR_FAIL(MPI_File_close);
    ret = real_MPI_File_close_(fh);
  }
  return (ret);
}

int HERMES_DECL(MPI_File_seek_shared)(MPI_File fh, MPI_Offset offset,
                                      int whence) {
  int ret;
  if (IsTracked(&fh)) {
    LOG(INFO) << "Intercept MPI_File_seek_shared offset:" << offset
              << " whence:" << whence << "." << std::endl;
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(&fh);
    MPI_Offset sum_offset;
    int sum_whence;
    int comm_participators;
    MPI_Comm_size(existing.first.comm, &comm_participators);
    MPI_Allreduce(&offset, &sum_offset, 1, MPI_LONG_LONG_INT, MPI_SUM,
                  existing.first.comm);
    MPI_Allreduce(&whence, &sum_whence, 1, MPI_INT, MPI_SUM,
                  existing.first.comm);
    if (sum_offset / comm_participators != offset) {
      LOG(ERROR)
          << "Same offset should be passed across the opened file communicator."
          << std::endl;
    }
    if (sum_whence / comm_participators != whence) {
      LOG(ERROR)
          << "Same whence should be passed across the opened file communicator."
          << std::endl;
    }
    ret = MPI_File_seek(fh, offset, whence);
  } else {
    MAP_OR_FAIL(MPI_File_seek_shared);
    ret = real_MPI_File_seek_shared_(fh, offset, whence);
  }
  return (ret);
}

int HERMES_DECL(MPI_File_seek)(MPI_File fh, MPI_Offset offset, int whence) {
  int ret = -1;
  if (IsTracked(&fh)) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(&fh);
    if (existing.second) {
      LOG(INFO) << "Intercept fseek offset:" << offset << " whence:" << whence
                << "." << std::endl;
      if (!(existing.first.a_mode & MPI_MODE_APPEND)) {
        switch (whence) {
          case MPI_SEEK_SET: {
            existing.first.ptr = offset;
            break;
          }
          case MPI_SEEK_CUR: {
            existing.first.ptr += offset;
            break;
          }
          case MPI_SEEK_END: {
            existing.first.ptr = existing.first.size + offset;
            break;
          }
          default: {
            // TODO(hari): throw not implemented error.
          }
        }
        mdm->Update(&fh, existing.first);
        ret = 0;
      } else {
        LOG(INFO)
            << "File pointer not updating as file was opened in append mode."
            << std::endl;
        ret = -1;
      }
    } else {
      MAP_OR_FAIL(MPI_File_seek);
      ret = real_MPI_File_seek_(fh, offset, whence);
    }
  } else {
    MAP_OR_FAIL(MPI_File_seek);
    ret = real_MPI_File_seek_(fh, offset, whence);
  }
  return (ret);
}
int HERMES_DECL(MPI_File_get_position)(MPI_File fh, MPI_Offset *offset) {
  int ret = -1;
  if (IsTracked(&fh)) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(&fh);
    *offset = existing.first.ptr;
    ret = MPI_SUCCESS;
  } else {
    MAP_OR_FAIL(MPI_File_get_position);
    ret = real_MPI_File_get_position_(fh, offset);
  }
  return ret;
}
/**
 * Sync Read/Write
 */
int HERMES_DECL(MPI_File_read_all)(MPI_File fh, void *buf, int count,
                                   MPI_Datatype datatype, MPI_Status *status) {
  int ret;
  if (IsTracked(&fh)) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(&fh);
    if (existing.second) {
      MPI_Barrier(existing.first.comm);
      LOG(INFO) << "Intercept MPI_File_read_all." << std::endl;
      auto read_ret =
          read_internal(existing, buf, count, datatype, &fh, status, true);
      ret = read_ret.first;
      MPI_Barrier(existing.first.comm);
    } else {
      MAP_OR_FAIL(MPI_File_read_all);
      ret = real_MPI_File_read_all_(fh, buf, count, datatype, status);
    }
  } else {
    MAP_OR_FAIL(MPI_File_read_all);
    ret = real_MPI_File_read_all_(fh, buf, count, datatype, status);
  }
  return (ret);
}
int HERMES_DECL(MPI_File_read_at_all)(MPI_File fh, MPI_Offset offset, void *buf,
                                      int count, MPI_Datatype datatype,
                                      MPI_Status *status) {
  int ret;
  if (IsTracked(&fh)) {
    ret = MPI_File_seek(fh, offset, MPI_SEEK_SET);
    if (ret == MPI_SUCCESS) {
      ret = MPI_File_read_all(fh, buf, count, datatype, status);
    }
  } else {
    MAP_OR_FAIL(MPI_File_read_at_all);
    ret = real_MPI_File_read_at_all_(fh, offset, buf, count, datatype, status);
  }
  return ret;
}
int HERMES_DECL(MPI_File_read_at)(MPI_File fh, MPI_Offset offset, void *buf,
                                  int count, MPI_Datatype datatype,
                                  MPI_Status *status) {
  int ret;
  if (IsTracked(&fh)) {
    ret = MPI_File_seek(fh, offset, MPI_SEEK_SET);
    if (ret == MPI_SUCCESS) {
      ret = MPI_File_read(fh, buf, count, datatype, status);
    }
  } else {
    MAP_OR_FAIL(MPI_File_read_at);
    ret = real_MPI_File_read_at_(fh, offset, buf, count, datatype, status);
  }
  return ret;
}
int HERMES_DECL(MPI_File_read)(MPI_File fh, void *buf, int count,
                               MPI_Datatype datatype, MPI_Status *status) {
  int ret;
  if (IsTracked(&fh)) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(&fh);
    if (existing.second) {
      LOG(INFO) << "Intercept MPI_File_read." << std::endl;
      auto read_ret =
          read_internal(existing, buf, count, datatype, &fh, status);
      ret = read_ret.first;
    } else {
      MAP_OR_FAIL(MPI_File_read);
      ret = real_MPI_File_read_(fh, buf, count, datatype, status);
    }
  } else {
    MAP_OR_FAIL(MPI_File_read);
    ret = real_MPI_File_read_(fh, buf, count, datatype, status);
  }
  return (ret);
}
int HERMES_DECL(MPI_File_read_ordered)(MPI_File fh, void *buf, int count,
                                       MPI_Datatype datatype,
                                       MPI_Status *status) {
  int ret;
  if (IsTracked(&fh)) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(&fh);
    int total;
    MPI_Scan(&count, &total, 1, MPI_INT, MPI_SUM, existing.first.comm);
    MPI_Offset my_offset = total - count;
    ret = MPI_File_read_at_all(fh, my_offset, buf, count, datatype, status);
  } else {
    MAP_OR_FAIL(MPI_File_read_ordered);
    ret = real_MPI_File_read_ordered_(fh, buf, count, datatype, status);
  }
  return (ret);
}
int HERMES_DECL(MPI_File_read_shared)(MPI_File fh, void *buf, int count,
                                      MPI_Datatype datatype,
                                      MPI_Status *status) {
  int ret;
  if (IsTracked(&fh)) {
    ret = MPI_File_read(fh, buf, count, datatype, status);
  } else {
    MAP_OR_FAIL(MPI_File_read_shared);
    ret = real_MPI_File_read_shared_(fh, buf, count, datatype, status);
  }
  return ret;
}
int HERMES_DECL(MPI_File_write_all)(MPI_File fh, const void *buf, int count,
                                    MPI_Datatype datatype, MPI_Status *status) {
  int ret;
  if (IsTracked(&fh)) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(&fh);
    if (existing.second) {
      MPI_Barrier(existing.first.comm);
      LOG(INFO) << "Intercept MPI_File_write." << std::endl;
      auto write_ret =
          write_internal(existing, buf, count, datatype, &fh, status, true);
      ret = write_ret.first;
      MPI_Barrier(existing.first.comm);
    } else {
      MAP_OR_FAIL(MPI_File_write_all);
      ret = real_MPI_File_write_all_(fh, buf, count, datatype, status);
    }
  } else {
    MAP_OR_FAIL(MPI_File_write_all);
    ret = real_MPI_File_write_all_(fh, buf, count, datatype, status);
  }
  return (ret);
}
int HERMES_DECL(MPI_File_write_at_all)(MPI_File fh, MPI_Offset offset,
                                       const void *buf, int count,
                                       MPI_Datatype datatype,
                                       MPI_Status *status) {
  int ret;
  if (IsTracked(&fh)) {
    ret = MPI_File_seek(fh, offset, MPI_SEEK_SET);
    if (ret == MPI_SUCCESS) {
      ret = MPI_File_write_all(fh, buf, count, datatype, status);
    }
  } else {
    MAP_OR_FAIL(MPI_File_write_at_all);
    ret = real_MPI_File_write_at_all_(fh, offset, buf, count, datatype, status);
  }
  return ret;
}
int HERMES_DECL(MPI_File_write_at)(MPI_File fh, MPI_Offset offset,
                                   const void *buf, int count,
                                   MPI_Datatype datatype, MPI_Status *status) {
  int ret;
  if (IsTracked(&fh)) {
    ret = MPI_File_seek(fh, offset, MPI_SEEK_SET);
    if (ret == MPI_SUCCESS) {
      ret = MPI_File_write(fh, buf, count, datatype, status);
    }
  } else {
    MAP_OR_FAIL(MPI_File_write_at);
    ret = real_MPI_File_write_at_(fh, offset, buf, count, datatype, status);
  }
  return ret;
}
int HERMES_DECL(MPI_File_write)(MPI_File fh, const void *buf, int count,
                                MPI_Datatype datatype, MPI_Status *status) {
  int ret;
  if (IsTracked(&fh)) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(&fh);
    if (existing.second) {
      LOG(INFO) << "Intercept MPI_File_write." << std::endl;
      auto write_ret =
          write_internal(existing, buf, count, datatype, &fh, status, false);
      ret = write_ret.first;
    } else {
      MAP_OR_FAIL(MPI_File_write);
      ret = real_MPI_File_write_(fh, buf, count, datatype, status);
    }
  } else {
    MAP_OR_FAIL(MPI_File_write);
    ret = real_MPI_File_write_(fh, buf, count, datatype, status);
  }
  return (ret);
}
int HERMES_DECL(MPI_File_write_ordered)(MPI_File fh, const void *buf, int count,
                                        MPI_Datatype datatype,
                                        MPI_Status *status) {
  int ret;
  if (IsTracked(&fh)) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(&fh);
    int total;
    MPI_Scan(&count, &total, 1, MPI_INT, MPI_SUM, existing.first.comm);
    MPI_Offset my_offset = total - count;
    ret = MPI_File_write_at_all(fh, my_offset, buf, count, datatype, status);
  } else {
    MAP_OR_FAIL(MPI_File_write_ordered);
    ret = real_MPI_File_write_ordered_(fh, buf, count, datatype, status);
  }
  return (ret);
}
int HERMES_DECL(MPI_File_write_shared)(MPI_File fh, const void *buf, int count,
                                       MPI_Datatype datatype,
                                       MPI_Status *status) {
  int ret;
  if (IsTracked(&fh)) {
    ret = MPI_File_write_ordered(fh, buf, count, datatype, status);
  } else {
    MAP_OR_FAIL(MPI_File_write_shared);
    ret = real_MPI_File_write_shared_(fh, buf, count, datatype, status);
  }
  return ret;
}
/**
 * Async Read/Write
 */
int HERMES_DECL(MPI_File_iread_at)(MPI_File fh, MPI_Offset offset, void *buf,
                                   int count, MPI_Datatype datatype,
                                   MPI_Request *request) {
  int ret;
  if (IsTracked(&fh)) {
    auto pool =
        hermes::adapter::Singleton<ThreadPool>::GetInstance(kNumThreads);
    HermesRequest *req = new HermesRequest();
    auto func = std::bind(MPI_File_read_at, fh, offset, buf, count, datatype,
                          &req->status);
    req->return_future = pool->run(func);
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    mdm->request_map.emplace(request, req);
    ret = MPI_SUCCESS;
  } else {
    MAP_OR_FAIL(MPI_File_iread_at);
    ret = real_MPI_File_iread_at_(fh, offset, buf, count, datatype, request);
  }
  return ret;
}
int HERMES_DECL(MPI_File_iread)(MPI_File fh, void *buf, int count,
                                MPI_Datatype datatype, MPI_Request *request) {
  int ret;
  if (IsTracked(&fh)) {
    auto pool =
        hermes::adapter::Singleton<ThreadPool>::GetInstance(kNumThreads);
    HermesRequest *req = new HermesRequest();
    auto func =
        std::bind(MPI_File_read, fh, buf, count, datatype, &req->status);
    req->return_future = pool->run(func);
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    mdm->request_map.emplace(request, req);
    ret = MPI_SUCCESS;
  } else {
    MAP_OR_FAIL(MPI_File_iread);
    ret = real_MPI_File_iread_(fh, buf, count, datatype, request);
  }
  return ret;
}
int HERMES_DECL(MPI_File_iread_shared)(MPI_File fh, void *buf, int count,
                                       MPI_Datatype datatype,
                                       MPI_Request *request) {
  int ret;
  if (IsTracked(&fh)) {
    auto pool =
        hermes::adapter::Singleton<ThreadPool>::GetInstance(kNumThreads);
    HermesRequest *req = new HermesRequest();
    auto func =
        std::bind(MPI_File_read_shared, fh, buf, count, datatype, &req->status);
    req->return_future = pool->run(func);
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    mdm->request_map.emplace(request, req);
    ret = MPI_SUCCESS;
  } else {
    MAP_OR_FAIL(MPI_File_iread_shared);
    ret = real_MPI_File_iread_shared_(fh, buf, count, datatype, request);
  }
  return ret;
}
int HERMES_DECL(MPI_File_iwrite_at)(MPI_File fh, MPI_Offset offset,
                                    const void *buf, int count,
                                    MPI_Datatype datatype,
                                    MPI_Request *request) {
  int ret;
  if (IsTracked(&fh)) {
    auto pool =
        hermes::adapter::Singleton<ThreadPool>::GetInstance(kNumThreads);
    HermesRequest *req = new HermesRequest();
    auto func = std::bind(MPI_File_write_at, fh, offset, buf, count, datatype,
                          &req->status);
    req->return_future = pool->run(func);
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    mdm->request_map.emplace(request, req);
    ret = MPI_SUCCESS;
  } else {
    MAP_OR_FAIL(MPI_File_iwrite_at);
    ret = real_MPI_File_iwrite_at_(fh, offset, buf, count, datatype, request);
  }
  return ret;
}

int HERMES_DECL(MPI_File_iwrite)(MPI_File fh, const void *buf, int count,
                                 MPI_Datatype datatype, MPI_Request *request) {
  int ret;
  if (IsTracked(&fh)) {
    auto pool =
        hermes::adapter::Singleton<ThreadPool>::GetInstance(kNumThreads);
    HermesRequest *req = new HermesRequest();
    auto func =
        std::bind(MPI_File_write, fh, buf, count, datatype, &req->status);
    req->return_future = pool->run(func);
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    mdm->request_map.emplace(request, req);
    ret = MPI_SUCCESS;
  } else {
    MAP_OR_FAIL(MPI_File_iwrite);
    ret = real_MPI_File_iwrite_(fh, buf, count, datatype, request);
  }
  return ret;
}
int HERMES_DECL(MPI_File_iwrite_shared)(MPI_File fh, const void *buf, int count,
                                        MPI_Datatype datatype,
                                        MPI_Request *request) {
  int ret;
  if (IsTracked(&fh)) {
    auto pool =
        hermes::adapter::Singleton<ThreadPool>::GetInstance(kNumThreads);
    HermesRequest *req = new HermesRequest();
    auto func = std::bind(MPI_File_write_shared, fh, buf, count, datatype,
                          &req->status);
    req->return_future = pool->run(func);
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    mdm->request_map.emplace(request, req);
    ret = MPI_SUCCESS;
  } else {
    MAP_OR_FAIL(MPI_File_iwrite_shared);
    ret = real_MPI_File_iwrite_shared_(fh, buf, count, datatype, request);
  }
  return ret;
}

/**
 * Other functions
 */
int HERMES_DECL(MPI_File_sync)(MPI_File fh) {
  int ret = -1;
  if (IsTracked(&fh)) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(&fh);
    if (existing.second) {
      LOG(INFO) << "Intercept MPI_File_sync." << std::endl;
      auto filename = existing.first.st_bkid->GetName();
      auto persist = INTERCEPTOR_LIST->Persists(filename);
      mdm->Delete(&fh);
      hapi::Context ctx;
      const auto &blob_names = existing.first.st_blobs;
      if (!blob_names.empty() && persist) {
        LOG(INFO) << "Adapter flushes " << blob_names.size()
                  << " blobs to filename:" << filename << "." << std::endl;
        INTERCEPTOR_LIST->hermes_flush_exclusion.insert(filename);
        hermes::api::VBucket file_vbucket(filename, mdm->GetHermes(), true);
        auto offset_map = std::unordered_map<std::string, hermes::u64>();

        for (auto blob_name : blob_names) {
          auto h_struct = mdm->DecodeBlobNameLocal(blob_name);
          auto status = file_vbucket.Link(blob_name, filename, ctx);
          if (!status.Failed()) {
            if (h_struct.first == -1) {
              auto page_index = std::stol(blob_name) - 1;
              offset_map.emplace(blob_name, page_index * kPageSize);
            } else {
              auto page_index = std::stol(h_struct.second.blob_name_) - 1;
              offset_map.emplace(
                  blob_name, page_index * kPageSize + h_struct.second.offset_);
            }
          }
        }
        auto trait = hermes::api::FileMappingTrait(filename, offset_map,
                                                   nullptr, NULL, NULL);
        file_vbucket.Attach(&trait, ctx);
        file_vbucket.Destroy(ctx);
        for (const auto &vbucket : existing.first.st_vbuckets) {
          hermes::api::VBucket blob_vbucket(vbucket, mdm->GetHermes(), false,
                                            ctx);
          auto blob_names_v = blob_vbucket.GetLinks(ctx);
          for (auto &blob_name : blob_names_v) {
            blob_vbucket.Unlink(blob_name, existing.first.st_bkid->GetName());
          }
          auto blob_names_temp = blob_vbucket.GetLinks(ctx);
          blob_vbucket.Destroy(ctx);
        }
        for (auto &blob_name : existing.first.st_blobs) {
          existing.first.st_bkid->DeleteBlob(blob_name);
        }
        existing.first.st_blobs.clear();
        INTERCEPTOR_LIST->hermes_flush_exclusion.erase(filename);
      }
      ret = 0;
    }
  } else {
    MAP_OR_FAIL(MPI_File_sync);
    ret = real_MPI_File_sync_(fh);
  }
  return (ret);
}
