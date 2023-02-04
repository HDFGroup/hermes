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

#include "posix_stager.h"

using hermes::adapter::fs::PosixFS;
using hermes::api::PlacementPolicyConv;
using hermes::api::PlacementPolicy;
using hermes::adapter::fs::FsIoOptions;
namespace stdfs = std::filesystem;

namespace hermes {

void PosixStager::StageIn(std::string path, PlacementPolicy dpe) {
  if (stdfs::is_regular_file(path)) {
    FileStageIn(path, dpe);
  } else if (stdfs::is_directory(path)) {
    DirectoryStageIn(path, dpe);
  } else {
    LOG(ERROR) << "Posix stage in is neither a file or directory" << std::endl;
  }
}

void PosixStager::FileStageIn(std::string path, PlacementPolicy dpe) {
  off_t per_proc_off;
  size_t per_proc_size;
  DivideRange(0, stdfs::file_size(path), per_proc_off, per_proc_size);
  FileStageIn(path, per_proc_off, per_proc_size, dpe);
}

void PosixStager::DirectoryStageIn(std::string path, PlacementPolicy dpe) {
  for (auto &file_path : stdfs::directory_iterator(path)) {
    FileStageIn(file_path.path(), dpe);
  }
}

void PosixStager::StageIn(std::string path, off_t off,
                         size_t size, PlacementPolicy dpe) {
  if (stdfs::is_regular_file(path)) {
    FileStageIn(path, off, size, dpe);
  } else if (stdfs::is_directory(path)) {
    LOG(ERROR) << "Posix stage-in with offset " <<
        "is not supported for directories" << std::endl;
  } else {
    LOG(ERROR) << "Posix stage-in is neither a file or directory" << std::endl;
  }
}

void PosixStager::FileStageIn(std::string path,
                             off_t off, size_t size, PlacementPolicy dpe) {
  auto fs_api = PosixFS();
  std::vector<char> buf(size);
  AdapterStat stat;
  bool stat_exists;
  IoStatus io_status;
  File f = fs_api.Open(stat, path);
  fs_api.Read(f, stat_exists, buf.data(), off, size,
              io_status, FsIoOptions::WithParallelDpe(dpe));
  fs_api.Close(f, stat_exists, false);
}

void PosixStager::StageOut(std::string path) {
  if (stdfs::is_regular_file(path)) {
    FileStageOut(path);
  } else if (stdfs::is_directory(path)) {
    DirectoryStageOut(path);
  } else {
    LOG(ERROR) << "Posix stage-out is neither a file or directory" << std::endl;
  }
}

void PosixStager::FileStageOut(std::string path) {
  auto fs_api = PosixFS();
  AdapterStat stat;
  bool stat_exists;
  File f = fs_api.Open(stat, path);
  if (!f.status_) {
    LOG(INFO) << "Couldn't open file: " << path << std::endl;
    return;
  }
  fs_api.Close(f, stat_exists, true);
}

void PosixStager::DirectoryStageOut(std::string path) {
  for (auto &file_path : stdfs::directory_iterator(path)) {
    FileStageOut(file_path.path());
  }
}

}  // namespace hermes
