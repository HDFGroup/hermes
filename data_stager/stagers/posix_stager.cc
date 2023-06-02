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


using hermes::api::PlacementPolicyConv;
using hermes::api::PlacementPolicy;
using hermes::adapter::fs::AdapterStat;
using hermes::adapter::fs::IoStatus;
using hermes::adapter::fs::File;
using hermes::adapter::fs::FsIoOptions;
namespace stdfs = std::filesystem;

namespace hermes {

void PosixStager::StageIn(std::string path, PlacementPolicy dpe) {
  if (stdfs::is_directory(path)) {
    DirectoryStageIn(path, dpe);
  } else if (stdfs::is_regular_file(path)) {
    FileStageIn(path, dpe);
  } else {
    HELOG(kError, "{} is neither directory or file", path);
  }
}

void PosixStager::FileStageIn(std::string path, PlacementPolicy dpe) {
  off_t per_proc_off;
  size_t per_proc_size;
  DivideRange(0, stdfs::file_size(path), per_proc_off, per_proc_size);
  FileStageIn(path, per_proc_off, per_proc_size, dpe);
}

void PosixStager::DirectoryStageIn(std::string path, PlacementPolicy dpe) {
  HILOG(kInfo, "Staging in the directory {}", path)
  for (auto &file_path : stdfs::directory_iterator(path)) {
    FileStageIn(file_path.path(), dpe);
  }
}

void PosixStager::StageIn(std::string path, off_t off,
                         size_t size, PlacementPolicy dpe) {
  if (stdfs::is_directory(path)) {
    HELOG(kError, "Posix stage-in with offset is "
                  "not supported for directories")
  } else if (stdfs::is_regular_file(path)) {
    FileStageIn(path, off, size, dpe);
  } else {
    HELOG(kError, "{} is neither directory or file", path);
  }
}

void PosixStager::FileStageIn(std::string path,
                             off_t off, size_t size, PlacementPolicy dpe) {
  auto fs_api = HERMES_POSIX_FS;
  std::vector<char> buf(size);
  AdapterStat stat;
  bool stat_exists;
  IoStatus io_status;
  HILOG(kInfo, "Staging in {}", path)
  File f = fs_api->Open(stat, path);
  fs_api->Read(f, stat, buf.data(), off, size,
              io_status, FsIoOptions::WithDpe(dpe));
  fs_api->Close(f, stat_exists);
}

void PosixStager::StageOut(std::string path) {
  if (stdfs::is_regular_file(path)) {
    FileStageOut(path);
  } else if (stdfs::is_directory(path)) {
    DirectoryStageOut(path);
  } else {
    HILOG(kDebug, "Posix stage-out is neither a file or directory")
  }
}

void PosixStager::FileStageOut(std::string path) {
  HILOG(kInfo, "Staging out the file {}", path)
  auto fs_api = HERMES_POSIX_FS;
  AdapterStat stat;
  bool stat_exists;
  File f = fs_api->Open(stat, path);
  if (!f.status_) {
    HELOG(kError, "Couldn't open file: {}", path)
    return;
  }
  fs_api->Close(f, stat_exists);
}

void PosixStager::DirectoryStageOut(std::string path) {
  HILOG(kInfo, "Staging out the directory {}", path)
  for (auto &file_path : stdfs::directory_iterator(path)) {
    FileStageOut(file_path.path());
  }
}

}  // namespace hermes
