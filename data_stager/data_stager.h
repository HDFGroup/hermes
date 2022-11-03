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

#ifndef HERMES_DATA_STAGER_STAGE_IN_H_
#define HERMES_DATA_STAGER_STAGE_IN_H_

#include <hermes.h>

#include "hermes_types.h"
#include "posix/fs_api.h"

namespace hermes {

enum class DataStagerType { kUnix, kHdf5 };

class DataStagerTypeConv {
 public:
  static DataStagerType from_url(const std::string &url) {
    if (url.rfind("h5::", 0) != std::string::npos) {
      return DataStagerType::kHdf5;
    } else {
      return DataStagerType::kUnix;
    }
  }
};

class DataStager {
 public:
  virtual void StageIn(std::string url, PlacementPolicy dpe) = 0;
  virtual void StageIn(std::string url, off_t off, size_t size,
                       PlacementPolicy dpe) = 0;
  virtual void StageOut(std::string url) = 0;

 protected:
  void DivideRange(off_t off, size_t size, off_t &new_off, size_t &new_size) {
    auto mdm = Singleton<hermes::adapter::fs::MetadataManager>::GetInstance();
    int concurrency = 1;
    int rank = 0;
    if (mdm->is_mpi) {
      concurrency = mdm->comm_size;
      rank = mdm->rank;
    }
    new_size = size / concurrency;
    new_off = off + new_size * rank;
    if (rank == concurrency - 1) {
      new_size += size % concurrency;
    }
  }
};

}  // namespace hermes

#endif  // HERMES_DATA_STAGER_STAGE_IN_H_
