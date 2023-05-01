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
#include <mpi.h>

#include "hermes_types.h"
#include "posix/posix_io_client.h"

namespace hermes {

enum class DataStagerType {
  kPosix,
  kHdf5
};

class DataStagerTypeConv {
 public:
  static DataStagerType from_url(const std::string &url) {
    if (url.rfind("h5::", 0) != std::string::npos) {
      return DataStagerType::kHdf5;
    } else {
      return DataStagerType::kPosix;
    }
  }
};

class DataStager {
 public:
  virtual void StageIn(std::string url, hapi::PlacementPolicy dpe) = 0;
  virtual void StageIn(std::string url, off_t off, size_t size,
                       hapi::PlacementPolicy dpe) = 0;
  virtual void StageOut(std::string url) = 0;

 protected:
  void DivideRange(off_t off, size_t size, off_t &new_off, size_t &new_size) {
    int nprocs = 1;
    int rank = 0;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    int ranks_for_io = nprocs;

    // Ensure that all ranks perform at least 32MB of I/O
    size_t min_io_per_rank = MEGABYTES(32);
    if (size < nprocs * min_io_per_rank) {
      ranks_for_io = size / min_io_per_rank;
      if (size % min_io_per_rank) {
        ranks_for_io += 1;
      }
    }

    new_size = size / ranks_for_io;
    new_off = off + new_size * rank;
    if (rank == ranks_for_io - 1) {
      new_size += size % ranks_for_io;
    }
  }
};

}  // namespace hermes

#endif  // HERMES_DATA_STAGER_STAGE_IN_H_
