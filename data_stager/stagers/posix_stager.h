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

#ifndef HERMES_DATA_STAGER_STAGERS_POSIX_STAGE_H
#define HERMES_DATA_STAGER_STAGERS_POSIX_STAGE_H

#include "../data_stager.h"
#include "posix/fs_api.h"

namespace hermes {

class PosixStager : public DataStager {
 public:
  void StageIn(std::string url, PlacementPolicy dpe) override;
  void FileStageIn(std::string path, PlacementPolicy dpe);
  void DirectoryStageIn(std::string path, PlacementPolicy dpe);

  void StageIn(std::string url, off_t off, size_t size,
               PlacementPolicy dpe) override;
  void FileStageIn(std::string path, off_t off, size_t size,
                   PlacementPolicy dpe);

  void StageOut(std::string url) override;
  void FileStageOut(std::string path);
  void DirectoryStageOut(std::string path);
};

}  // namespace hermes

#endif  // HERMES_DATA_STAGER_STAGERS_POSIX_STAGE_H
