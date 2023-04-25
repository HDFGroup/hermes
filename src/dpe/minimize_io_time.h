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

#ifndef HERMES_SRC_DPE_MINIMIZE_IO_TIME_H_
#define HERMES_SRC_DPE_MINIMIZE_IO_TIME_H_

#include "data_placement_engine.h"

namespace hermes {
/**
 A class to represent data placement engine that minimizes I/O time.
*/
class MinimizeIoTime : public DPE {
 public:
  MinimizeIoTime() = default;
  ~MinimizeIoTime() = default;
  Status Placement(const std::vector<size_t> &blob_sizes,
                   std::vector<TargetInfo> &targets,
                   api::Context &ctx,
                   std::vector<PlacementSchema> &output);
};

}  // namespace hermes

#endif  // HERMES_SRC_DPE_MINIMIZE_IO_TIME_H_
