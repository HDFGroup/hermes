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
 private:
  std::vector<double> placement_ratios_; /**< a vector of placement ratios */

 public:
  MinimizeIoTime() = default;
  ~MinimizeIoTime() = default;
  Status Placement(const std::vector<size_t> &blob_sizes,
                   const hipc::vector<TargetInfo> &targets,
                   const api::Context &ctx,
                   std::vector<PlacementSchema> &output);

 private:
  /** get the absolute difference value from \a x size and \a y size */
  size_t AbsDiff(size_t x, size_t y, bool &y_gt_x);
  /** place bytes */
  void PlaceBytes(size_t j, ssize_t bytes, std::vector<size_t> &vars_bytes,
                  const hipc::vector<TargetInfo> &targets);
  /** get placement ratios from node states in \a ctx  context */
  void GetPlacementRatios(const hipc::vector<TargetInfo> &targets,
                          const api::Context &ctx);
};

}  // namespace hermes

#endif  // HERMES_SRC_DPE_MINIMIZE_IO_TIME_H_
