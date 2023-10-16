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

#ifndef HRUN_TASKS_HERMES_INCLUDE_STATUSES_H_
#define HRUN_TASKS_HERMES_INCLUDE_STATUSES_H_

#include "status.h"

#define STATUS_T static inline const Status

namespace hermes {

STATUS_T NOT_IMPLEMENTED(0, "Function was not implemented");
STATUS_T DPE_PLACEMENT_SCHEMA_EMPTY(1, "DPE placement schema is empty");
STATUS_T DPE_NO_SPACE(1, "Placement failed. Non-fatal.");
STATUS_T DPE_MIN_IO_TIME_NO_SOLUTION(
    1, "DPE could not find solution for the minimize I/O time DPE");

}  // namespace hermes

#endif  // HRUN_TASKS_HERMES_INCLUDE_STATUSES_H_
