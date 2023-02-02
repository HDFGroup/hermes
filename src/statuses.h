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

#ifndef HERMES_SRC_STATUSES_H_
#define HERMES_SRC_STATUSES_H_

#include "status.h"

namespace hermes {

using api::Status;

const Status NOT_IMPLEMENTED("The function was not implemented");
const Status DPE_PLACEMENT_SCHEMA_EMPTY("Placement failed. Non-fatal.");
const Status DPE_NO_SPACE("DPE has no remaining space.");
const Status DPE_MIN_IO_TIME_NO_SOLUTION(
    "DPE could not find solution for the minimize I/O time DPE");
const Status BUFFER_POOL_OUT_OF_RAM(
    "Could not allocate the ram tier of storage in BPM");
const Status PARTIAL_GET_OR_CREATE_OVERFLOW(
    "The read exceeds the size of the backend's data");

}  // namespace hermes

#endif  // HERMES_SRC_STATUSES_H_
