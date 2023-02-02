//
// Created by lukemartinlogan on 12/21/22.
//

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

}

#endif  // HERMES_SRC_STATUSES_H_
