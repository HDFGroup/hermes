//
// Created by lukemartinlogan on 12/21/22.
//

#ifndef HERMES_SRC_STATUSES_H_
#define HERMES_SRC_STATUSES_H_

#include "status.h"

namespace hermes {

using api::Status;

const Status DPE_PLACEMENT_SCHEMA_EMPTY("Placement failed. Non-fatal.");
const Status DPE_NO_SPACE("DPE has no remaining space.");
const Status DPE_MIN_IO_TIME_NO_SOLUTION(
    "DPE could not find solution for the minimize I/O time DPE");
}

#endif  // HERMES_SRC_STATUSES_H_
