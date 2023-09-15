//
// Created by lukemartinlogan on 7/13/23.
//

#ifndef LABSTOR_TASKS_HERMES_INCLUDE_STATUSES_H_
#define LABSTOR_TASKS_HERMES_INCLUDE_STATUSES_H_

#include "status.h"

#define STATUS_T static inline const Status

namespace hermes {

STATUS_T NOT_IMPLEMENTED(0, "Function was not implemented");
STATUS_T DPE_PLACEMENT_SCHEMA_EMPTY(1, "DPE placement schema is empty");
STATUS_T DPE_NO_SPACE(1, "Placement failed. Non-fatal.");
STATUS_T DPE_MIN_IO_TIME_NO_SOLUTION(1, "DPE could not find solution for the minimize I/O time DPE");

}  // namespace hermes

#endif //LABSTOR_TASKS_HERMES_INCLUDE_STATUSES_H_
