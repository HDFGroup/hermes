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

class StatusTable {
 public:
  std::vector<Status> table_;

 public:
  StatusTable() {
    table_.reserve(500);
    Define(0, "The function was not implemented");
    Define(100, "Placement failed. Non-fatal.");
    Define(102, "DPE could not find solution for the minimize I/O time DPE");
    Define(200, "Could not allocate the ram tier of storage in BPM");
    Define(300, "The read exceeds the size of the backend's data (PartialGet)");
    Define(301, "The read exceeds the size of the backend's data (PartialPut)");
  }

  void Define(int code, const char *msg) {
    table_.emplace_back(code, msg);
  }

  Status Get(int code) {
    return table_[code];
  }
};
#define HERMES_STATUS \
  hshm::EasySingleton<hermes::StatusTable>::GetInstance()
#define HERMES_STATUS_T hermes::StatusTable*

const Status NOT_IMPLEMENTED = HERMES_STATUS->Get(0);
const Status DPE_PLACEMENT_SCHEMA_EMPTY = HERMES_STATUS->Get(100);
const Status DPE_NO_SPACE = HERMES_STATUS->Get(101);
const Status DPE_MIN_IO_TIME_NO_SOLUTION = HERMES_STATUS->Get(102);
const Status BUFFER_POOL_OUT_OF_RAM = HERMES_STATUS->Get(200);
const Status PARTIAL_GET_OR_CREATE_OVERFLOW = HERMES_STATUS->Get(300);
const Status PARTIAL_PUT_OR_CREATE_OVERFLOW = HERMES_STATUS->Get(301);

}  // namespace hermes

#endif  // HERMES_SRC_STATUSES_H_
