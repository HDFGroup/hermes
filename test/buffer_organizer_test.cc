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

#include "abt.h"

#include "hermes.h"
#include "test_utils.h"

void TestIsBoFunction() {
  using hermes::IsBoFunction;
  Assert(IsBoFunction("BO::TriggerBufferOrganizer"));
  Assert(IsBoFunction("BO::A"));
  Assert(IsBoFunction("BO::"));
  Assert(!IsBoFunction(""));
  Assert(!IsBoFunction("A"));
  Assert(!IsBoFunction("BO:"));
  Assert(!IsBoFunction("BO:A"));
  Assert(!IsBoFunction("TriggerBufferOrganizer"));
}

void TestBoTasks() {
  using hermes::BoTask;
  using hermes::BufferID;
  using hermes::TargetID;

  // TODO(chogan): Pass BO threads from config
  // TODO(chogan): Pool needs to be accessible from context
  hermes::ThreadPool pool(2);

  const int kNumThreads = 3;
  for (int i = 0; i < kNumThreads; ++i) {
    BoTask task = {};
    BufferID bid = {};
    TargetID tid = {};
    bid.as_int = i;
    tid.as_int = i;
    if (i == 0) {
      task.op = hermes::BoOperation::kMove;
      task.args.move_args = {bid, tid};
    } else if (i == 1) {
      task.op = hermes::BoOperation::kCopy;
      task.args.copy_args = {bid, tid};
    } else {
      task.op = hermes::BoOperation::kDelete;
      task.args.delete_args = {bid};
    }
    LocalEnqueueBoTask(NULL, pool, task);
  }
}

int main(int argc, char *argv[]) {
  (void)argc;
  (void)argv;
  TestIsBoFunction();
  TestBoTasks();

  return 0;
}
