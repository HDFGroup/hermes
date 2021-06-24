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

void enqueue(void *task) {
  hermes::BoTask *bo_task = (hermes::BoTask *)task;

  switch (bo_task->op) {
    case hermes::BoOperation::kMove: {
      hermes::BoMove(&bo_task->args);
      break;
    }
    case hermes::BoOperation::kCopy: {
      hermes::BoCopy(&bo_task->args);
      break;
    }
    case hermes::BoOperation::kDelete: {
      hermes::BoDelete(&bo_task->args);
      break;
    }
    default: {
      HERMES_INVALID_CODE_PATH;
    }
  }
}

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

int main(int argc, char *argv[]) {
  (void)argc;
  (void)argv;
  TestIsBoFunction();

  // TODO(chogan): Pass BO threads from config
  hermes::BufferOrganizer bo;

  // TODO(chogan): Need 2 shared pools (high and low priority)

  int num_threads = 3;
  std::vector<std::future<void>> results;
  std::vector<hermes::BoTask *> tasks(num_threads);
  for (int i = 0; i < num_threads; ++i) {
    // TODO(chogan): Task memory
    tasks[i] = new hermes::BoTask();
    if (i == 0) {
      tasks[i]->op = hermes::BoOperation::kMove;
      hermes::BufferID bid = {};
      hermes::TargetID tid = {};
      bid.as_int = 1;
      tid.as_int = 1;
      tasks[i]->args.move_args = {bid, tid};
    } else if (i == 1) {
      tasks[i]->op = hermes::BoOperation::kCopy;
      hermes::BufferID bid = {};
      hermes::TargetID tid = {};
      bid.as_int = 1;
      tid.as_int = 1;
      tasks[i]->args.copy_args = {bid, tid};
    } else {
      tasks[i]->op = hermes::BoOperation::kDelete;
      hermes::BufferID bid = {};
      bid.as_int = 1;
      tasks[i]->args.delete_args = {bid};
    }
    results.emplace_back(bo.pool.run(std::bind(enqueue, tasks[i])));
  }

  for (int i = 0; i < num_threads; ++i) {
    results[i].get();
    delete tasks[i];
  }

  return 0;
}
