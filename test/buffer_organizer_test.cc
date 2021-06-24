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


void enqueue(void *task) {
  hermes::BoTask *bo_task = (hermes::BoTask *)task;

  switch (bo_task->op) {
    case hermes::BoOperation::kMove: {
      hermes::BoMove(&bo_task->args.move_args);
      break;
    }
    case hermes::BoOperation::kCopy: {
      hermes::BoCopy(&bo_task->args.copy_args);
      break;
    }
    case hermes::BoOperation::kDelete: {
      hermes::BoDelete(&bo_task->args.delete_args);
      break;
    }
    default: {
      HERMES_INVALID_CODE_PATH;
    }
  }
}

void TestIsBoFunction() {
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
  TestIsBoFunction();

  hermes::BufferOrganizer bo;

  ABT_init(argc, argv);

  // Create 2 shared pools (high and low priority)
  for (int i = 0; i < hermes::kNumPools; ++i) {
    // TODO(chogan): Should automatic free be ABT_TRUE?
    ABT_pool_create_basic(ABT_POOL_FIFO, ABT_POOL_ACCESS_MPMC, ABT_TRUE,
                          &bo.pools[i]);
  }

  // Create schedulers
  for (int i = 0; i < hermes::kNumXstreams; i++) {
    ABT_sched_create_basic(ABT_SCHED_DEFAULT, hermes::kNumPools, bo.pools,
                           ABT_SCHED_CONFIG_NULL, &bo.scheds[i]);
  }

  // Create xstreams
  ABT_xstream_self(&bo.xstreams[0]);
  ABT_xstream_set_main_sched(bo.xstreams[0], bo.scheds[0]);
  for (int i = 1; i < hermes::kNumXstreams; i++) {
    ABT_xstream_create(bo.scheds[i], &bo.xstreams[i]);
  }

  // TODO(chogan): Task memory?
  hermes::BoTask task = {};

  // Create ULTs
  for (int i = 0; i < hermes::kNumXstreams; i++) {
    task.op = hermes::BoOperation::kMove;
    task.args.move_args = {};
    ABT_thread_create(bo.pools[i], enqueue, (void *)&task,
                      ABT_THREAD_ATTR_NULL, &bo.threads[i]);
  }

  // Join & Free
  for (int i = 0; i < hermes::kNumXstreams; i++) {
    ABT_thread_join(bo.threads[i]);
    ABT_thread_free(&bo.threads[i]);
  }
  for (int i = 1; i < hermes::kNumXstreams; i++) {
    ABT_xstream_join(bo.xstreams[i]);
    ABT_xstream_free(&bo.xstreams[i]);
  }

  // Free schedulers
  // Note that we do not need to free the scheduler for the primary ES,
  // i.e., xstreams[0], because its scheduler will be automatically freed in
  // ABT_finalize(). */
  for (int i = 1; i < hermes::kNumXstreams; i++) {
    ABT_sched_free(&bo.scheds[i]);
  }

  ABT_finalize();

  return 0;
}
