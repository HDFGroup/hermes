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

const int kNumPools = 2;
const int kNumXstreams = 2;

int main(int argc, char *argv[]) {
  (void)argc;
  (void)argv;

  ABT_xstream xstreams[kNumXstreams];
  ABT_sched scheds[kNumXstreams];
  ABT_pool shared_pools[kNumPools];

  ABT_init(argc, argv);

  // Create 2 shared pools (high and low priority)
  for (int i = 0; i < kNumPools; ++i) {
    // TODO(chogan): Should automatic free be ABT_TRUE?
    ABT_pool_create_basic(ABT_POOL_FIFO, ABT_POOL_ACCESS_MPMC, ABT_TRUE,
                          &shared_pools[i]);
  }

  // Create schedulers
  for (int i = 0; i < kNumXstreams; i++) {
    ABT_sched_create_basic(ABT_SCHED_DEFAULT, kNumPools, shared_pools,
                           ABT_SCHED_CONFIG_NULL, &scheds[i]);
  }

  // Create xstreams
  ABT_xstream_self(&xstreams[0]);
  ABT_xstream_set_main_sched(xstreams[0], scheds[0]);
  for (int i = 1; i < kNumXstreams; i++) {
    ABT_xstream_create(scheds[i], &xstreams[i]);
  }

  return 0;
}
