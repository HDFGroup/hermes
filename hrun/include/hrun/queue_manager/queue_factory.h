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

#ifndef HRUN_INCLUDE_HRUN_QUEUE_MANAGER_QUEUE_FACTORY_H_
#define HRUN_INCLUDE_HRUN_QUEUE_MANAGER_QUEUE_FACTORY_H_

#include "queues/hshm_queue.h"

namespace hrun {

#ifdef QUEUE_TYPE
using MultiQueue = MultiQueueT<QUEUE_TYPE>;
#else
using MultiQueue = MultiQueueT<Hshm>;
#endif
}  // namespace hrun

#endif  // HRUN_INCLUDE_HRUN_QUEUE_MANAGER_QUEUE_FACTORY_H_
