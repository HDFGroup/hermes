//
// Created by llogan on 7/1/23.
//

#ifndef LABSTOR_INCLUDE_LABSTOR_QUEUE_MANAGER_QUEUE_FACTORY_H_
#define LABSTOR_INCLUDE_LABSTOR_QUEUE_MANAGER_QUEUE_FACTORY_H_

#include "queues/hshm_queue.h"

namespace labstor {

#ifdef QUEUE_TYPE
using MultiQueue = MultiQueueT<QUEUE_TYPE>;
#else
using MultiQueue = MultiQueueT<Hshm>;
#endif
}  // namespace labstor

#endif  // LABSTOR_INCLUDE_LABSTOR_QUEUE_MANAGER_QUEUE_FACTORY_H_
