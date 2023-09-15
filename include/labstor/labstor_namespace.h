//
// Created by lukemartinlogan on 8/14/23.
//

#ifndef LABSTOR_INCLUDE_LABSTOR_LABSTOR_NAMESPACE_H_
#define LABSTOR_INCLUDE_LABSTOR_LABSTOR_NAMESPACE_H_

#include "labstor/api/labstor_client.h"
#include "labstor/task_registry/task_lib.h"

using labstor::TaskMethod;
using labstor::BinaryOutputArchive;
using labstor::BinaryInputArchive;
using labstor::Task;
using labstor::TaskPointer;
using labstor::MultiQueue;
using labstor::PriorityInfo;
using labstor::Task;
using labstor::TaskFlags;
using labstor::DataTransfer;
using labstor::TaskLib;
using labstor::TaskLibClient;
using labstor::config::QueueManagerInfo;
using labstor::TaskPrio;

using hshm::RwLock;
using hshm::Mutex;
using hshm::bitfield;
using hshm::bitfield32_t;
typedef hshm::bitfield<uint64_t> bitfield64_t;
using hshm::ScopedRwReadLock;
using hshm::ScopedRwWriteLock;
using hipc::LPointer;

#endif  // LABSTOR_INCLUDE_LABSTOR_LABSTOR_NAMESPACE_H_
