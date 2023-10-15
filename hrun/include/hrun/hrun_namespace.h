//
// Created by lukemartinlogan on 8/14/23.
//

#ifndef HRUN_INCLUDE_HRUN_HRUN_NAMESPACE_H_
#define HRUN_INCLUDE_HRUN_HRUN_NAMESPACE_H_

#include "hrun/api/hrun_client.h"
#include "hrun/task_registry/task_lib.h"

using hrun::TaskMethod;
using hrun::BinaryOutputArchive;
using hrun::BinaryInputArchive;
using hrun::Task;
using hrun::TaskPointer;
using hrun::MultiQueue;
using hrun::PriorityInfo;
using hrun::TaskNode;
using hrun::DomainId;
using hrun::TaskStateId;
using hrun::QueueId;
using hrun::TaskFlags;
using hrun::DataTransfer;
using hrun::TaskLib;
using hrun::TaskLibClient;
using hrun::config::QueueManagerInfo;
using hrun::TaskPrio;
using hrun::RunContext;

using hshm::RwLock;
using hshm::Mutex;
using hshm::bitfield;
using hshm::bitfield32_t;
typedef hshm::bitfield<uint64_t> bitfield64_t;
using hshm::ScopedRwReadLock;
using hshm::ScopedRwWriteLock;
using hipc::LPointer;

#endif  // HRUN_INCLUDE_HRUN_HRUN_NAMESPACE_H_
