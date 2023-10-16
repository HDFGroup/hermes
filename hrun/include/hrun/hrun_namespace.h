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
