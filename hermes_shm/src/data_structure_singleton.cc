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

#include <hermes_shm/constants/macros.h>
#include <hermes_shm/util/singleton.h>
#include <hermes_shm/thread/lock/mutex.h>
#include <hermes_shm/constants/data_structure_singleton_macros.h>

#include <hermes_shm/introspect/system_info.h>
template<> hshm::SystemInfo hshm::GlobalSingleton<hshm::SystemInfo>::obj_ = hshm::SystemInfo();
#include <hermes_shm/memory/memory_registry.h>
template<> hshm::ipc::MemoryRegistry hshm::GlobalSingleton<hshm::ipc::MemoryRegistry>::obj_ = hshm::ipc::MemoryRegistry();
#include <hermes_shm/memory/memory_manager.h>
template<> hshm::ipc::MemoryManager hshm::GlobalSingleton<hshm::ipc::MemoryManager>::obj_ = hshm::ipc::MemoryManager();
#include <hermes_shm/thread/thread_manager.h>
template<> hshm::ThreadManager hshm::GlobalSingleton<hshm::ThreadManager>::obj_ = hshm::ThreadManager();
