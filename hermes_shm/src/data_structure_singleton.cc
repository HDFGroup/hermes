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
template<> hermes_shm::SystemInfo scs::Singleton<hermes_shm::SystemInfo>::obj_ = hermes_shm::SystemInfo();
#include <hermes_shm/memory/memory_registry.h>
template<> hermes_shm::ipc::MemoryRegistry scs::Singleton<hermes_shm::ipc::MemoryRegistry>::obj_ = hermes_shm::ipc::MemoryRegistry();
#include <hermes_shm/memory/memory_manager.h>
template<> hermes_shm::ipc::MemoryManager scs::Singleton<hermes_shm::ipc::MemoryManager>::obj_ = hermes_shm::ipc::MemoryManager();
#include <hermes_shm/thread/thread_manager.h>
template<> hermes_shm::ThreadManager scs::Singleton<hermes_shm::ThreadManager>::obj_ = hermes_shm::ThreadManager();
