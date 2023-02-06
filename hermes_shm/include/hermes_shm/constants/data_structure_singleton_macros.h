#ifndef HERMES_SHM_INCLUDE_HERMES_SHM_CONSTANTS_DATA_STRUCTURE_SINGLETON_MACROS_H_H
#define HERMES_SHM_INCLUDE_HERMES_SHM_CONSTANTS_DATA_STRUCTURE_SINGLETON_MACROS_H_H

#include <hermes_shm/util/singleton.h>

#define HERMES_SHM_SYSTEM_INFO scs::Singleton<hermes_shm::SystemInfo>::GetInstance()
#define HERMES_SHM_SYSTEM_INFO_T hermes_shm::SystemInfo*

#define HERMES_SHM_MEMORY_MANAGER scs::Singleton<hermes_shm::ipc::MemoryManager>::GetInstance()
#define HERMES_SHM_MEMORY_MANAGER_T hermes_shm::ipc::MemoryManager*

#define HERMES_SHM_THREAD_MANAGER scs::Singleton<hermes_shm::ThreadManager>::GetInstance()
#define HERMES_SHM_THREAD_MANAGER_T hermes_shm::ThreadManager*

#endif  // include_labstor_constants_data_structure_singleton_macros_h
