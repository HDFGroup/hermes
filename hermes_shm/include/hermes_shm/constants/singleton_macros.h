#ifndef HERMES_SHM_INCLUDE_HERMES_SHM_CONSTANTS_SINGLETON_MACROS_H_H
#define HERMES_SHM_INCLUDE_HERMES_SHM_CONSTANTS_SINGLETON_MACROS_H_H

#include <hermes_shm/util/singleton.h>

#define HERMES_SHM_IPC_MANAGER scs::Singleton<hermes_shm::IpcManager>::GetInstance()
#define HERMES_SHM_IPC_MANAGER_T hermes_shm::IpcManager*

#define HERMES_SHM_CONFIGURATION_MANAGER scs::Singleton<hermes_shm::ConfigurationManager>::GetInstance()
#define HERMES_SHM_CONFIGURATION_MANAGER_T hermes_shm::ConfigurationManager*

#endif  // include_labstor_constants_singleton_macros_h
