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

#ifndef HERMES_INCLUDE_HERMES_CONSTANTS_SINGLETON_MACROS_H_H
#define HERMES_INCLUDE_HERMES_CONSTANTS_SINGLETON_MACROS_H_H

#include <hermes_shm/util/singleton.h>

#define HERMES_IPC_MANAGER hermes_shm::GlobalSingleton<hermes_shm::IpcManager>::GetInstance()
#define HERMES_IPC_MANAGER_T hermes_shm::IpcManager*

#define HERMES_CONFIGURATION_MANAGER hermes_shm::GlobalSingleton<hermes_shm::ConfigurationManager>::GetInstance()
#define HERMES_CONFIGURATION_MANAGER_T hermes_shm::ConfigurationManager*

#endif  // include_labstor_constants_singleton_macros_h
