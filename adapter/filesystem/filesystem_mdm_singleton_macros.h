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

#ifndef HERMES_SINGLETON_ADAPTER_MACROS_H
#define HERMES_SINGLETON_ADAPTER_MACROS_H

#include "singleton.h"

#define HERMES_FS_METADATA_MANAGER \
  hermes::GlobalSingleton<hermes::adapter::fs::MetadataManager>::GetInstance()
#define HERMES_FS_METADATA_MANAGER_T hermes::adapter::fs::MetadataManager*

#endif  // HERMES_SINGLETON_ADAPTER_MACROS_H
