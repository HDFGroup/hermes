#ifndef HERMES_SINGLETON_ADAPTER_MACROS_H
#define HERMES_SINGLETON_ADAPTER_MACROS_H

#include "singleton.h"

#define HERMES_FS_METADATA_MANAGER hermes::Singleton<hermes::adapter::fs::MetadataManager>::GetInstance()
#define HERMES_FS_METADATA_MANAGER_T hermes::adapter::posix::fs::MetadataManager*

#endif  // HERMES_SINGLETON_ADAPTER_MACROS_H
