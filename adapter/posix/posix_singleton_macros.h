#ifndef HERMES_POSIX_SINGLETON_ADAPTER_MACROS_H
#define HERMES_POSIX_SINGLETON_ADAPTER_MACROS_H

#include "singleton.h"

#define HERMES_POSIX_API hermes::Singleton<hermes::adapter::posix::PosixApi>::GetInstance()
#define HERMES_POSIX_API_T hermes::adapter::posix::PosixApi*

#endif  // HERMES_POSIX_SINGLETON_ADAPTER_MACROS_H
