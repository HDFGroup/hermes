#ifndef HERMES_SINGLETON_SRC_MACROS_H
#define HERMES_SINGLETON_SRC_MACROS_H

#include "singleton.h"

#define HERMES hermes::Singleton<hermes::api::Hermes>::GetInstance()
#define HERMES_T hermes::api::Hermes*

#endif  // HERMES_SINGLETON_SRC_MACROS_H
