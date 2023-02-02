//
// Created by lukemartinlogan on 2/1/23.
//

#ifndef HERMES_ADAPTER_GLOBAL_INIT_HERMES_H_
#define HERMES_ADAPTER_GLOBAL_INIT_HERMES_H_

#include "hermes.h"

/** Use C++ runtime to ensure Hermes starts and finalizes */
struct TransparentInitHermes {
  TransparentInitHermes() {
    HERMES->Create(hermes::HermesType::kClient);
  }

  ~TransparentInitHermes() {
    HERMES->Finalize();
  }
};

/** Initialize Hermes transparently */
TransparentInitHermes x;

#endif  // HERMES_ADAPTER_GLOBAL_INIT_HERMES_H_
