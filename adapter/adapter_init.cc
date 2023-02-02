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
