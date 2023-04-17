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

#include "hermes_shm/util/singleton.h"
#include "hermes.h"

int main(int argc, char **argv) {
  HILOG(kDebug, "Finalize Hermes begins")
  HERMES->Create(hermes::HermesType::kClient);
  HERMES->client_config_.stop_daemon_ = true;
  HERMES->Flush(true);
  HERMES->WaitForFinalize();
  HERMES->Finalize();
}
