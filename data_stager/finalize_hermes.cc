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

#include "filesystem/metadata_manager.h"
#include "singleton.h"

using hermes::Singleton;

int main(int argc, char **argv) {
  auto mdm = Singleton<hermes::adapter::fs::MetadataManager>::GetInstance();
  setenv("HERMES_CLIENT", "1", true);
  setenv("HERMES_STOP_DAEMON", "1", true);
  MPI_Init(&argc, &argv);
  mdm->InitializeHermes(true);
  mdm->FinalizeHermes();
  MPI_Finalize();
}
