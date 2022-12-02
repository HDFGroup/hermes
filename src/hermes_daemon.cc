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

#include <cstdlib>
#include <string>

#include <mpi.h>
#include <glog/logging.h>

/**
 * kHermesConf env variable is used to define path to kHermesConf in adapters.
 * This is used for initialization of Hermes.
 */
const char* kHermesConf = "HERMES_CONF";

int main(int argc, char* argv[]) {
  MPI_Init(&argc, &argv);
  std::string hermes_config = "";
  if (argc == 2) {
    hermes_config = argv[1];
  }

  auto hermes = hermes::InitHermesDaemon(hermes_config);
  hermes->RunDaemon();

  MPI_Finalize();

  return 0;
}
