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

#define CATCH_CONFIG_RUNNER
#include <catch2/catch_all.hpp>
#include <iostream>
#include <cstdlib>
#include <mpi.h>
#include "hermes/hermes.h"

namespace cl = Catch::Clara;
cl::Parser define_options();

int main(int argc, char **argv) {
  int rc;
  MPI_Init(&argc, &argv);
  Catch::Session session;
  auto cli = session.cli();
  session.cli(cli);
  rc = session.applyCommandLine(argc, argv);
  if (rc != 0) return rc;
  rc = session.run();
  if (rc != 0) return rc;
  MPI_Finalize();
  return rc;
}

TEST_CASE("TestHermesConnect") {
  int rank, nprocs;
  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
  HERMES->ClientInit();
  MPI_Barrier(MPI_COMM_WORLD);
}