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

#ifndef HERMES_CATCH_CONFIG_H
#define HERMES_CATCH_CONFIG_H

#define CATCH_CONFIG_RUNNER
#include <catch2/catch_all.hpp>
#include <mpi.h>

namespace cl = Catch::Clara;

cl::Parser define_options();

int init();
int finalize();

int main(int argc, char* argv[]) {
    MPI_Init(&argc, &argv);
    Catch::Session session;
    auto cli = session.cli() | define_options();
    int returnCode = init();
    if (returnCode != 0) return returnCode;
    session.cli(cli);
    returnCode = session.applyCommandLine(argc, argv);
    if (returnCode != 0) return returnCode;
    int test_return_code = session.run();
    returnCode = finalize();
    if (returnCode != 0) return returnCode;
    MPI_Finalize();
    return test_return_code;
}

#endif
