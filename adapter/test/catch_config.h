#ifndef HERMES_CATCH_CONFIG_H
#define HERMES_CATCH_CONFIG_H

#define CATCH_CONFIG_RUNNER
#include <catch2/catch.hpp>
#include <mpi.h>

namespace cl = Catch::clara;

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
