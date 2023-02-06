cmake_minimum_required(VERSION 3.10)
project(hermes_shm)

set(CMAKE_CXX_STANDARD 17)

add_executable(test_types
        ${TEST_MAIN}/main.cc
        test_init.cc
        test_argpack.cc)
target_link_libraries(test_types
        Catch2::Catch2 MPI::MPI_CXX OpenMP::OpenMP_CXX)