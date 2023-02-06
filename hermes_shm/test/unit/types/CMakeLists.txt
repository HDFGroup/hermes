cmake_minimum_required(VERSION 3.10)
project(hermes_shm)

set(CMAKE_CXX_STANDARD 17)

add_executable(test_types
        ${TEST_MAIN}/main.cc
        test_init.cc
        test_argpack.cc)
target_link_libraries(test_types
        Catch2::Catch2 MPI::MPI_CXX OpenMP::OpenMP_CXX)

#add_test(NAME test_connect COMMAND
#        bash ${CMAKE_CURRENT_SOURCE_DIR}/test_connect.sh
#            ${CMAKE_BINARY_DIR}/labstor_runtime
#            ${CMAKE_CURRENT_BINARY_DIR}/test_connect_exec
#            ${CMAKE_SOURCE_DIR}/config/config.yaml)