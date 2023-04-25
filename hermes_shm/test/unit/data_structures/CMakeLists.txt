cmake_minimum_required(VERSION 3.10)
project(hermes_shm)

set(CMAKE_CXX_STANDARD 17)

include_directories(${HERMES_SHM_ROOT}/test/unit)
add_subdirectory(backend)
add_subdirectory(containers)
#add_subdirectory(containers_mpi)
add_subdirectory(serialize)
