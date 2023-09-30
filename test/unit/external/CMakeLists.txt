cmake_minimum_required(VERSION 3.10)
project(hermes)

set(CMAKE_CXX_STANDARD 17)
find_package(Hermes REQUIRED)
find_package(MPI REQUIRED COMPONENTS C CXX)
include_directories(Hermes_INCLUDE_DIRS)
add_executable(test_hermes_external_compile
        ../../main_mpi.cc
        ../hermes/test_init.cc
        ../hermes/test_bucket.cc
)
target_link_libraries(test_hermes_external_compile
        ${Hermes_LIBRARIES} Catch2::Catch2 MPI::MPI_CXX)
message("${Hermes_LIBRARIES}")

