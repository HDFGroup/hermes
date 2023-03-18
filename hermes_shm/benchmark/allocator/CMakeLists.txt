cmake_minimum_required(VERSION 3.10)
project(hermes_shm)

set(CMAKE_CXX_STANDARD 17)

include_directories( ${Boost_INCLUDE_DIRS} )
include_directories( ${TEST_MAIN} )
add_executable(benchmark_allocators
        ${TEST_MAIN}/main.cc
        test_init.cc
        allocator.cc
)
add_dependencies(benchmark_allocators hermes_shm_data_structures)
target_link_libraries(benchmark_allocators
        hermes_shm_data_structures
        Catch2::Catch2
        MPI::MPI_CXX
        OpenMP::OpenMP_CXX
        ${Boost_LIBRARIES})
