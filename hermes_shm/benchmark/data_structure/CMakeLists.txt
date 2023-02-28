cmake_minimum_required(VERSION 3.10)
project(hermes_shm)

set(CMAKE_CXX_STANDARD 17)

include_directories( ${Boost_INCLUDE_DIRS} )
include_directories( ${TEST_MAIN} )
add_executable(benchmark_data_structures
        ${TEST_MAIN}/main.cc
        test_init.cc
        atomic.cc
        string.cc
        list.cc
        vector.cc
        # manual_ptr.cc
        # unique_ptr.cc
        unordered_map.cc
        # unordered_map_thread.cc
)
add_dependencies(benchmark_data_structures hermes_shm_data_structures)
target_link_libraries(benchmark_data_structures
        hermes_shm_data_structures
        Catch2::Catch2
        MPI::MPI_CXX
        OpenMP::OpenMP_CXX
        ${Boost_LIBRARIES})

make_gprof(benchmark_data_structures ${CMAKE_CURRENT_BINARY_DIR})