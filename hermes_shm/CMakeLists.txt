cmake_minimum_required(VERSION 3.10)
project(hermes_shm)

set(CMAKE_CXX_STANDARD 17)

include_directories(${CMAKE_SOURCE_DIR}/include)

###################OPTIONS
option(BUILD_SHARED_LIBS "Build shared libraries (.dll/.so) instead of static ones (.lib/.a)" ON)
option(BUILD_TESTS "Wether or not to build unit tests" OFF)
option(BUILD_MPI_TESTS "Build tests which depend on MPI" OFF)
option(BUILD_OpenMP_TESTS "Build tests which depend on OpenMP" OFF)
option(BUILD_Boost_TESTS "Build tests which depend on libboost" OFF)

##################CMAKE MODULES
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_CURRENT_SOURCE_DIR}/CMake)

##################OPTIMIZATION
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    message("IN DEBUG MODE")
    set(CMAKE_CXX_FLAGS "-g -O0")
elseif(CMAKE_BUILD_TYPE STREQUAL "Release")
    message("IN RELEASE MODE")
    set(CMAKE_CXX_FLAGS "-g -O3")
endif()
function(make_gprof exec_name exec_dir)
    message("gprof -b -A -p -q ${exec_dir}/${exec_name} gmon.out > gprof_out.txt")
    add_custom_target(
      ${exec_name}_gprof
      COMMAND gprof -b -A -p -q ${exec_dir}/${exec_name} gmon.out)
endfunction()

##################REQUIRED EXTERNAL LIBRARIES

# YAML-CPP
find_package(yaml-cpp REQUIRED)
message(STATUS "found yaml-cpp at ${yaml-cpp_DIR}")

# Catch2
find_package(Catch2 3.0.1 REQUIRED)
message(STATUS "found catch2.h at ${Catch2_CXX_INCLUDE_DIRS}")

# MPICH
if(BUILD_MPI_TESTS)
    find_package(MPI REQUIRED COMPONENTS C CXX)
    message(STATUS "found mpi.h at ${MPI_CXX_INCLUDE_DIRS}")
endif()

# OpenMP
#if(BUILD_OpenMP_TESTS)
#    find_package(OpenMP REQUIRED COMPONENTS C CXX)
#    message(STATUS "found omp.h at ${OpenMP_CXX_INCLUDE_DIRS}")
#endif()

##################Build HermesShm main packages
add_subdirectory(src)

##################MODULES & TESTS
set(TEST_MAIN ${CMAKE_SOURCE_DIR}/test/unit)
#if (BUILD_TESTS)
#    enable_testing()
#    add_subdirectory(test)
#endif()