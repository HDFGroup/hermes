cmake_minimum_required(VERSION 3.10)
project(hermes_shm)

set(CMAKE_CXX_STANDARD 17)

#------------------------------------------------------------------------------
# Global variables
#------------------------------------------------------------------------------
set(HERMES_SHM_ROOT ${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${HERMES_SHM_ROOT}/include)

#------------------------------------------------------------------------------
# Options
#------------------------------------------------------------------------------
option(BUILD_SHARED_LIBS "Build shared libraries (.dll/.so) instead of static ones (.lib/.a)" ON)
option(BUILD_MPI_TESTS "Build tests which depend on MPI" ON)
option(BUILD_OpenMP_TESTS "Build tests which depend on OpenMP" ON)
option(BUILD_Boost_TESTS "Build tests which depend on libboost" ON)
option(HERMES_RPC_THALLIUM "Build tests which depend on thallium" ON)
option(HERMES_ENABLE_COVERAGE "Check how well tests cover code" ON)
option(HERMES_ENABLE_DOXYGEN "Check how well the code is documented" ON)
option(HERMES_CXX_PROFILE "Generate profiling data from benchmarks" OFF)

#------------------------------------------------------------------------------
# Setup install and output Directories
#------------------------------------------------------------------------------
if(NOT HERMES_INSTALL_BIN_DIR)
    set(HERMES_INSTALL_BIN_DIR ${CMAKE_INSTALL_PREFIX}/bin)
endif()
if(NOT HERMES_INSTALL_LIB_DIR)
    set(HERMES_INSTALL_LIB_DIR ${CMAKE_INSTALL_PREFIX}/lib)
endif()
if(NOT HERMES_INSTALL_INCLUDE_DIR)
    set(HERMES_INSTALL_INCLUDE_DIR ${CMAKE_INSTALL_PREFIX}/include)
endif()
if(NOT HERMES_INSTALL_DATA_DIR)
    set(HERMES_INSTALL_DATA_DIR ${CMAKE_INSTALL_PREFIX}/share)
endif()

#------------------------------------------------------------------------------
# CMake Modules
#------------------------------------------------------------------------------
if (NOT IS_HERMES_MAIN)
    set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_CURRENT_SOURCE_DIR}/CMake)
    message(${CMAKE_MODULE_PATH})
endif()

#------------------------------------------------------------------------------
# Optimization
#------------------------------------------------------------------------------
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    message("IN DEBUG MODE")
    set(CMAKE_CXX_FLAGS "-g -O0")
elseif(CMAKE_BUILD_TYPE STREQUAL "Release")
    message("IN RELEASE MODE")
    set(CMAKE_CXX_FLAGS "-g -O3")
endif()
if(HERMES_CXX_PROFILE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")
    message("CXX PROFILER IS ON")
endif()
function(make_gprof exec_name exec_dir)
    message("gprof -b -A -p -q ${exec_dir}/${exec_name} gmon.out > gprof_out.txt")
    add_custom_target(
      ${exec_name}_gprof
      COMMAND gprof -b -A -p -q ${exec_dir}/${exec_name} gmon.out)
endfunction()

#------------------------------------------------------------------------------
# External libraries
#------------------------------------------------------------------------------

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
find_package(OpenMP REQUIRED COMPONENTS C CXX)
message(STATUS "found omp.h at ${OpenMP_CXX_INCLUDE_DIRS}")
#endif()

# thallium
if(HERMES_RPC_THALLIUM)
    find_package(thallium CONFIG REQUIRED)
    if(thallium_FOUND)
        message(STATUS "found thallium at ${thallium_DIR}")
    endif()
endif()

# Boost
if(BUILD_Boost_TESTS)
    find_package(Boost REQUIRED)
    message(STATUS "found boost.h at ${Boost_INCLUDE_DIRS}")
endif()

#------------------------------------------------------------------------------
# Code Coverage
#------------------------------------------------------------------------------
if (NOT IS_HERMES_MAIN)
    if(HERMES_ENABLE_COVERAGE)
        set(COVERAGE_FLAGS "-fprofile-arcs -ftest-coverage" CACHE STRING
            "Flags to the coverage program to perform coverage inspection")
        mark_as_advanced(COVERAGE_FLAGS)

        macro(set_coverage_flags target)
            set_target_properties(${target}
                    PROPERTIES
                    COMPILE_FLAGS ${COVERAGE_FLAGS}
                    LINK_FLAGS ${COVERAGE_FLAGS}
                    )
        endmacro()
    endif()
endif()

#------------------------------------------------------------------------------
# Documentation
#------------------------------------------------------------------------------
if (NOT IS_HERMES_MAIN)
    if(HERMES_ENABLE_DOXYGEN)
        include(UseDoxygenDoc)
        add_doxygen_doc(
                BUILD_DIR
                ${CMAKE_CURRENT_BINARY_DIR}/_build
                DOXY_FILE
                ${CMAKE_CURRENT_SOURCE_DIR}/doc/Doxyfile.in
                TARGET_NAME
                dox
                COMMENT
                "HTML documentation"
        )
    endif()
endif()

#------------------------------------------------------------------------------
# Build hermes_shm
#------------------------------------------------------------------------------
add_subdirectory(src)

if (NOT IS_HERMES_MAIN)
    add_custom_target(lint COMMAND bash ${HERMES_SHM_ROOT}/scripts/lint.sh ${HERMES_SHM_ROOT})
    add_custom_target(preamble COMMAND python3 ${HERMES_SHM_ROOT}/scripts/preamble.py ${HERMES_SHM_ROOT})
endif()

#------------------------------------------------------------------------------
# Build tests + benchmarks
#------------------------------------------------------------------------------
#set(TEST_MAIN ${HERMES_SHM_ROOT}/test/unit)
#enable_testing()
#add_subdirectory(test)
#add_subdirectory(benchmark)
#add_subdirectory(example)

#------------------------------------------------------------------------------
# Install hshm
#------------------------------------------------------------------------------
install(DIRECTORY include DESTINATION ${CMAKE_INSTALL_PREFIX})
