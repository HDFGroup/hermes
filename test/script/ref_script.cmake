# Usage:
#     ctest -S ref_script.cmake -VV

# This script takes in optional environment variables.
#   HERMES_BUILD_CONFIGURATION=Debug | Release
#   HERMES_DASHBOARD_MODEL=Experimental | Nightly | Continuous

# Modify dashboard_cache as needed.

cmake_minimum_required (VERSION 3.10)

set(dashboard_do_checkout FALSE)
set(dashboard_do_update FALSE)

# HERMES_BUILD_CONFIGURATION = Debug | Release
set(HERMES_BUILD_CONFIGURATION "$ENV{HERMES_BUILD_CONFIGURATION}")
if(NOT HERMES_BUILD_CONFIGURATION)
  set(HERMES_BUILD_CONFIGURATION "Release")
endif()
set(CTEST_BUILD_CONFIGURATION ${HERMES_BUILD_CONFIGURATION})

# HERMES_DASHBOARD_MODEL=Experimental | Nightly | Continuous
set(HERMES_DASHBOARD_MODEL "$ENV{HERMES_DASHBOARD_MODEL}")
if(NOT HERMES_DASHBOARD_MODEL)
  set(HERMES_DASHBOARD_MODEL "Experimental")
endif()
set(dashboard_model ${HERMES_DASHBOARD_MODEL})

execute_process(
         COMMAND which mpicc
         OUTPUT_VARIABLE CMAKE_C_COMPILER
         )
execute_process(
         COMMAND which mpicxx
         OUTPUT_VARIABLE CMAKE_CXX_COMPILER
         )
execute_process(
         COMMAND which mpiexec
         OUTPUT_VARIABLE MPIEXEC_EXECUTABLE
         )

# Number of jobs to build
set(CTEST_BUILD_FLAGS "-j4")

# Build name referenced in cdash
set(CTEST_BUILD_NAME "test-x64-${HERMES_BUILD_CONFIGURATION}")

set(CTEST_CMAKE_GENERATOR "Unix Makefiles")

set(CTEST_SOURCE_DIRECTORY "/home/kmu/coverage_hermes")
set(CTEST_BINARY_DIRECTORY "/home/kmu/coverage_hermes/ctest_build")

# Give a site name
set(CTEST_SITE "$ENV{HOSTNAME}")
set(CTEST_TEST_TIMEOUT 180) # 3 minute timeout

# Optional coverage options
set(HERMES_DO_COVERAGE $ENV{HERMES_DO_COVERAGE})
if(NOT HERMES_DO_COVERAGE)
  set(HERMES_DO_COVERAGE ON)
endif()
if(HERMES_DO_COVERAGE)
  message("Enabling Coverage")
  find_program (CTEST_COVERAGE_COMMAND NAMES gcov)
  set(CTEST_BUILD_NAME "${CTEST_BUILD_NAME}-coverage")
  # don't run parallel coverage tests, no matter what.
  set(CTEST_TEST_ARGS PARALLEL_LEVEL 1)

  # needed by hermes_common.cmake
  set(dashboard_do_coverage TRUE)
endif()

set(dashboard_source_name hermes)
set(dashboard_binary_name hermes-${HERMES_BUILD_CONFIGURATION})

# Initial cache used to build hermes, options can be modified here
# This one is  consistent with github action setup, otherwise using default value
set(dashboard_cache "
CMAKE_CXX_FLAGS:STRING=${CXXFLAGS} -std=c++17 -Werror -Wall -Wextra
CMAKE_BUILD_TYPE:STRING=${HERMES_BUILD_CONFIGURATION}

BUILD_SHARED_LIBS:BOOL=ON
BUILD_TESTING:BOOL=ON

COVERAGE_COMMAND:FILEPATH=${CTEST_COVERAGE_COMMAND}

HERMES_ENABLE_COVERAGE:BOOL=${dashboard_do_coverage}
HERMES_INTERCEPT_IO:BOOL=OFF
HERMES_COMMUNICATION_MPI:BOOL=ON
HERMES_BUILD_BUFFER_POOL_VISUALIZER:BOOL=OFF
HERMES_USE_ADDRESS_SANITIZER:BOOL=ON
HERMES_USE_THREAD_SANITIZER:BOOL=OFF
HERMES_RPC_THALLIUM:BOOL=ON
HERMES_MDM_STORAGE_STBDS:BOOL=ON
HERMES_DEBUG_HEAP:BOOL=OFF
HERMES_BUILD_BENCHMARKS:BOOL=OFF
HERMES_ENABLE_TIMING:BOOL=OFF
CMAKE_C_COMPILER:STRING=${CMAKE_C_COMPILER}
CMAKE_CXX_COMPILER:STRING=${CMAKE_CXX_COMPILER}
MPIEXEC_EXECUTABLE:STRING=${MPIEXEC_EXECUTABLE}
ORTOOLS_ROOT:STRING=/home/kmu/software/or-tools_Ubuntu-18.04-64bit_v7.7.7810
")

include(hermes_common.cmake)
