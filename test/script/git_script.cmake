# Usage:
#     ctest -S git_script.cmake -VV

# This script takes in optional environment variables.
#   HERMES_DASHBOARD_MODEL=Experimental | Nightly | Continuous
#   HERMES_DO_COVERAGE

# Modify dashboard_cache as needed.

cmake_minimum_required (VERSION 3.10)

# Checkout is done by git
set(dashboard_do_checkout 0)
set(dashboard_do_update 0)

if(NOT DEFINED CTEST_TEST_TIMEOUT)
  set(CTEST_TEST_TIMEOUT 180)
endif()

if(NOT DEFINED CTEST_SUBMIT_NOTES)
  set(CTEST_SUBMIT_NOTES TRUE)
endif()

# Give a site name
set(CTEST_SITE "github.com/HDFGroup/hermes/actions")

set (CTEST_SOURCE_DIRECTORY ".")
set (CTEST_BINARY_DIRECTORY "${CTEST_SOURCE_DIRECTORY}/build")

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

# Number of jobs to build
set(CTEST_BUILD_FLAGS "-j4")

# Build name referenced in cdash
set(CTEST_BUILD_NAME "test-x64-${HERMES_BUILD_CONFIGURATION}")

set(CTEST_CMAKE_GENERATOR "Unix Makefiles")

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
set(dashboard_cache "
CMAKE_CXX_FLAGS:STRING=${CXXFLAGS} -std=c++17 -Werror -Wall -Wextra
CMAKE_BUILD_TYPE:STRING=$ENV{BUILD_TYPE}
CMAKE_INSTALL_PREFIX:PATH=$ENV{HOME}/local
CMAKE_PREFIX_PATH:PATH=$ENV{HOME}/local
CMAKE_BUILD_RPATH:PATH=$ENV{HOME}/local/lib
CMAKE_INSTALL_RPATH:PATH=$ENV{HOME}/local/lib
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
CMAKE_C_COMPILER:STRING=${CMAKE_C_COMPILER}
CMAKE_CXX_COMPILER:STRING=${CMAKE_CXX_COMPILER}
")

include(${CTEST_SCRIPT_DIRECTORY}/hermes_common.cmake)
