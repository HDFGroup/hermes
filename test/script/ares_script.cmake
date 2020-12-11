# Usage:
#     ctest -S ares_script.cmake -VV

# This script takes in optional environment variables.
#   HERMES_BUILD_CONFIGURATION=Debug | Release
#   HERMES_DASHBOARD_MODEL=Experimental | Nightly | Continuous
#   HERMES_DO_COVERAGE

# Modify dashboard_cache as needed.

cmake_minimum_required (VERSION 3.10)

set(CTEST_PROJECT_NAME "HERMES")

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

# Number of jobs to build
set(CTEST_BUILD_FLAGS "-j4")

# Build name referenced in cdash
set(CTEST_BUILD_NAME "test-x64-${HERMES_BUILD_CONFIGURATION}")

set(CTEST_CMAKE_GENERATOR "Unix Makefiles")
# Must point to the root where we can checkout/build/run the tests
#set(CTEST_DASHBOARD_ROOT "$ENV{HOME}/workspace/Testing/${HERMES_DASHBOARD_MODEL}")
set(CTEST_DASHBOARD_ROOT "${CTEST_SCRIPT_DIRECTORY}")

# Give a site name
set(CTEST_SITE "$ENV{HOSTNAME}")
set(CTEST_TEST_TIMEOUT 180) # 3 minute timeout

# Optional coverage options
set(HERMES_DO_COVERAGE $ENV{HERMES_DO_COVERAGE})
if(NOT HERMES_DO_COVERAGE)
  set(HERMES_DO_COVERAGE OFF)
endif()
if(HERMES_DO_COVERAGE)
  message("Enabling Coverage")
  find_program (CTEST_COVERAGE_COMMAND NAMES gcov)
  set(CTEST_BUILD_NAME "${CTEST_BUILD_NAME}-coverage")
  # don't run parallel coverage tests, no matter what.
  set(CTEST_TEST_ARGS PARALLEL_LEVEL 1)

  # needed by hermes_common.cmake
  set(dashboard_do_coverage TRUE)

  # add Coverage dir to the root so that we don't mess the non-coverage
  # dashboard.
  set(CTEST_DASHBOARD_ROOT "${CTEST_DASHBOARD_ROOT}/Coverage")
endif()

set(dashboard_source_name hermes)
set(dashboard_binary_name hermes-${HERMES_BUILD_CONFIGURATION})

# Initial cache used to build hermes, options can be modified here
# This one is  consistent with github action setup, otherwise using default value
set(dashboard_cache "
CMAKE_CXX_FLAGS:STRING=-Wall -Wextra

BUILD_SHARED_LIBS:BOOL=ON
BUILD_TESTING:BOOL=ON
HERMES_INTERCEPT_IO:BOOL=OFF
HERMES_COMMUNICATION_MPI:BOOL=ON
HERMES_BUILD_BUFFER_POOL_VISUALIZER:BOOL=OFF
HERMES_USE_ADDRESS_SANITIZER:BOOL=ON
HERMES_USE_THREAD_SANITIZER:BOOL=OFF
HERMES_RPC_THALLIUM:BOOL=ON
HERMES_MDM_STORAGE_STBDS:BOOL=ON
HERMES_DEBUG_HEAP:BOOL=OFF
HERMES_BUILD_BENCHMARKS:BOOL=OFF
CMAKE_CXX_COMPILER:STRING=/opt/ohpc/pub/compiler/gcc/7.3.0/bin/g++
MPIEXEC_EXECUTABLE:STRING=/opt/ohpc/pub/mpi/openmpi3-gnu7/3.0.0/bin/mpirun
ORTOOLS_ROOT:STRING=/home/kmu/software/or-tools_Ubuntu-18.04-64bit_v7.7.7810
")

set(CTEST_SOURCE_DIRECTORY "/home/kmu/coverage_hermes")
set(CTEST_BINARY_DIRECTORY "/home/kmu/coverage_hermes/ctest_build")

set(dashboard_do_checkout FALSE)
set(dashboard_do_update FALSE)

include(hermes_common.cmake)
