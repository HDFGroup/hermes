# This script takes in optional environment variables.
#   HERMES_BUILD_CONFIGURATION=Debug | Release
#   HERMES_DASHBOARD_MODEL=Experimental | Nightly | Continuous
#   HERMES_DO_COVERAGE

# HERMES_BUILD_CONFIGURATION = Debug | Release
set(HERMES_BUILD_CONFIGURATION "$ENV{HERMES_BUILD_CONFIGURATION}")
if(NOT HERMES_BUILD_CONFIGURATION)
  set(HERMES_BUILD_CONFIGURATION "Debug")
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
set(CTEST_DASHBOARD_ROOT "$ENV{HOME}/workspace/Testing/${HERMES_DASHBOARD_MODEL}")
# Give a site name
set(CTEST_SITE "$ENV{HOSTNAME}")
set(CTEST_TEST_TIMEOUT 180) # 3 minute timeout

# Optional coverage options
set(HERMES_DO_COVERAGE $ENV{HERMES_DO_COVERAGE})
if(HERMES_DO_COVERAGE)
  message("Enabling Coverage")
  set(CTEST_COVERAGE_COMMAND "/usr/bin/gcov")
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
# Be consistent with github action setup, otherwise using default value
set(dashboard_cache "
CMAKE_CXX_FLAGS:STRING=-Wall -Wextra

BUILD_SHARED_LIBS:BOOL=ON
BUILD_TESTING:BOOL=OFF
HERMES_INTERCEPT_IO:BOOL=ON
HERMES_COMMUNICATION_MPI:BOOL=ON
HERMES_BUILD_BUFFER_POOL_VISUALIZER:BOOL=ON
HERMES_USE_ADDRESS_SANITIZER:BOOL=ON
HERMES_USE_THREAD_SANITIZER:BOOL=OFF
HERMES_RPC_THALLIUM:BOOL=ON
HERMES_MDM_STORAGE_STBDS:BOOL=ON
HERMES_DEBUG_HEAP:BOOL=OFF
HERMES_BUILD_BENCHMARKS:BOOL=OFF
ORTOOLS_ROOT:STRING=/home/cc/software/or-tools_Ubuntu-18.04-64bit_v8.0.8283
")

include(hermes_common.cmake)
