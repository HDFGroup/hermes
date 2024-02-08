# Find Hermes header and library.
#

# This module defines the following uncached variables:
#  Hermes_FOUND, if false, do not try to use Hermes.
#  Hermes_INCLUDE_DIRS, where to find Hermes.h.
#  Hermes_LIBRARIES, the libraries to link against to use the Hermes library
#  Hermes_LIBRARY_DIRS, the directory where the Hermes library is found.

find_path(
  Hermes_INCLUDE_DIR
        hermes/hermes_types.h
  HINTS ENV PATH ENV CPATH
)
if (NOT Hermes_INCLUDE_DIR)
  message(STATUS "FindHermes: Could not find Hermes.h")
  set(Hermes_FOUND FALSE)
  return()
endif()
get_filename_component(Hermes_DIR ${Hermes_INCLUDE_DIR} PATH)

#-----------------------------------------------------------------------------
# Find all packages needed by Hermes
#-----------------------------------------------------------------------------
find_library(
  Hermes_LIBRARY
  NAMES hrun_client
  HINTS ENV LD_LIBRARY_PATH ENV PATH
)
if (NOT Hermes_LIBRARY)
    message(STATUS "FindHermes: Could not find libhrun_client.so")
    set(Hermes_FOUND FALSE)
    message(STATUS "LIBS: $ENV{LD_LIBRARY_PATH}")
    return()
endif()

# HermesShm
find_package(HermesShm CONFIG REQUIRED)
message(STATUS "found hermes_shm.h at ${HermesShm_INCLUDE_DIRS}")

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
if(BUILD_OpenMP_TESTS)
  find_package(OpenMP REQUIRED COMPONENTS C CXX)
  message(STATUS "found omp.h at ${OpenMP_CXX_INCLUDE_DIRS}")
endif()

# Cereal
find_package(cereal REQUIRED)
message(STATUS "found cereal")

# Boost
find_package(Boost REQUIRED COMPONENTS regex system filesystem fiber REQUIRED)
message(STATUS "found boost at ${Boost_INCLUDE_DIRS}")

# Thallium
find_package(thallium CONFIG REQUIRED)
if(thallium_FOUND)
  message(STATUS "found thallium at ${thallium_DIR}")
endif()

#-----------------------------------------------------------------------------
# Mark hermes as found and set all needed packages
#-----------------------------------------------------------------------------
set(Hermes_LIBRARY_DIR "")
get_filename_component(Hermes_LIBRARY_DIRS ${Hermes_LIBRARY} PATH)
# Set uncached variables as per standard.
set(Hermes_FOUND ON)
set(Hermes_INCLUDE_DIRS ${Boost_INCLUDE_DIRS} ${Hermes_INCLUDE_DIR})
set(Hermes_LIBRARIES
        ${HermesShm_LIBRARIES}
        yaml-cpp
        cereal::cereal
        -ldl -lrt -lc -pthread
        thallium
        hermes
        ${Boost_LIBRARIES} ${Hermes_LIBRARY})
set(Hermes_CLIENT_LIBRARIES ${Hermes_LIBRARIES})
set(Hermes_RUNTIME_LIBRARIES
        ${Hermes_CLIENT_LIBRARIES}
        hrun_runtime
        ${Boost_LIBRARIES})
set(Hermes_RUNTIME_DEPS "")
