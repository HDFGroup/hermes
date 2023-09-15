# Find labstor header and library.
#

# This module defines the following uncached variables:
#  Labstor_FOUND, if false, do not try to use labstor.
#  Labstor_INCLUDE_DIRS, where to find labstor.h.
#  Labstor_LIBRARIES, the libraries to link against to use the labstor library
#  Labstor_LIBRARY_DIRS, the directory where the labstor library is found.

find_path(
  Labstor_INCLUDE_DIR
        labstor/labstor_types.h
)

if( Labstor_INCLUDE_DIR )
  get_filename_component(Labstor_DIR ${Labstor_INCLUDE_DIR} PATH)

  #-----------------------------------------------------------------------------
  # Find all packages needed by labstor
  #-----------------------------------------------------------------------------
  find_library(
    Labstor_LIBRARY
    NAMES labstor_client labstor_runtime
  )

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
  if(cereal)
    message(STATUS "found cereal")
  endif()

  #-----------------------------------------------------------------------------
  # Mark hermes as found and set all needed packages
  #-----------------------------------------------------------------------------
  if( Labstor_LIBRARY )
    set(Labstor_LIBRARY_DIR "")
    get_filename_component(Labstor_LIBRARY_DIRS ${Labstor_LIBRARY} PATH)
    # Set uncached variables as per standard.
    set(Labstor_FOUND ON)
    set(Labstor_INCLUDE_DIRS ${Labstor_INCLUDE_DIR})
    set(Labstor_LIBRARIES
            ${HermesShm_LIBRARIES}
            yaml-cpp
            cereal::cereal
            -ldl -lrt -lc -pthread ${Labstor_LIBRARY})
  endif(Labstor_LIBRARY)

else(Labstor_INCLUDE_DIR)
  message(STATUS "FindLabstor: Could not find labstor.h")
endif(Labstor_INCLUDE_DIR)
	    
if(Labstor_FOUND)
  if(NOT Labstor_FIND_QUIETLY)
    message(STATUS "FindLabstor: Found both labstor.h and liblabstor_client.so")
  endif(NOT Labstor_FIND_QUIETLY)
else(Labstor_FOUND)
  if(Labstor_FIND_REQUIRED)
    message(STATUS "FindLabstor: Could not find labstor.h and/or liblabstor_client.so")
  endif(Labstor_FIND_REQUIRED)
endif(Labstor_FOUND)
