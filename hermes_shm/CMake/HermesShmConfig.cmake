# Find labstor header and library.
#

# This module defines the following uncached variables:
#  HermesShm_FOUND, if false, do not try to use labstor.
#  HermesShm_INCLUDE_DIRS, where to find labstor.h.
#  HermesShm_LIBRARIES, the libraries to link against to use the labstor library
#  HermesShm_LIBRARY_DIRS, the directory where the labstor library is found.

find_path(
  HermesShm_INCLUDE_DIR
        hermes_shm/hermes_shm.h
)

if( HermesShm_INCLUDE_DIR )
  get_filename_component(HermesShm_DIR ${HermesShm_INCLUDE_DIR} PATH)

  #-----------------------------------------------------------------------------
  # Find all packages needed by hermes_shm
  #-----------------------------------------------------------------------------
  find_library(
    HermesShm_LIBRARY
    NAMES hermes_shm_data_structures
  )
  find_library(LIBRT rt)
  if(NOT LIBRT)
    message(FATAL_ERROR "librt is required for POSIX shared memory")
  endif()

  #-----------------------------------------------------------------------------
  # Mark hermes as found and set all needed packages
  #-----------------------------------------------------------------------------
  if( HermesShm_LIBRARY )
    set(HermesShm_LIBRARY_DIR "")
    get_filename_component(HermesShm_LIBRARY_DIRS ${HermesShm_LIBRARY} PATH)
    # Set uncached variables as per standard.
    set(HermesShm_FOUND ON)
    set(HermesShm_INCLUDE_DIRS ${HermesShm_INCLUDE_DIR})
    set(HermesShm_LIBRARIES -lrt -ldl ${HermesShm_LIBRARY})
  endif(HermesShm_LIBRARY)

else(HermesShm_INCLUDE_DIR)
  message(STATUS "FindHermesShm: Could not find hermes_shm.h")
endif(HermesShm_INCLUDE_DIR)
	    
if(HermesShm_FOUND)
  if(NOT HermesShm_FIND_QUIETLY)
    message(STATUS "FindHermesShm: Found both hermes_shm.h and libhermes_shm.a")
  endif(NOT HermesShm_FIND_QUIETLY)
else(HermesShm_FOUND)
  if(HermesShm_FIND_REQUIRED)
    message(STATUS "FindHermesShm: Could not find hermes_shm.h and/or libhermes_shm.a")
  endif(HermesShm_FIND_REQUIRED)
endif(HermesShm_FOUND)
