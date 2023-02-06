# Find hermes_shm header and library.
#

# This module defines the following uncached variables:
#  LabStor_FOUND, if false, do not try to use hermes_shm.
#  LabStor_INCLUDE_DIRS, where to find hermes_shm.h.
#  LabStor_LIBRARIES, the libraries to link against to use the hermes_shm library
#  LabStor_LIBRARY_DIRS, the directory where the hermes_shm library is found.

find_path(
  LabStor_INCLUDE_DIR
  hermes_shm/hermes_shm.h
)

if( LabStor_INCLUDE_DIR )
  get_filename_component(LabStor_DIR ${LabStor_INCLUDE_DIR} PATH)

  find_library(
    LabStor_LIBRARY
    NAMES hermes_shm_data_structures
  )

  if( LabStor_LIBRARY )
    set(LabStor_LIBRARY_DIR "")
    get_filename_component(LabStor_LIBRARY_DIRS ${LabStor_LIBRARY} PATH)
    # Set uncached variables as per standard.
    set(LabStor_FOUND ON)
    set(LabStor_INCLUDE_DIRS ${LabStor_INCLUDE_DIR})
    set(LabStor_LIBRARIES ${LabStor_LIBRARY})
  endif(LabStor_LIBRARY)
else(LabStor_INCLUDE_DIR)
  message(STATUS "Findlabstor: Could not find hermes_shm.h")
endif(LabStor_INCLUDE_DIR)
	    
if(LabStor_FOUND)
  if(NOT LabStor_FIND_QUIETLY)
    message(STATUS "Findlabstor: Found both hermes_shm.h and liblabstor.a")
  endif(NOT LabStor_FIND_QUIETLY)
else(LabStor_FOUND)
  if(LabStor_FIND_REQUIRED)
    message(STATUS "Findlabstor: Could not find hermes_shm.h and/or liblabstor.a")
  endif(LabStor_FIND_REQUIRED)
endif(LabStor_FOUND)
