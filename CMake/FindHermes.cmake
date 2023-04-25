# Find labstor header and library.
#

# This module defines the following uncached variables:
#  Hermes_FOUND, if false, do not try to use labstor.
#  Hermes_INCLUDE_DIRS, where to find labstor.h.
#  Hermes_LIBRARIES, the libraries to link against to use the labstor library
#  Hermes_LIBRARY_DIRS, the directory where the labstor library is found.

find_path(
  Hermes_INCLUDE_DIR
        hermes/hermes.h
)

if( Hermes_INCLUDE_DIR )
  get_filename_component(Hermes_DIR ${Hermes_INCLUDE_DIR} PATH)

  find_library(
    Hermes_LIBRARY
    NAMES hermes
  )

  if( Hermes_LIBRARY )
    set(Hermes_LIBRARY_DIR "")
    get_filename_component(Hermes_LIBRARY_DIRS ${Hermes_LIBRARY} PATH)
    # Set uncached variables as per standard.
    set(Hermes_FOUND ON)
    set(Hermes_INCLUDE_DIRS ${Hermes_INCLUDE_DIR})
    set(Hermes_LIBRARIES ${Hermes_LIBRARY})
  endif(Hermes_LIBRARY)
else(Hermes_INCLUDE_DIR)
  message(STATUS "FindHermes: Could not find hermes_shm.h")
endif(Hermes_INCLUDE_DIR)
	    
if(Hermes_FOUND)
  if(NOT Hermes_FIND_QUIETLY)
    message(STATUS "FindHermes: Found both hermes_shm.h and libhermes.so")
  endif(NOT Hermes_FIND_QUIETLY)
else(Hermes_FOUND)
  if(Hermes_FIND_REQUIRED)
    message(STATUS "FindHermes: Could not find hermes_shm.h and/or libhermes.so")
  endif(Hermes_FIND_REQUIRED)
endif(Hermes_FOUND)
