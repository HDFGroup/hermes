# Find hermes header and library.
#

# This module defines the following uncached variables:
#  Hermes_FOUND, if false, do not try to use hermes.
#  Hermes_INCLUDE_DIRS, where to find hermes.h.
#  Hermes_LIBRARIES, the libraries to link against to use the hermes library
#  Hermes_LIBRARY_DIRS, the directory where the hermes library is found.

#-----------------------------------------------------------------------------
# Version Strings
#-----------------------------------------------------------------------------
set(HERMES_VERSION_STRING  @HERMES_PACKAGE_VERSION@)
set(HERMES_VERSION_MAJOR   @HERMES_VERSION_MAJOR@)
set(HERMES_VERSION_MINOR   @HERMES_VERSION_MINOR@)
set(HERMES_VERSION_PATCH   @HERMES_VERSION_PATCH@)

#-----------------------------------------------------------------------------
# C++ Version
#-----------------------------------------------------------------------------
set(CMAKE_CXX_STANDARD 17)

#-----------------------------------------------------------------------------
# Find hermes.h
#-----------------------------------------------------------------------------
find_path(Hermes_INCLUDE_DIR hermes.h)
if (NOT Hermes_INCLUDE_DIR)
    set(Hermes_FOUND OFF)
    message(SEND_ERROR "FindHermes: Could not find hermes.h")
else()
    #-----------------------------------------------------------------------------
    # Find hermes install dir and hermes.so
    #-----------------------------------------------------------------------------
    get_filename_component(Hermes_DIR ${Hermes_INCLUDE_DIR} PATH)
    find_library(
            Hermes_LIBRARY
            NAMES hermes
    )

    #-----------------------------------------------------------------------------
    # Mark hermes as found
    #-----------------------------------------------------------------------------
    if( Hermes_LIBRARY )
        set(Hermes_LIBRARY_DIR "")
        get_filename_component(Hermes_LIBRARY_DIRS ${Hermes_LIBRARY} PATH)
        # Set uncached variables as per standard.
        set(Hermes_FOUND ON)
        set(Hermes_INCLUDE_DIRS ${Hermes_INCLUDE_DIR})
        set(Hermes_LIBRARIES ${Hermes_LIBRARY})
    endif(Hermes_LIBRARY)

    #-----------------------------------------------------------------------------
    # Find all packages needed by hermes
    #-----------------------------------------------------------------------------
    # thallium
    if(@HERMES_RPC_THALLIUM@)
        find_package(thallium CONFIG REQUIRED)
        if(thallium_FOUND)
            message(STATUS "found thallium at ${thallium_DIR}")
        endif()
    endif()

    #YAML-CPP
    find_package(yaml-cpp REQUIRED)
    if(yaml-cpp_FOUND)
        message(STATUS "found yaml-cpp at ${yaml-cpp_DIR}")
    endif()

    #Cereal
    find_package(cereal REQUIRED)
    if(cereal)
        message(STATUS "found cereal")
    endif()

    #if(HERMES_COMMUNICATION_MPI)
    find_package(MPI REQUIRED COMPONENTS C CXX)
    message(STATUS "found mpi.h at ${MPI_CXX_INCLUDE_DIRS}")
    #endif()

    # librt
    if(NOT APPLE)
        find_library(LIBRT rt)
        if(NOT LIBRT)
            message(FATAL_ERROR "librt is required for POSIX shared memory")
        endif()
    endif()

    #-----------------------------------------------------------------------------
    # Set all packages needed by hermes
    #-----------------------------------------------------------------------------

    # Set the Hermes library dependencies
    set(Hermes_LIBRARIES yaml-cpp thallium
            MPI::MPI_CXX stdc++fs dl cereal::cereal hermes)
endif()
