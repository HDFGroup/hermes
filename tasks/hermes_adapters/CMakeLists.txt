#------------------------------------------------------------------------------
# Build Labstor Admin Task Library
#------------------------------------------------------------------------------
include_directories(include)
add_subdirectory(filesystem)
if (HERMES_ENABLE_POSIX_ADAPTER)
    add_subdirectory(posix)
endif()
if (HERMES_ENABLE_STDIO_ADAPTER)
    add_subdirectory(stdio)
endif()
if (HERMES_ENABLE_MPIIO_ADAPTER)
    add_subdirectory(mpiio)
endif()
if (HERMES_ENABLE_HDF5_ADAPTER)
    add_subdirectory(vfd)
endif()
add_subdirectory(src)

#-----------------------------------------------------------------------------
# Install LabStor Admin Task Library Headers
#-----------------------------------------------------------------------------
install(DIRECTORY include DESTINATION ${CMAKE_INSTALL_PREFIX})
