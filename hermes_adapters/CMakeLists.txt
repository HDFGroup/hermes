#------------------------------------------------------------------------------
# Build Hrun Admin Task Library
#------------------------------------------------------------------------------
add_subdirectory(filesystem)
add_subdirectory(posix)
if (HERMES_ENABLE_STDIO_ADAPTER)
    add_subdirectory(stdio)
endif()
if (HERMES_ENABLE_MPIIO_ADAPTER)
    add_subdirectory(mpiio)
endif()
if (HERMES_ENABLE_HDF5_ADAPTER)
    add_subdirectory(vfd)
endif()

#-----------------------------------------------------------------------------
# Install HRUN Admin Task Library Headers
#-----------------------------------------------------------------------------
install(DIRECTORY include DESTINATION ${CMAKE_INSTALL_PREFIX})
