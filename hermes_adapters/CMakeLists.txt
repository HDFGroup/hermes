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
if (HERMES_ENABLE_VFD)
    add_subdirectory(vfd)
endif()

#-----------------------------------------------------------------------------
# Install HRUN Admin Task Library Headers
#-----------------------------------------------------------------------------
file(GLOB_RECURSE HERMES_HEADERS "*.h")
install(
        FILES
        ${HERMES_HEADERS}
        DESTINATION
        ${CMAKE_INSTALL_PREFIX}/include/hermes_adapters
        COMPONENT
        headers
)
install(DIRECTORY mapper DESTINATION ${CMAKE_INSTALL_PREFIX}/include/hermes_adapters)