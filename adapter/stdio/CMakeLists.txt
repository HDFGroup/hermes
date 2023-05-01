project(StdioAdapter VERSION ${HERMES_PACKAGE_VERSION})
include_directories(
        ${CMAKE_SOURCE_DIR}
        ${HERMES_SRC_DIR}
        ${HERMES_ADAPTER_DIR}
        ${HERMES_IO_CLIENT_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR})

# Creates the STDIO I/O client
add_library(hermes_stdio_io_client stdio_io_client.cc)
add_dependencies(hermes_stdio_io_client
        hermes hermes_fs_base)
target_link_libraries(hermes_stdio_io_client
        hermes hermes_fs_base
        MPI::MPI_CXX stdc++fs dl)

# Create the STDIO interceptor
set(INTERCEPTOR_DEPS
        hermes
        hermes_stdio_io_client)
add_library(hermes_stdio SHARED
        ${CMAKE_CURRENT_SOURCE_DIR}/stdio_api.cc)
add_dependencies(hermes_stdio ${INTERCEPTOR_DEPS})
target_link_libraries(hermes_stdio ${INTERCEPTOR_DEPS})

#-----------------------------------------------------------------------------
# Add Target(s) to CMake Install
#-----------------------------------------------------------------------------
install(
        TARGETS
        hermes_stdio_io_client
        hermes_stdio
        EXPORT
        ${HERMES_EXPORTED_TARGETS}
        LIBRARY DESTINATION ${HERMES_INSTALL_LIB_DIR}
        ARCHIVE DESTINATION ${HERMES_INSTALL_LIB_DIR}
        RUNTIME DESTINATION ${HERMES_INSTALL_BIN_DIR}
)

#-----------------------------------------------------------------------------
# Export all exported targets to the build tree for use by parent project
#-----------------------------------------------------------------------------
set(HERMES_EXPORTED_LIBS
        hermes_stdio_io_client
        hermes_stdio
        ${HERMES_EXPORTED_LIBS})
if(NOT HERMES_EXTERNALLY_CONFIGURED)
    EXPORT (
            TARGETS
            ${HERMES_EXPORTED_LIBS}
            FILE
            ${HERMES_EXPORTED_TARGETS}.cmake
    )
endif()

#-----------------------------------------------------------------------------
# Add Target(s) to Coverage
#-----------------------------------------------------------------------------
if(HERMES_ENABLE_COVERAGE)
    set_coverage_flags(hermes_stdio_io_client)
    #set_coverage_flags(hermes_stdio)
endif()
