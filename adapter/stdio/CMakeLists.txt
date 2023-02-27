project(StdioAdapter VERSION ${HERMES_PACKAGE_VERSION})
include_directories(
        ${CMAKE_SOURCE_DIR}
        ${HERMES_SRC_DIR}
        ${HERMES_ADAPTER_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR})

# Creates the STDIO I/O client and singleton
add_library(hermes_stdio_io_client
        ${CMAKE_CURRENT_SOURCE_DIR}/stdio_api_singleton.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/stdio_io_client.cc)
target_compile_options(hermes_stdio_io_client PUBLIC -fPIC)
add_dependencies(hermes_stdio_io_client
        hermes_shm_data_structures)
target_link_libraries(hermes_stdio_io_client
        hermes_shm_data_structures MPI::MPI_CXX glog::glog stdc++fs dl)

# Create the STDIO interceptor
set(INTERCEPTOR_DEPS
        hermes
        hermes_stdio_io_client
        hermes_fs_base)
add_library(hermes_stdio SHARED
        ${CMAKE_CURRENT_SOURCE_DIR}/stdio_api.cc)
add_dependencies(hermes_stdio ${INTERCEPTOR_DEPS})
target_link_libraries(hermes_stdio ${INTERCEPTOR_DEPS})
target_compile_options(hermes_stdio PUBLIC -fPIC)

#-----------------------------------------------------------------------------
# Add Target(s) to CMake Install
#-----------------------------------------------------------------------------
install(
        TARGETS
        hermes_stdio_io_client
        EXPORT
        ${HERMES_EXPORTED_TARGETS}
        LIBRARY DESTINATION ${HERMES_INSTALL_LIB_DIR}
        ARCHIVE DESTINATION ${HERMES_INSTALL_LIB_DIR}
        RUNTIME DESTINATION ${HERMES_INSTALL_BIN_DIR}
)
install(
        TARGETS
        hermes_stdio
        EXPORT
        ${HERMES_EXPORTED_TARGETS}
        LIBRARY DESTINATION ${HERMES_INSTALL_LIB_DIR}
        ARCHIVE DESTINATION ${HERMES_INSTALL_LIB_DIR}
        RUNTIME DESTINATION ${HERMES_INSTALL_BIN_DIR}
)
#-----------------------------------------------------------------------------
# Add Target(s) to Coverage
#-----------------------------------------------------------------------------
if(HERMES_ENABLE_COVERAGE)
    set_coverage_flags(hermes_stdio)
endif()
