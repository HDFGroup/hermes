project(StdioAdapter VERSION ${HERMES_PACKAGE_VERSION})
include_directories(
        ${CMAKE_SOURCE_DIR}
        ${HERMES_SRC_DIR}
        ${HERMES_ADAPTER_DIR}
        ${HERMES_IO_CLIENT_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR})

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
