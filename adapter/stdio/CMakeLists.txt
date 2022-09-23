project(StdioAdapter VERSION ${HERMES_PACKAGE_VERSION})
include_directories(${HERMES_ADAPTER_DIR} ${CMAKE_CURRENT_SOURCE_DIR})

# STDIO src code. We only include stdio.cc as it includes other cc to reduce compilation time.
set(STDIO_ADAPTER_SRC stdio.cc)

set(HERMES_STDIO_ADAPTER_DIR ${HERMES_ADAPTER_DIR}/stdio)

# Only stdio.h is the public adapter.
set(STDIO_ADAPTER_PUBLIC_HEADER
        ${HERMES_STDIO_ADAPTER_DIR}/stdio.h
        ${HERMES_STDIO_ADAPTER_DIR}/native.h)

# Add library hermes_stdio
add_library(hermes_stdio_backend ${CMAKE_CURRENT_SOURCE_DIR}/native.cc)
add_dependencies(hermes_stdio_backend hermes)
target_link_libraries(hermes_stdio_backend hermes MPI::MPI_CXX glog::glog stdc++fs dl)

add_library(hermes_stdio SHARED ${STDIO_ADAPTER_PUBLIC_HEADER} ${STDIO_ADAPTER_SRC})
add_dependencies(hermes_stdio hermes_stdio_backend)
target_link_libraries(hermes_stdio hermes_stdio_backend)

#-----------------------------------------------------------------------------
# Add Target(s) to CMake Install
#-----------------------------------------------------------------------------
install(
        TARGETS
        hermes_stdio_backend
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
