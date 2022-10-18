project(PosixAdapter VERSION ${HERMES_PACKAGE_VERSION})
include_directories(${HERMES_ADAPTER_DIR} ${CMAKE_CURRENT_SOURCE_DIR})

# POSIX src code. We only include posix.cc as it includes other cc to reduce compilation time.
set(POSIX_ADAPTER_SRC posix.cc)

set(HERMES_POSIX_ADAPTER_DIR ${HERMES_ADAPTER_DIR}/posix)

# Only posix.h is the public adapter.
set(POSIX_ADAPTER_PUBLIC_HEADER
        ${HERMES_POSIX_ADAPTER_DIR}/real_api.h
        ${HERMES_POSIX_ADAPTER_DIR}/fs_api.h)

# Add library hermes_posix_backend
add_library(hermes_posix_backend ${CMAKE_CURRENT_SOURCE_DIR}/fs_api.cc)
target_compile_options(hermes_posix_backend PUBLIC -fPIC)
add_dependencies(hermes_posix_backend hermes)
target_link_libraries(hermes_posix_backend hermes MPI::MPI_CXX glog::glog stdc++fs dl)

# Add library hermes_posix
add_library(hermes_posix SHARED ${POSIX_ADAPTER_PUBLIC_HEADER} ${POSIX_ADAPTER_SRC})
add_dependencies(hermes_posix hermes_posix_backend)
target_link_libraries(hermes_posix hermes_posix_backend)

#-----------------------------------------------------------------------------
# Add Target(s) to CMake Install
#-----------------------------------------------------------------------------
install(
        TARGETS
        hermes_posix_backend
        EXPORT
        ${HERMES_EXPORTED_TARGETS}
        LIBRARY DESTINATION ${HERMES_INSTALL_LIB_DIR}
        ARCHIVE DESTINATION ${HERMES_INSTALL_LIB_DIR}
        RUNTIME DESTINATION ${HERMES_INSTALL_BIN_DIR}
)
install(
        TARGETS
        hermes_posix
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
    set_coverage_flags(hermes_posix)
endif()
