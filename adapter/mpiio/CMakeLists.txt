project(MPIIOAdapter VERSION ${HERMES_PACKAGE_VERSION})
include_directories(${HERMES_ADAPTER_DIR} ${CMAKE_CURRENT_SOURCE_DIR})

# MPIIO src code. We only include mpiio.cc as it includes other cc to reduce compilation time.
set(MPIIO_ADAPTER_SRC ${CMAKE_CURRENT_SOURCE_DIR}/mpiio.cc)

set(HERMES_MPIIO_ADAPTER_DIR ${HERMES_ADAPTER_DIR}/mpiio)

# Public headers
set(MPIIO_ADAPTER_PUBLIC_HEADER
        ${HERMES_MPIIO_ADAPTER_DIR}/real_api.h
        ${HERMES_MPIIO_ADAPTER_DIR}/fs_api.h)

# Add library hermes_mpiio
add_library(hermes_mpiio_backend ${CMAKE_CURRENT_SOURCE_DIR}/fs_api.cc)
add_dependencies(hermes_mpiio_backend hermes)
target_link_libraries(hermes_mpiio_backend hermes MPI::MPI_CXX glog::glog stdc++fs)

add_library(hermes_mpiio SHARED ${MPIIO_ADAPTER_PUBLIC_HEADER} ${MPIIO_ADAPTER_SRC})
add_dependencies(hermes_mpiio hermes_mpiio_backend)
target_link_libraries(hermes_mpiio hermes_mpiio_backend)

#-----------------------------------------------------------------------------
# Add Target(s) to CMake Install
#-----------------------------------------------------------------------------
install(
        TARGETS
        hermes_mpiio_backend
        EXPORT
        ${HERMES_EXPORTED_TARGETS}
        LIBRARY DESTINATION ${HERMES_INSTALL_LIB_DIR}
        ARCHIVE DESTINATION ${HERMES_INSTALL_LIB_DIR}
        RUNTIME DESTINATION ${HERMES_INSTALL_BIN_DIR}
)
install(
        TARGETS
        hermes_mpiio
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
    set_coverage_flags(hermes_mpiio)
endif()
