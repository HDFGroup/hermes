include_directories(
        ${CMAKE_SOURCE_DIR}
        ${HERMES_SRC_DIR}
        ${HERMES_ADAPTER_DIR}
        ${HERMES_IO_CLIENT_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR})

# Create the MPIIO I/O client
add_library(hermes_mpiio_io_client SHARED mpiio_io_client.cc)
add_dependencies(hermes_mpiio_io_client
        hermes hermes_fs_base)
target_link_libraries(hermes_mpiio_io_client
        hermes hermes_fs_base MPI::MPI_CXX stdc++fs dl)

# Create the MPIIO interceptor
set(INTERCEPTOR_DEPS
        hermes
        hermes_mpiio_io_client)
add_library(hermes_mpiio SHARED
        ${CMAKE_CURRENT_SOURCE_DIR}/mpiio_api.cc)
add_dependencies(hermes_mpiio ${INTERCEPTOR_DEPS})
target_link_libraries(hermes_mpiio ${INTERCEPTOR_DEPS})

#-----------------------------------------------------------------------------
# Add Target(s) to CMake Install
#-----------------------------------------------------------------------------
install(
        TARGETS
        hermes_mpiio_io_client
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
    set_coverage_flags(hermes_mpiio_io_client)
    #set_coverage_flags(hermes_mpiio)
endif()