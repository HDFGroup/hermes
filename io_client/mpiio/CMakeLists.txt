project(MPIIOAdapter VERSION ${HERMES_PACKAGE_VERSION})
# Creates the MPIIO I/O client and singleton
add_library(hermes_mpiio_io_client
        mpiio_api_singleton.cc
        mpiio_io_client.cc)
target_compile_options(hermes_mpiio_io_client PUBLIC -fPIC)
add_dependencies(hermes_mpiio_io_client
        hermes_shm_data_structures)
target_link_libraries(hermes_mpiio_io_client
        hermes_shm_data_structures MPI::MPI_CXX glog::glog stdc++fs dl)

#-----------------------------------------------------------------------------
# Add Target(s) to CMake Install
#-----------------------------------------------------------------------------
install(
        TARGETS
        hermes_mpiio_io_client
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
        hermes_mpiio_io_client
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
    set_coverage_flags(hermes_mpiio_io_client)
endif()