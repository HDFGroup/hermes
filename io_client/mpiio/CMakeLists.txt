project(MPIIOAdapter VERSION ${HERMES_PACKAGE_VERSION})
# Create the MPIIO singleton
add_library(hermes_mpiio_real mpiio_api_singleton.cc)
target_link_libraries(hermes_mpiio_real
        MPI::MPI_CXX stdc++fs dl)
# Create the MPIIO I/O client
add_library(hermes_mpiio_io_client  mpiio_io_client.cc)
target_compile_options(hermes_mpiio_io_client PUBLIC -fPIC)
add_dependencies(hermes_mpiio_io_client
        hermes hermes_mpiio_real)
target_link_libraries(hermes_mpiio_io_client
        hermes hermes_mpiio_real MPI::MPI_CXX stdc++fs dl)

#-----------------------------------------------------------------------------
# Add Target(s) to CMake Install
#-----------------------------------------------------------------------------
install(
        TARGETS
        hermes_mpiio_real
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
        hermes_mpiio_real
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
    set_coverage_flags(hermes_mpiio_real)
    set_coverage_flags(hermes_mpiio_io_client)
endif()