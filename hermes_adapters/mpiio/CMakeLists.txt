include_directories(
        ${CMAKE_SOURCE_DIR}
        ${HERMES_SRC_DIR}
        ${HERMES_ADAPTER_DIR}
        ${HERMES_IO_CLIENT_DIR}
        .)

# Create the MPIIO interceptor
if (HERMES_MPICH)
    message(STATUS "Using HERMES_MPICH")
    add_definitions(-DHERMES_MPICH)
elseif(HERMES_OPENMPI)
    message(STATUS "Using HERMES_OPENMPI")
    add_definitions(-DHERMES_OPENMPI)
endif()
set(INTERCEPTOR_DEPS
        hermes hermes_fs_base)
add_library(hermes_mpiio SHARED
        ${CMAKE_CURRENT_SOURCE_DIR}/mpiio_api.cc)
add_dependencies(hermes_mpiio ${INTERCEPTOR_DEPS})
target_link_libraries(hermes_mpiio
        MPI::MPI_CXX stdc++fs ${libelf_LIBRARIES} dl ${INTERCEPTOR_DEPS})

#-----------------------------------------------------------------------------
# Add Target(s) to CMake Install
#-----------------------------------------------------------------------------
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
# Install headers
#-----------------------------------------------------------------------------
file(GLOB_RECURSE HERMES_HEADERS "*.h")
install(
        FILES
        ${HERMES_HEADERS}
        DESTINATION
        ${CMAKE_INSTALL_PREFIX}/include/hermes_adapters/stdio
        COMPONENT
        headers
)

#-----------------------------------------------------------------------------
# Add Target(s) to Coverage
#-----------------------------------------------------------------------------
if(HERMES_ENABLE_COVERAGE)
    set_coverage_flags(hermes_mpiio)
endif()