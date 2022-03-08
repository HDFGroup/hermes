project(MPIIOAdapter VERSION 0.4.0)

# MPIIO src code. We only include mpiio.cc as it includes other cc to reduce compilation time.
set(MPIIO_ADAPTER_SRC mpiio.cc)

set(HERMES_MPIIO_ADAPTER_DIR ${HERMES_ADAPTER_DIR}/mpiio)

# Only mpiio.h is the public adapter.
set(MPIIO_ADAPTER_PUBLIC_HEADER ${HERMES_MPIIO_ADAPTER_DIR}/mpiio.h)
# Private headers
set(MPIIO_ADAPTER_PRIVATE_HEADER ${CMAKE_CURRENT_SOURCE_DIR}/metadata_manager.h
        ${CMAKE_CURRENT_SOURCE_DIR}/mapper/mapper_factory.h
        ${CMAKE_CURRENT_SOURCE_DIR}/mapper/abstract_mapper.h
        ${CMAKE_CURRENT_SOURCE_DIR}/mapper/balanced_mapper.h
        ${CMAKE_CURRENT_SOURCE_DIR}/datastructures.h
        ${CMAKE_CURRENT_SOURCE_DIR}/enumerations.h
        ${CMAKE_CURRENT_SOURCE_DIR}/constants.h)

# Add library hermes_mpiio
add_library(hermes_mpiio SHARED ${MPIIO_ADAPTER_PRIVATE_HEADER} ${MPIIO_ADAPTER_PUBLIC_HEADER} ${MPIIO_ADAPTER_SRC})
target_include_directories(hermes_mpiio PRIVATE ${HERMES_ADAPTER_DIR})
add_dependencies(hermes_mpiio hermes)
target_link_libraries(hermes_mpiio hermes MPI::MPI_CXX stdc++fs)

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
# Add Target(s) to Coverage
#-----------------------------------------------------------------------------
if(HERMES_ENABLE_COVERAGE)
    set_coverage_flags(hermes_mpiio)
endif()
