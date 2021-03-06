project(MPIIOAdapter VERSION 0.1.0)

# include directories for mpiio headers
include_directories(${CMAKE_SOURCE_DIR}/adapter/include)

# MPIIO src code. We only include mpiio.cc as it includes other cc to reduce compilation time.
set(MPIIO_ADAPTER_SRC mpiio.cc)

# Only mpiio.h is the public adapter.
set(MPIIO_ADAPTER_PUBLIC_HEADER ${CMAKE_SOURCE_DIR}/adapter/include/hermes/adapter/mpiio.h)
# Private headers
set(MPIIO_ADAPTER_PRIVATE_HEADER ${CMAKE_CURRENT_SOURCE_DIR}/metadata_manager.h
        ${CMAKE_CURRENT_SOURCE_DIR}/mapper/mapper_factory.h
        ${CMAKE_CURRENT_SOURCE_DIR}/mapper/abstract_mapper.h
        ${CMAKE_CURRENT_SOURCE_DIR}/mapper/balanced_mapper.h
        ${CMAKE_CURRENT_SOURCE_DIR}/common/datastructures.h
        ${CMAKE_CURRENT_SOURCE_DIR}/common/enumerations.h
        ${CMAKE_CURRENT_SOURCE_DIR}/common/constants.h)

set(MPIIO_INTERNAL_ADAPTER_SRC ${CMAKE_CURRENT_SOURCE_DIR}/metadata_manager.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/mapper/balanced_mapper.cc)

# Add library hermes_mpiio
add_library(hermes_mpiio SHARED ${MPIIO_ADAPTER_PRIVATE_HEADER} ${MPIIO_ADAPTER_PUBLIC_HEADER} ${MPIIO_ADAPTER_SRC})
add_dependencies(hermes_mpiio hermes)
target_link_libraries(hermes_mpiio hermes MPI::MPI_CXX)

add_library(hermes_mpiio_internal SHARED ${MPIIO_ADAPTER_PRIVATE_HEADER} ${MPIIO_ADAPTER_PUBLIC_HEADER} ${MPIIO_INTERNAL_ADAPTER_SRC})
add_dependencies(hermes_mpiio_internal hermes)
target_link_libraries(hermes_mpiio_internal hermes MPI::MPI_CXX)

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