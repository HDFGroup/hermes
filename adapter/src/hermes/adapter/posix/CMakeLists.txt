project(PosixAdapter VERSION 0.1.0)

# include directories for posix headers
include_directories(${CMAKE_SOURCE_DIR}/adapter/include)

# POSIX src code. We only include posix.cc as it includes other cc to reduce compilation time.
set(POSIX_ADAPTER_SRC posix.cc)

# Only posix.h is the public adapter.
set(POSIX_ADAPTER_PUBLIC_HEADER ${CMAKE_SOURCE_DIR}/adapter/include/hermes/adapter/posix.h)
# Private headers
set(POSIX_ADAPTER_PRIVATE_HEADER ${CMAKE_CURRENT_SOURCE_DIR}/metadata_manager.h
        ${CMAKE_CURRENT_SOURCE_DIR}/mapper/mapper_factory.h
        ${CMAKE_CURRENT_SOURCE_DIR}/mapper/abstract_mapper.h
        ${CMAKE_CURRENT_SOURCE_DIR}/mapper/balanced_mapper.h
        ${CMAKE_CURRENT_SOURCE_DIR}/common/datastructures.h
        ${CMAKE_CURRENT_SOURCE_DIR}/common/enumerations.h
        ${CMAKE_CURRENT_SOURCE_DIR}/common/constants.h)

# Add library hermes_posix
add_library(hermes_posix SHARED ${POSIX_ADAPTER_PRIVATE_HEADER} ${POSIX_ADAPTER_PUBLIC_HEADER} ${POSIX_ADAPTER_SRC})
add_dependencies(hermes_posix hermes)
target_link_libraries(hermes_posix hermes MPI::MPI_CXX)

#-----------------------------------------------------------------------------
# Add Target(s) to CMake Install
#-----------------------------------------------------------------------------
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
