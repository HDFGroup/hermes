project(StdioAdapter VERSION 0.1.0)

# include directories for stdio headers
include_directories(${CMAKE_SOURCE_DIR}/adapter/include)

# STDIO src code. We only include stdio.cc as it includes other cc to reduce compilation time.
set(STDIO_ADAPTER_SRC stdio.cc)

# Only stdio.h is the public adapter.
set(STDIO_ADAPTER_PUBLIC_HEADER ${CMAKE_SOURCE_DIR}/adapter/include/hermes/adapter/stdio.h)
# Private headers
set(STDIO_ADAPTER_PRIVATE_HEADER ${CMAKE_CURRENT_SOURCE_DIR}/metadata_manager.h
        ${CMAKE_CURRENT_SOURCE_DIR}/mapper/mapper_factory.h
        ${CMAKE_CURRENT_SOURCE_DIR}/mapper/abstract_mapper.h
        ${CMAKE_CURRENT_SOURCE_DIR}/mapper/balanced_mapper.h
        ${CMAKE_CURRENT_SOURCE_DIR}/common/datastructures.h
        ${CMAKE_CURRENT_SOURCE_DIR}/common/enumerations.h
        ${CMAKE_CURRENT_SOURCE_DIR}/common/constants.h)

set(STDIO_INTERNAL_ADAPTER_SRC ${CMAKE_CURRENT_SOURCE_DIR}/metadata_manager.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/mapper/balanced_mapper.cc)

# Add library hermes_stdio
add_library(hermes_stdio SHARED ${STDIO_ADAPTER_PRIVATE_HEADER} ${STDIO_ADAPTER_PUBLIC_HEADER} ${STDIO_ADAPTER_SRC})
add_dependencies(hermes_stdio hermes)
target_link_libraries(hermes_stdio hermes MPI::MPI_CXX)

add_library(hermes_stdio_internal SHARED ${STDIO_ADAPTER_PRIVATE_HEADER} ${STDIO_ADAPTER_PUBLIC_HEADER} ${STDIO_INTERNAL_ADAPTER_SRC})
add_dependencies(hermes_stdio_internal hermes)
target_link_libraries(hermes_stdio_internal hermes MPI::MPI_CXX)

#-----------------------------------------------------------------------------
# Add Target(s) to CMake Install
#-----------------------------------------------------------------------------
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