project(StdioAdapter VERSION 0.1.0)

# STDIO src code. We only include stdio.cc as it includes other cc to reduce compilation time.
set(STDIO_ADAPTER_SRC stdio.cc)

set(HERMES_STDIO_ADAPTER_DIR ${HERMES_ADAPTER_DIR}/stdio)

# Only stdio.h is the public adapter.
set(STDIO_ADAPTER_PUBLIC_HEADER ${HERMES_STDIO_ADAPTER_DIR}/stdio.h)
# Private headers
set(STDIO_ADAPTER_PRIVATE_HEADER ${CMAKE_CURRENT_SOURCE_DIR}/metadata_manager.h
        ${CMAKE_CURRENT_SOURCE_DIR}/mapper/mapper_factory.h
        ${CMAKE_CURRENT_SOURCE_DIR}/mapper/abstract_mapper.h
        ${CMAKE_CURRENT_SOURCE_DIR}/mapper/balanced_mapper.h
        ${CMAKE_CURRENT_SOURCE_DIR}/datastructures.h
        ${CMAKE_CURRENT_SOURCE_DIR}/enumerations.h
        ${CMAKE_CURRENT_SOURCE_DIR}/constants.h)

# Add library hermes_stdio
add_library(hermes_stdio SHARED ${STDIO_ADAPTER_PRIVATE_HEADER} ${STDIO_ADAPTER_PUBLIC_HEADER} ${STDIO_ADAPTER_SRC})
add_dependencies(hermes_stdio hermes)
target_link_libraries(hermes_stdio hermes MPI::MPI_CXX)

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
