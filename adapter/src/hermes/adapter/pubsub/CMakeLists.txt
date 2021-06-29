project(StdioAdapter VERSION 0.1.0)

# include directories for stdio headers
include_directories(${CMAKE_SOURCE_DIR}/adapter/include)

# Only stdio.h is the public adapter.
set(PUBSUB_ADAPTER_PUBLIC_HEADER ${CMAKE_SOURCE_DIR}/adapter/include/hermes/adapter/pubsub.h)
# Private headers
set(PUBSUB_ADAPTER_PRIVATE_HEADER ${CMAKE_CURRENT_SOURCE_DIR}/metadata_manager.h
        ${CMAKE_CURRENT_SOURCE_DIR}/common/datastructures.h
        ${CMAKE_CURRENT_SOURCE_DIR}/common/constants.h
        ${CMAKE_SOURCE_DIR}/adapter/src/hermes/adapter/singleton.h)

# STDIO src code. We only include stdio.cc as it includes other cc to reduce compilation time.
set(PUBSUB_ADAPTER_SRC pubsub.cc ${CMAKE_CURRENT_SOURCE_DIR}/metadata_manager.cc)

# Add library hermes_stdio
add_library(hermes_pubsub SHARED ${PUBSUB_ADAPTER_PRIVATE_HEADER} ${PUBSUB_ADAPTER_PUBLIC_HEADER} ${PUBSUB_ADAPTER_SRC})
add_dependencies(hermes_pubsub hermes)
target_link_libraries(hermes_pubsub hermes MPI::MPI_CXX)

#-----------------------------------------------------------------------------
# Add Target(s) to CMake Install
#-----------------------------------------------------------------------------
install(
        TARGETS
        hermes_pubsub
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
    set_coverage_flags(hermes_pubsub)
endif()