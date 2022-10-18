project(PubSubAdapter VERSION ${HERMES_PACKAGE_VERSION})

set(HERMES_PUBSUB_ADAPTER_DIR ${HERMES_ADAPTER_DIR}/pubsub)

set(PUBSUB_ADAPTER_PUBLIC_HEADER pubsub.h)
set(PUBSUB_ADAPTER_PRIVATE_HEADER ${CMAKE_CURRENT_SOURCE_DIR}/metadata_manager.h
        ${CMAKE_CURRENT_SOURCE_DIR}/datastructures.h
        ${HERMES_ADAPTER_DIR}/constants.h)
set(PUBSUB_ADAPTER_SRC pubsub.cc metadata_manager.cc)

add_library(hermes_pubsub SHARED ${PUBSUB_ADAPTER_PRIVATE_HEADER} ${PUBSUB_ADAPTER_PUBLIC_HEADER} ${PUBSUB_ADAPTER_SRC})
target_include_directories(hermes_pubsub PRIVATE ${HERMES_ADAPTER_DIR})
add_dependencies(hermes_pubsub hermes)
target_link_libraries(hermes_pubsub hermes MPI::MPI_CXX glog::glog)

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
