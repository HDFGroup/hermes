#-----------------------------------------------------------------------------
# Build KVStore adapter
#-----------------------------------------------------------------------------
include_directories(
        ${CMAKE_SOURCE_DIR}
        ${HERMES_SRC_DIR}
        ${HERMES_ADAPTER_DIR}
        ${HERMES_IO_CLIENT_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR})

# Create KVStore Adapter
set(INTERCEPTOR_DEPS hermes)
add_library(hermes_kvstore SHARED
        ${CMAKE_CURRENT_SOURCE_DIR}/kvstore.cc)
add_dependencies(hermes_kvstore ${INTERCEPTOR_DEPS})
target_link_libraries(hermes_kvstore ${INTERCEPTOR_DEPS})

#-----------------------------------------------------------------------------
# Add Target(s) to CMake Install
#-----------------------------------------------------------------------------
install(
        TARGETS
        hermes_kvstore
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
        hermes_kvstore
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
    set_coverage_flags(hermes_kvstore)
endif()
