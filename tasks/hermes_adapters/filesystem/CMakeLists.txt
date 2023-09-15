include_directories(
        ${CMAKE_SOURCE_DIR}
        ${HERMES_SRC_DIR}
        ${HERMES_ADAPTER_DIR}
        ${HERMES_IO_CLIENT_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR})

# Create the metadata manager singleton + FS base class
add_library(hermes_fs_base SHARED
        ${CMAKE_CURRENT_SOURCE_DIR}/filesystem.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/filesystem_mdm.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/filesystem_mdm_singleton.cc)
add_dependencies(hermes_fs_base
        hermes)
target_link_libraries(hermes_fs_base
        MPI::MPI_CXX
        hermes)

#-----------------------------------------------------------------------------
# Add Target(s) to CMake Install
#-----------------------------------------------------------------------------
install(
        TARGETS
        hermes_fs_base
        EXPORT
        ${LABSTOR_EXPORTED_TARGETS}
        LIBRARY DESTINATION ${LABSTOR_INSTALL_LIB_DIR}
        ARCHIVE DESTINATION ${LABSTOR_INSTALL_LIB_DIR}
        RUNTIME DESTINATION ${LABSTOR_INSTALL_BIN_DIR}
)
install(
        FILES
        filesystem_io_client.h
        DESTINATION
        ${LABSTOR_INSTALL_INCLUDE_DIR}
        COMPONENT
        headers
)

#-----------------------------------------------------------------------------
# Export all exported targets to the build tree for use by parent project
#-----------------------------------------------------------------------------
set(LABSTOR_EXPORTED_LIBS
        hermes_fs_base
        ${LABSTOR_EXPORTED_LIBS})
if(NOT LABSTOR_EXTERNALLY_CONFIGURED)
EXPORT (
        TARGETS
        ${LABSTOR_EXPORTED_LIBS}
        FILE
        ${LABSTOR_EXPORTED_TARGETS}.cmake
)
endif()

#-----------------------------------------------------------------------------
# Add Target(s) to Coverage
#-----------------------------------------------------------------------------
if(LABSTOR_ENABLE_COVERAGE)
    set_coverage_flags(hermes_fs_base)
endif()
