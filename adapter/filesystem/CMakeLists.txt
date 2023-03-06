include_directories(
        ${CMAKE_SOURCE_DIR}
        ${HERMES_SRC_DIR}
        ${HERMES_ADAPTER_DIR}
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
target_compile_options(hermes_fs_base PUBLIC -fPIC)

#-----------------------------------------------------------------------------
# Add Target(s) to CMake Install
#-----------------------------------------------------------------------------
install(
        TARGETS
        hermes_fs_base
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
        hermes_fs_base
        ${HERMES_EXPORTED_LIBS})
if(NOT HERMES_EXTERNALLY_CONFIGURED)
EXPORT (
        TARGETS
        ${HERMES_EXPORTED_LIBS}
        FILE
        ${HERMES_EXPORTED_TARGETS}.cmake
)
endif()