include_directories(
        ${CMAKE_SOURCE_DIR}
        ${HERMES_SRC_DIR}
        ${HERMES_ADAPTER_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR})

# Create the POSIX Real API singleton
add_library(hermes_fs_mdm_singleton SHARED
        ${CMAKE_CURRENT_SOURCE_DIR}/fs_metadata_manager_singleton.cc)
add_dependencies(hermes_fs_mdm_singleton hermes)
target_link_libraries(hermes_fs_mdm_singleton
        MPI::MPI_CXX hermes)
target_compile_options(hermes_fs_mdm_singleton PUBLIC -fPIC)

#-----------------------------------------------------------------------------
# Add Target(s) to CMake Install
#-----------------------------------------------------------------------------
install(
        TARGETS
        hermes_fs_mdm_singleton
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
        hermes_fs_mdm_singleton
        ${HERMES_EXPORTED_LIBS})
if(NOT HERMES_EXTERNALLY_CONFIGURED)
EXPORT (
        TARGETS
        ${HERMES_EXPORTED_LIBS}
        FILE
        ${HERMES_EXPORTED_TARGETS}.cmake
)
endif()