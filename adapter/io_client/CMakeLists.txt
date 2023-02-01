# Add library to wrap around all adapter native APIs for use internally in Hermes
add_library(hermes_adapter_native
        ${CMAKE_CURRENT_SOURCE_DIR}/empty.cc)
target_compile_options(hermes_adapter_native PUBLIC -fPIC)
add_dependencies(hermes_adapter_native
        hermes_posix_fs)
target_link_libraries(hermes_adapter_native
        hermes_posix_fs
        MPI::MPI_CXX glog::glog stdc++fs dl)

#-----------------------------------------------------------------------------
# Add Target(s) to CMake Install
#-----------------------------------------------------------------------------
install(
        TARGETS
        hermes_adapter_native
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
        hermes_adapter_native
        ${HERMES_EXPORTED_LIBS})
if(NOT HERMES_EXTERNALLY_CONFIGURED)
  EXPORT (
          TARGETS
          ${HERMES_EXPORTED_LIBS}
          FILE
          ${HERMES_EXPORTED_TARGETS}.cmake
  )
endif()