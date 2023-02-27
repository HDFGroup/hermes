include_directories(
        ${CMAKE_SOURCE_DIR}
        ${HERMES_SRC_DIR}
        ${HERMES_ADAPTER_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR})

# Creates the POSIX I/O client and singleton
add_library(hermes_posix_io_client
        ${CMAKE_CURRENT_SOURCE_DIR}/posix_api_singleton.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/posix_io_client.cc)
target_compile_options(hermes_posix_io_client PUBLIC -fPIC)
add_dependencies(hermes_posix_io_client
        hermes_shm_data_structures)
target_link_libraries(hermes_posix_io_client
        hermes_shm_data_structures
        MPI::MPI_CXX glog::glog stdc++fs dl)

# Create the POSIX interceptor
set(INTERCEPTOR_DEPS
        hermes
        hermes_posix_io_client
        hermes_fs_base)
add_library(hermes_posix SHARED
        ${CMAKE_CURRENT_SOURCE_DIR}/posix_api.cc)
add_dependencies(hermes_posix ${INTERCEPTOR_DEPS})
target_link_libraries(hermes_posix ${INTERCEPTOR_DEPS})
target_compile_options(hermes_posix PUBLIC -fPIC)

#-----------------------------------------------------------------------------
# Add Target(s) to CMake Install
#-----------------------------------------------------------------------------
install(
        TARGETS
        hermes_posix_io_client
        hermes_posix
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
        hermes_posix_io_client
        hermes_posix
        ${HERMES_EXPORTED_LIBS})
if(NOT HERMES_EXTERNALLY_CONFIGURED)
EXPORT (
        TARGETS
        ${HERMES_EXPORTED_LIBS}
        FILE
        ${HERMES_EXPORTED_TARGETS}.cmake
)
endif()