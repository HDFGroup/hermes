include_directories(
        ${CMAKE_SOURCE_DIR}
        ${HERMES_IO_CLIENT_DIR})

#-----------------------------------------------------------------------------
# Compile all I/O clients into a single library
#-----------------------------------------------------------------------------
set(IO_CLIENT_LIBS
        hermes_stdio_io_client
        hermes_posix_io_client)
add_library(hermes_adapter_io_clients
        empty.cc)
target_compile_options(hermes_adapter_io_clients PUBLIC -fPIC)
add_dependencies(hermes_adapter_io_clients
        ${IO_CLIENT_LIBS})
target_link_libraries(hermes_adapter_io_clients
        ${IO_CLIENT_LIBS}
        MPI::MPI_CXX glog::glog stdc++fs dl)

#-----------------------------------------------------------------------------
# Add Target(s) to CMake Install
#-----------------------------------------------------------------------------
install(
        TARGETS
        hermes_adapter_io_clients
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
        hermes_adapter_io_clients
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
  set_coverage_flags(hermes_adapter_io_clients)
endif()