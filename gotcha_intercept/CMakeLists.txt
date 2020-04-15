include_directories(${CMAKE_CURRENT_SOURCE_DIR})

if(HERMES_BUILD_POSIX_IO)
  add_library(gotcha_stdio gotcha_stdio.cc)
  hermes_set_lib_options(gotcha_stdio "gotcha_stdio" ${HERMES_LIBTYPE})
  list(APPEND GOTCHA_MODULE_LIBS gotcha_stdio)
  set(HERMES_EXPORTED_LIBS gotcha_stdio ${HERMES_EXPORTED_LIBS})
  # Add Target(s) to CMake Install
  install(
  TARGETS
    gotcha_stdio 
  EXPORT
    ${HERMES_EXPORTED_TARGETS}
  LIBRARY DESTINATION ${HERMES_INSTALL_LIB_DIR}
  ARCHIVE DESTINATION ${HERMES_INSTALL_LIB_DIR}
  RUNTIME DESTINATION ${HERMES_INSTALL_BIN_DIR}
)
endif()

if(HERMES_BUILD_POSIX_IO)
  add_library(gotcha_posix gotcha_posix.cc)
  hermes_set_lib_options(gotcha_posix "gotcha_posix" ${HERMES_LIBTYPE})
  list(APPEND GOTCHA_MODULE_LIBS gotcha_posix)
  set(HERMES_EXPORTED_LIBS gotcha_posix ${HERMES_EXPORTED_LIBS})
  # Add Target(s) to CMake Install
  install(
  TARGETS
    gotcha_posix
  EXPORT
    ${HERMES_EXPORTED_TARGETS}
  LIBRARY DESTINATION ${HERMES_INSTALL_LIB_DIR}
  ARCHIVE DESTINATION ${HERMES_INSTALL_LIB_DIR}
  RUNTIME DESTINATION ${HERMES_INSTALL_BIN_DIR}
  )
endif()

if(HERMES_BUILD_MPI_IO)
  add_library(gotcha_mpi_io gotcha_mpi_io.cc)
  hermes_set_lib_options(gotcha_mpi_io "gotcha_mpi_io" ${HERMES_LIBTYPE})
  list(APPEND GOTCHA_MODULE_LIBS gotcha_mpi_io)
  set(HERMES_EXPORTED_LIBS gotcha_mpi_io ${HERMES_EXPORTED_LIBS})
  # Add Target(s) to CMake Install
  install(
  TARGETS
    gotcha_mpi_io
  EXPORT
    ${HERMES_EXPORTED_TARGETS}
  LIBRARY DESTINATION ${HERMES_INSTALL_LIB_DIR}
  ARCHIVE DESTINATION ${HERMES_INSTALL_LIB_DIR}
  RUNTIME DESTINATION ${HERMES_INSTALL_BIN_DIR}
  )
endif()

list(APPEND GOTCHA_MODULE_LIBS ${GOTCHA_LIBRARIES})
set(GOTCHA_MODULE_LIBS ${GOTCHA_MODULE_LIBS} PARENT_SCOPE)

set(HERMES_EXPORTED_LIBS ${HERMES_EXPORTED_LIBS} PARENT_SCOPE)

#------------------------------------------------------------------------------
# Set variables for parent scope
#------------------------------------------------------------------------------
# Used by config.cmake.build.in and Testing
set(HERMES_INCLUDES_BUILD_TIME
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}
  $HERMES_EXT_INCLUDE_DEPENDENCIES}
  PARENT_SCOPE
)

# Used by config.cmake.install.in
set(HERMES_INCLUDES_INSTALL_TIME
  ${HERMES_INSTALL_INCLUDE_DIR}
  ${HERMES_EXT_INCLUDE_DEPENDENCIES}
  PARENT_SCOPE
)

