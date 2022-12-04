#------------------------------------------------------------------------------
# Include source and build directories
#------------------------------------------------------------------------------
include_directories(
  ${PROJECT_SOURCE_DIR}/src/api
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${HERMES_INCLUDES_BUILD_TIME}
)

#------------------------------------------------------------------------------
# Hermes Wrapper
#------------------------------------------------------------------------------
add_library(hermes_wrapper hermes_wrapper.cpp)
target_link_libraries(hermes_wrapper ${LIBRT} hermes)

set(HERMES_EXPORTED_LIBS hermes_wrapper ${HERMES_EXPORTED_LIBS})

#-----------------------------------------------------------------------------
# Add file(s) to CMake Install
#-----------------------------------------------------------------------------
install(
  FILES
    hermes_wrapper.h
  DESTINATION
    ${HERMES_INSTALL_INCLUDE_DIR}
  COMPONENT
    headers
)

#-----------------------------------------------------------------------------
# Add Target(s) to CMake Install
#-----------------------------------------------------------------------------
install(
  TARGETS
    hermes_wrapper
  EXPORT
    ${HERMES_EXPORTED_TARGETS}
  LIBRARY DESTINATION ${HERMES_INSTALL_LIB_DIR}
  ARCHIVE DESTINATION ${HERMES_INSTALL_LIB_DIR}
  RUNTIME DESTINATION ${HERMES_INSTALL_BIN_DIR}
)

#-----------------------------------------------------------------------------
# Export all exported targets to the build tree for use by parent project
#-----------------------------------------------------------------------------
if(NOT HERMES_EXTERNALLY_CONFIGURED)
EXPORT (
  TARGETS
    ${HERMES_EXPORTED_LIBS}
  FILE
    ${HERMES_EXPORTED_TARGETS}.cmake
)
endif()

#------------------------------------------------------------------------------
# Set variables for parent scope
#------------------------------------------------------------------------------
# Used by config.cmake.build.in and Testing
set(HERMES_INCLUDES_BUILD_TIME
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}
  ${HERMES_EXT_INCLUDE_DEPENDENCIES}
  PARENT_SCOPE
)

# Used by config.cmake.install.in
set(HERMES_INCLUDES_INSTALL_TIME
  ${HERMES_INSTALL_INCLUDE_DIR}
  ${HERMES_EXT_INCLUDE_DEPENDENCIES}
  PARENT_SCOPE
)
