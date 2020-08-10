#------------------------------------------------------------------------------
# Include source and build directories
#------------------------------------------------------------------------------
include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}/api
  ${CMAKE_CURRENT_SOURCE_DIR}/buffer_pool
  ${CMAKE_CURRENT_SOURCE_DIR}/io_clients
)

#------------------------------------------------------------------------------
# External dependencies
#------------------------------------------------------------------------------
# google ortools
find_package(ortools REQUIRED)
if(ORTOOLS_FOUND)
  include_directories(${ORTOOLS_INCLUDE_DIRS})
  message(STATUS "Found ortools")
  set(HERMES_EXT_INCLUDE_DEPENDENCIES ${ORTOOLS_INCLUDE_DIRS}
      ${HERMES_EXT_INCLUDE_DEPENDENCIES}
  )
  set(HERMES_EXT_LIB_DEPENDENCIES
      ${ORTOOLS_LIBRARIES} ${HERMES_EXT_LIB_DEPENDENCIES})
else()
  message(STATUS, "ortools not found")
endif()

#------------------------------------------------------------------------------
# Set sources
#------------------------------------------------------------------------------
set(HERMES_SRCS
  ${CMAKE_CURRENT_SOURCE_DIR}/api/bucket.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/api/hermes.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/api/vbucket.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/buffer_pool.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/data_placement_engine.cc
)

#------------------------------------------------------------------------------
# Libraries
#------------------------------------------------------------------------------
# HERMES
set(HERMES_BUILD_INCLUDE_DEPENDENCIES
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_SOURCE_DIR}/api
  ${CMAKE_CURRENT_BINARY_DIR}
)

add_library(hermes ${HERMES_SRCS})

target_include_directories(hermes
  PUBLIC "$<BUILD_INTERFACE:${HERMES_BUILD_INCLUDE_DEPENDENCIES}>"
          $<INSTALL_INTERFACE:${HERMES_INSTALL_INCLUDE_INTERFACE}>
)

target_link_libraries(hermes
  PUBLIC ${ORTOOLS_LIBRARIES}
  PRIVATE thallium
  PUBLIC "$<$<BOOL:${HERMES_INTERCEPT_IO}>:${GOTCHA_MODULE_LIBS}>"
  PRIVATE $<$<BOOL:${HERMES_COMMUNICATION_MPI}>:MPI::MPI_CXX>
)

target_compile_definitions(hermes
  PRIVATE $<$<BOOL:${HERMES_COMMUNICATION_MPI}>:HERMES_COMMUNICATION_MPI>
  PRIVATE $<$<BOOL:${HERMES_RPC_THALLIUM}>:HERMES_RPC_THALLIUM>
  PRIVATE $<$<BOOL:${HERMES_DEBUG_HEAP}>:HERMES_DEBUG_HEAP>
  PRIVATE $<$<BOOL:${HERMES_MDM_STORAGE_STBDS}>:HERMES_MDM_STORAGE_STBDS>
)

hermes_set_lib_options(hermes "hermes" ${HERMES_LIBTYPE})

set(HERMES_EXPORTED_LIBS hermes ${HERMES_EXPORTED_LIBS})

#-----------------------------------------------------------------------------
# Specify project header files to be installed
#-----------------------------------------------------------------------------
set(HERMES_HEADERS
  ${CMAKE_CURRENT_SOURCE_DIR}/api/bucket.h
  ${CMAKE_CURRENT_SOURCE_DIR}/api/hermes.h
  ${CMAKE_CURRENT_SOURCE_DIR}/api/id.h
  ${CMAKE_CURRENT_SOURCE_DIR}/api/vbucket.h
  ${CMAKE_CURRENT_SOURCE_DIR}/buffer_pool.h
  ${CMAKE_CURRENT_SOURCE_DIR}/buffer_pool_internal.h
  ${CMAKE_CURRENT_SOURCE_DIR}/communication.h
  ${CMAKE_CURRENT_SOURCE_DIR}/data_placement_engine.h
  ${CMAKE_CURRENT_SOURCE_DIR}/hermes_types.h
  ${CMAKE_CURRENT_SOURCE_DIR}/memory_management.h
  ${CMAKE_CURRENT_SOURCE_DIR}/utils.h
)

#-----------------------------------------------------------------------------
# Add file(s) to CMake Install
#-----------------------------------------------------------------------------
install(
  FILES
    ${HERMES_HEADERS}
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
    hermes
  EXPORT
    ${HERMES_EXPORTED_TARGETS}
  LIBRARY DESTINATION ${HERMES_INSTALL_LIB_DIR}
  ARCHIVE DESTINATION ${HERMES_INSTALL_LIB_DIR}
  RUNTIME DESTINATION ${HERMES_INSTALL_BIN_DIR}
)

#-----------------------------------------------------------------------------
# Add Target(s) to CMake Install for import into other projects
#-----------------------------------------------------------------------------
install(
  EXPORT
    ${HERMES_EXPORTED_TARGETS}
  DESTINATION
    ${HERMES_INSTALL_DATA_DIR}/cmake/hermes
  FILE
    ${HERMES_EXPORTED_TARGETS}.cmake
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
