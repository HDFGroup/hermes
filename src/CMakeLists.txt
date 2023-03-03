include_directories(${CMAKE_SOURCE_DIR})

configure_file(hermes_version.h.in hermes_version.h)

# Create communication factory
set(CMAKE_HERMES_COMMUNICATION_TYPE HERMES_RPC_NONE)
if (HERMES_COMMUNICATION_MPI)
  set(CMAKE_HERMES_COMMUNICATION_TYPE HERMES_COMMUNICATION_MPI)
  set(CMAKE_HERMES_COMMUNICATION_TYPE_LIB MPI::MPI_CXX)
endif()
configure_file(communication_factory.h.in communication_factory.h)

# Create rpc factory
set(CMAKE_HERMES_COMMUNICATION_TYPE HERMES_RPC_NONE)
if (HERMES_RPC_THALLIUM)
  set(CMAKE_HERMES_RPC_TYPE HERMES_RPC_THALLIUM)
  set(CMAKE_HERMES_RPC_TYPE_LIB thallium)
endif()
configure_file(rpc_factory.h.in rpc_factory.h)

#------------------------------------------------------------------------------
# External dependencies
#------------------------------------------------------------------------------
# GNU Linear Programming Kit
find_package(GLPK REQUIRED)
if(GLPK_FOUND)
  include_directories(${GLPK_INCLUDE_DIRS})
  message(STATUS "Found GLPK")
  # set(HERMES_EXT_INCLUDE_DEPENDENCIES ${GLPK_INCLUDE_DIRS}
  #     ${HERMES_EXT_INCLUDE_DEPENDENCIES}
  # )
  set(HERMES_EXT_LIB_DEPENDENCIES
      ${GLPK_LIBRARIES} ${HERMES_EXT_LIB_DEPENDENCIES})
else()
  message(STATUS "GLPK not found")
endif()

#------------------------------------------------------------------------------
# Set sources
#------------------------------------------------------------------------------
set(HERMES_SRCS
  ${CMAKE_CURRENT_SOURCE_DIR}/api/hermes_singleton.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/config_client.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/config_server.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/hermes_types.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/status.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/utils.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/rpc.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/rpc_thallium.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/rpc_thallium_defs.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/metadata_manager.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/api/bucket.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/api/hermes.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/buffer_pool.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/buffer_organizer.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/data_placement_engine.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/dpe/random.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/dpe/round_robin.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/dpe/minimize_io_time.cc
        ../adapter/adapter_api.h)

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
target_include_directories(hermes
  SYSTEM PUBLIC ${HERMES_EXT_INCLUDE_DEPENDENCIES}
)
add_dependencies(hermes
        hermes_adapter_io_clients
        hermes_shm_data_structures)

target_link_libraries(hermes
  PUBLIC hermes_shm_data_structures
  PUBLIC hermes_adapter_io_clients
  PUBLIC ${GLPK_LIBRARIES}
  PUBLIC ${CMAKE_HERMES_COMMUNICATION_TYPE_LIB}
  PUBLIC ${CMAKE_HERMES_RPC_TYPE_LIB}
  PUBLIC glog::glog
  PUBLIC yaml-cpp
  PUBLIC hermes_shm_data_structures
  PUBLIC "$<$<BOOL:${HERMES_INTERCEPT_IO}>:${GOTCHA_MODULE_LIBS}>"
)

target_compile_definitions(hermes
  PRIVATE $<$<BOOL:${HERMES_ENABLE_TIMING}>:HERMES_ENABLE_TIMING>
)

hermes_set_lib_options(hermes "hermes" ${HERMES_LIBTYPE})

if(HERMES_ENABLE_COVERAGE)
  set_coverage_flags(hermes)
endif()

set(HERMES_EXPORTED_LIBS hermes ${HERMES_EXPORTED_LIBS})

# HERMES DAEMON
find_package(Catch2 REQUIRED)
add_executable(hermes_daemon api/hermes_daemon.cc)
add_dependencies(hermes_daemon hermes)
target_link_libraries(hermes_daemon hermes -ldl -lc MPI::MPI_CXX glog::glog)
target_compile_definitions(hermes_daemon
        PRIVATE $<$<BOOL:${HERMES_ENABLE_TIMING}>:HERMES_ENABLE_TIMING>)

add_executable(finalize_hermes api/finalize_hermes.cc)
add_dependencies(finalize_hermes hermes)
target_link_libraries(finalize_hermes hermes)

#-----------------------------------------------------------------------------
# Specify project header files to be installed
#-----------------------------------------------------------------------------
set(HERMES_HEADERS
  ${CMAKE_CURRENT_SOURCE_DIR}/api/bucket.h
  ${CMAKE_CURRENT_SOURCE_DIR}/api/hermes.h
  ${CMAKE_CURRENT_SOURCE_DIR}/buffer_pool.h
  ${CMAKE_CURRENT_SOURCE_DIR}/communication.h
  ${CMAKE_CURRENT_SOURCE_DIR}/data_placement_engine.h
  ${CMAKE_CURRENT_SOURCE_DIR}/hermes_types.h
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
message(${HERMES_EXPORTED_TARGETS})
install(
  TARGETS
    hermes_daemon
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