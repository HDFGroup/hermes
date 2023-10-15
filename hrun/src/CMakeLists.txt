#------------------------------------------------------------------------------
# External dependencies
#------------------------------------------------------------------------------
# None for now

#------------------------------------------------------------------------------
# Build Hrun Client Library
#------------------------------------------------------------------------------
add_library(hrun_client SHARED
        ${CMAKE_CURRENT_SOURCE_DIR}/config_client.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/config_server.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/hrun_client.cc)
target_link_libraries(hrun_client
  ${HermesShm_LIBRARIES}
  yaml-cpp
  cereal::cereal
  thallium
  -ldl -lrt -lc -pthread)

#------------------------------------------------------------------------------
# Build Hrun Runtime Library
#------------------------------------------------------------------------------
add_library(hrun_runtime
        worker.cc
        hrun_runtime.cc)
add_dependencies(hrun_runtime ${Hermes_CLIENT_DEPS})
target_link_libraries(hrun_runtime thallium ${Hermes_CLIENT_LIBRARIES})

#------------------------------------------------------------------------------
# Build Hrun Runtime Start Function
#------------------------------------------------------------------------------
add_executable(hrun_start_runtime
        hrun_start_runtime.cc)
add_dependencies(hrun_start_runtime ${Hermes_RUNTIME_DEPS})
target_link_libraries(hrun_start_runtime ${Hermes_RUNTIME_LIBRARIES})

#------------------------------------------------------------------------------
# Build HRUN Runtime Stop Function
#------------------------------------------------------------------------------
add_executable(hrun_stop_runtime hrun_stop_runtime.cc)
add_dependencies(hrun_stop_runtime ${Hermes_RUNTIME_DEPS})
target_link_libraries(hrun_stop_runtime ${Hermes_RUNTIME_LIBRARIES})

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
    hrun_client
    hrun_runtime
    hrun_start_runtime
    hrun_stop_runtime
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
set(HERMES_EXPORTED_LIBS
        hrun_client
        hrun_runtime
        hrun_start_runtime
        hrun_stop_runtime
        ${HERMES_EXPORTED_LIBS})
if(NOT HERMES_EXTERNALLY_CONFIGURED)
  EXPORT (
          TARGETS
          ${HERMES_EXPORTED_LIBS}
          FILE
          ${HERMES_EXPORTED_TARGETS}.cmake
  )
endif()

#------------------------------------------------------------------------------
# Coverage
#------------------------------------------------------------------------------
if(HERMES_ENABLE_COVERAGE)
  set_coverage_flags(hrun_client)
  set_coverage_flags(hrun_runtime)
  set_coverage_flags(hrun_start_runtime)
  set_coverage_flags(hrun_stop_runtime)
endif()