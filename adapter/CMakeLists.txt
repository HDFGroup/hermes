# Set hermes to preload if adapter is linked.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DHERMES_PRELOAD -DHERMES_RPC_THALLIUM")

set(HERMES_SRC_DIR ${CMAKE_SOURCE_DIR}/src)
set(HERMES_ADAPTER_DIR ${CMAKE_SOURCE_DIR}/adapter)
include_directories(${CMAKE_SOURCE_DIR} HERMES_SRC_DIR HERMES_ADAPTER_DIR)

if(HERMES_ENABLE_POSIX_ADAPTER)
  add_subdirectory(io_client)
  add_subdirectory(filesystem)
  add_subdirectory(posix)
  add_subdirectory(test)
endif()

# Transparently Start Hermes in Client Mode
add_library(adapter_init ${CMAKE_CURRENT_SOURCE_DIR}/adapter_init.cc)
add_dependencies(adapter_init hermes)
target_link_libraries(adapter_init hermes)

#-----------------------------------------------------------------------------
# Add Target(s) to CMake Install
#-----------------------------------------------------------------------------
install(
        TARGETS
        adapter_init
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
        adapter_init
        ${HERMES_EXPORTED_LIBS})
if(NOT HERMES_EXTERNALLY_CONFIGURED)
  EXPORT (
          TARGETS
          ${HERMES_EXPORTED_LIBS}
          FILE
          ${HERMES_EXPORTED_TARGETS}.cmake
  )
endif()