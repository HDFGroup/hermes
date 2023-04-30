# Set hermes to preload if adapter is linked.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DHERMES_PRELOAD -DHERMES_RPC_THALLIUM")

set(HERMES_SRC_DIR ${CMAKE_SOURCE_DIR}/src)
set(HERMES_ADAPTER_DIR ${CMAKE_SOURCE_DIR}/adapter)
set(HERMES_IO_CLIENT_DIR ${CMAKE_SOURCE_DIR}/io_client)

include_directories(
        ${CMAKE_SOURCE_DIR}
        ${HERMES_SRC_DIR}
        ${HERMES_ADAPTER_DIR}
        ${HERMES_IO_CLIENT_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR})

add_subdirectory(filesystem)
if(HERMES_ENABLE_POSIX_ADAPTER)
  add_subdirectory(posix)
endif()
if(HERMES_ENABLE_STDIO_ADAPTER)
  add_subdirectory(stdio)
endif()
if(HERMES_ENABLE_MPIIO_ADAPTER)
  add_subdirectory(mpiio)
endif()
if(HERMES_ENABLE_VFD)
  add_subdirectory(vfd)
endif()
if(HERMES_ENABLE_KVSTORE)
  add_subdirectory(kvstore)
endif()
if(HERMES_BUILD_ADAPTER_TESTS)
  add_subdirectory(test)
endif()