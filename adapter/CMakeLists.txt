# Set hermes to preload if adapter is linked.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DHERMES_PRELOAD -DHERMES_RPC_THALLIUM")

set(HERMES_SRC_DIR ${CMAKE_SOURCE_DIR}/src)
set(HERMES_ADAPTER_DIR ${CMAKE_SOURCE_DIR}/adapter)
include_directories(${CMAKE_SOURCE_DIR} HERMES_SRC_DIR HERMES_ADAPTER_DIR)

if(HERMES_ENABLE_POSIX_ADAPTER)
  add_subdirectory(io_client)
  add_subdirectory(filesystem)
  add_subdirectory(posix)
  add_subdirectory(stdio)
  add_subdirectory(mpiio)
  add_subdirectory(test)
endif()