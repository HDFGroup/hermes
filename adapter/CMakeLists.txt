# Set hermes to preload if adapter is linked.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DHERMES_PRELOAD -DHERMES_RPC_THALLIUM")

set(HERMES_ADAPTER_DIR ${CMAKE_SOURCE_DIR}/adapter)
include_directories(${CMAKE_SOURCE_DIR}/src)

if(HERMES_ENABLE_POSIX_ADAPTER)
  add_subdirectory(posix)
endif()
