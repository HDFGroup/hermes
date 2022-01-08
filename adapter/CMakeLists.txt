# Set hermes to preload if adapter is linked.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DHERMES_PRELOAD -DHERMES_RPC_THALLIUM")

set(HERMES_ADAPTER_DIR ${CMAKE_SOURCE_DIR}/adapter)

add_subdirectory(stdio)
add_subdirectory(posix)
add_subdirectory(mpiio)
add_subdirectory(pubsub)

if(BUILD_TESTING)
  enable_testing()
  add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/test)
endif()