# Set hermes to preload if adapter is linked.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DHERMES_PRELOAD -DHERMES_RPC_THALLIUM")

set(HERMES_ADAPTER_DIR ${CMAKE_SOURCE_DIR}/adapter)

if(HERMES_ENABLE_STDIO_ADAPTER)
  add_subdirectory(stdio)
endif()

if(HERMES_ENABLE_POSIX_ADAPTER)
  add_subdirectory(posix)
endif()

if(HERMES_ENABLE_MPIIO_ADAPTER)
  add_subdirectory(mpiio)
endif()

if(HERMES_ENABLE_PUBSUB_ADAPTER)
  add_subdirectory(pubsub)
endif()

if(HERMES_ENABLE_VFD)
  if(HERMES_ENABLE_WRAPPER)
    add_subdirectory(vfd)
  else()
    message(FATAL_ERROR "The Hermes VFD requires HERMES_ENABLE_WRAPPER=ON")
  endif()
endif()

if(BUILD_TESTING)
  enable_testing()
  add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/test)
endif()
