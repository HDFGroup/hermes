set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DHERMES_PRELOAD -DHERMES_RPC_THALLIUM")
set(ADAPTER_COMMON ${CMAKE_CURRENT_SOURCE_DIR}/catch_config.h)
set(HERMES_ADAPTER_TEST_DIR ${HERMES_ADAPTER_DIR}/test)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

find_package(Catch2 REQUIRED)
find_program(BASH_PROGRAM bash)
find_package(OpenMP REQUIRED COMPONENTS C CXX)

enable_testing()

if(HERMES_ENABLE_POSIX_ADAPTER)
    add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/posix)
endif()

if(HERMES_ENABLE_STDIO_ADAPTER)
  add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/stdio)
endif()

if(HERMES_ENABLE_MPIIO_ADAPTER)
  add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/mpiio)
endif()

if(HERMES_ENABLE_PUBSUB_ADAPTER)
  # add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/pubsub)
endif()

if(HERMES_ENABLE_VFD)
  add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/vfd)
endif()