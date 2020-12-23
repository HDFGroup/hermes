set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DHERMES_PRELOAD -DHERMES_RPC_THALLIUM")
include_directories(
        ${CMAKE_SOURCE_DIR}/adapter/include
        ${CMAKE_SOURCE_DIR}/adapter/src)

add_subdirectory(src/hermes/adapter/posix)
add_subdirectory(src/hermes/adapter/stdio)

if(BUILD_TESTING)
    enable_testing()
    add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/test)
endif()
