include_directories(${CMAKE_SOURCE_DIR}/adapter/include)

add_subdirectory(src/hermes/adapter/posix)
add_subdirectory(src/hermes/adapter/stdio)

if(BUILD_TESTING)
    enable_testing()
    add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/test)
endif()
