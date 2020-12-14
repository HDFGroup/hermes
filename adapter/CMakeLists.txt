include_directories(${CMAKE_SOURCE_DIR}/adapter/include)

add_subdirectory(src/hermes/adapter/posix)

if(CMAKE_PROJECT_NAME STREQUAL HERMES AND BUILD_TESTING)
    add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/test)
endif()
