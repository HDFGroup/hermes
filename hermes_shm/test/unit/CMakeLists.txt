cmake_minimum_required(VERSION 3.10)
project(hermes_shm)

set(CMAKE_CXX_STANDARD 17)

set(TEST_MAIN ${CMAKE_CURRENT_SOURCE_DIR})

include_directories(${CMAKE_CURRENT_SOURCE_DIR})
add_subdirectory(data_structures)
add_subdirectory(allocators)
add_subdirectory(types)
add_subdirectory(thread)