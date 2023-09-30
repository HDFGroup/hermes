cmake_minimum_required(VERSION 3.10)
project(hermes)

set(CMAKE_CXX_STANDARD 17)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_SOURCE_DIR}/tasks/labstor_admin/include)
add_subdirectory(ipc)
add_subdirectory(hermes)
add_subdirectory(hermes_adapters)
add_subdirectory(boost)
