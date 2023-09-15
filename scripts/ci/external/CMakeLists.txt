cmake_minimum_required(VERSION 3.10)
project(hermes_shm)

set(CMAKE_CXX_STANDARD 17)

#------------------------------------------------------------------------------
# External libraries
#------------------------------------------------------------------------------

find_package(HermesShm CONFIG REQUIRED)
message(STATUS "found hermes_shm at ${HermesShm_DIR}")

#------------------------------------------------------------------------------
# Build
#------------------------------------------------------------------------------

add_executable(test test.cc)
target_link_libraries(test ${HermesShm_LIBRARIES})