project(UNIT_TESTS)
cmake_minimum_required (VERSION 3.10)

set(CMAKE_MESSAGE_LOG_LEVEL "VERBOSE")
find_package(Hermes CONFIG REQUIRED)
message("Found Hermes at ${Hermes_INCLUDE_DIR} ${Hermes_LIBRARIES}")
add_executable(test_hermes_install  test_install.cc)
target_link_libraries(test_hermes_install ${Hermes_LIBRARIES})