project(UNIT_TESTS)
cmake_minimum_required (VERSION 3.10)

add_executable(test_hermes_install  test_install.cc)
target_link_libraries(test_hermes_install hermes)
