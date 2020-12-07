#------------------------------------------------------------------------------
# Include source and build directories
#------------------------------------------------------------------------------
include_directories(
        ${CMAKE_SOURCE_DIR}/include
)

#------------------------------------------------------------------------------
# Posix Adapter tests
#------------------------------------------------------------------------------
add_executable(posix_adapter_test posix_adapter_test.cpp)
target_link_libraries(posix_adapter_test hermes_posix)
add_dependencies(posix_adapter_test hermes_posix)