#------------------------------------------------------------------------------
# Include source and build directories
#------------------------------------------------------------------------------
include_directories(
        ${CMAKE_SOURCE_DIR}/adapter/include
)

find_package(Catch2 REQUIRED)

#------------------------------------------------------------------------------
# Posix Adapter tests
#------------------------------------------------------------------------------
add_executable(posix_adapter_test posix_adapter_test.cpp ${ADAPTER_COMMON})
target_link_libraries(posix_adapter_test hermes_posix)
target_link_libraries(posix_adapter_test Catch2::Catch2)
target_link_libraries(posix_adapter_test -lstdc++fs -lc)
add_dependencies(posix_adapter_test hermes_posix)
