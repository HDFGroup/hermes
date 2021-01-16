function(gcc_hermes exec tag_name tags)
    add_test(NAME TestHermes_${exec}_${tag_name} COMMAND "${CMAKE_BINARY_DIR}/bin/${exec}" ${tags} -s --reporter compact)
    set_property(TEST TestHermes_${exec}_${tag_name} APPEND
            PROPERTY ENVIRONMENT LSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/test/data/asan.supp)
endfunction()
#------------------------------------------------------------------------------
# Posix Adapter Internal tests
#------------------------------------------------------------------------------
add_executable(stdio_adapter_mapper_test stdio_adapter_mapper_test.cpp ${ADAPTER_COMMON})
target_link_libraries(stdio_adapter_mapper_test
        hermes_stdio Catch2::Catch2 -lstdc++fs -ldl -lc MPI::MPI_CXX)
add_dependencies(stdio_adapter_mapper_test hermes_stdio)

#------------------------------------------------------------------------------
# Posix Adapter End to End tests
#------------------------------------------------------------------------------
add_executable(stdio_adapter_test stdio_adapter_test.cpp ${ADAPTER_COMMON})
target_link_libraries(stdio_adapter_test Catch2::Catch2 -lstdc++fs -lc MPI::MPI_CXX)
add_dependencies(stdio_adapter_test hermes_stdio)

add_executable(hermes_stdio_adapter_test stdio_adapter_test.cpp ${ADAPTER_COMMON})
target_link_libraries(hermes_stdio_adapter_test Catch2::Catch2 -lstdc++fs -lc MPI::MPI_CXX)
target_link_libraries(hermes_stdio_adapter_test hermes_stdio)
add_dependencies(hermes_stdio_adapter_test hermes_stdio)
gcc(stdio_adapter_test "")

gcc_hermes(hermes_stdio_adapter_test open "[operation=single_open]")
gcc_hermes(hermes_stdio_adapter_test single_write "[operation=single_write]")
gcc_hermes(hermes_stdio_adapter_test batched_fixed_write "[operation=batched_write][request_size=type-fixed]")

add_executable(stdio_adapter_mpi_test stdio_adapter_mpi_test.cpp ${ADAPTER_COMMON})
target_link_libraries(stdio_adapter_mpi_test Catch2::Catch2 -lstdc++fs -lc MPI::MPI_CXX)
add_dependencies(stdio_adapter_mpi_test hermes_stdio)
mpi(stdio_adapter_mpi_test 2 "")



