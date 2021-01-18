function(gcc_hermes exec tag_name tags)
    add_test(NAME Test${exec}_${tag_name} COMMAND "${CMAKE_BINARY_DIR}/bin/${exec}" ${tags} -s --reporter compact)
    set_property(TEST Test${exec}_${tag_name}
            PROPERTY ENVIRONMENT HERMES_CONF=${CMAKE_SOURCE_DIR}/adapter/test/data/hermes.conf)
    set_property(TEST Test${exec}_${tag_name} APPEND
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
gcc_hermes(hermes_stdio_adapter_test batched_write "[operation=batched_write]")
gcc_hermes(hermes_stdio_adapter_test batched_update "[operation=batched_update]")
gcc_hermes(hermes_stdio_adapter_test single_read "[operation=single_read]")
gcc_hermes(hermes_stdio_adapter_test batched_read "[operation=batched_read]")
gcc_hermes(hermes_stdio_adapter_test single_mixed "[operation=single_mixed]")
gcc_hermes(hermes_stdio_adapter_test batched_mixed "[operation=batched_mixed]")

add_executable(stdio_adapter_mpi_test stdio_adapter_mpi_test.cpp ${ADAPTER_COMMON})
target_link_libraries(stdio_adapter_mpi_test Catch2::Catch2 -lstdc++fs -lc MPI::MPI_CXX)
add_dependencies(stdio_adapter_mpi_test hermes_stdio)
mpi(stdio_adapter_mpi_test 2 "")



