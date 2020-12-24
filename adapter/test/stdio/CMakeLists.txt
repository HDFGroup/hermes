function(gcc_hermes exec tag_name tags)
    add_test(NAME TestHermes_${exec}_${tag_name} COMMAND "${CMAKE_BINARY_DIR}/bin/${exec}" ${tags})
    set_property(TEST TestHermes_${exec}_${tag_name}
            PROPERTY ENVIRONMENT LD_PRELOAD=${CMAKE_BINARY_DIR}/bin/libhermes_stdio.so)
endfunction()
#------------------------------------------------------------------------------
# Posix Adapter Internal tests
#------------------------------------------------------------------------------
add_executable(stdio_adapter_mapper_test stdio_adapter_mapper_test.cpp ${ADAPTER_COMMON})
target_link_libraries(stdio_adapter_mapper_test
                        hermes_stdio Catch2::Catch2 -lstdc++fs -ldl -lc)
add_dependencies(stdio_adapter_mapper_test hermes_stdio)

#------------------------------------------------------------------------------
# Posix Adapter End to End tests
#------------------------------------------------------------------------------
add_executable(stdio_adapter_test stdio_adapter_test.cpp ${ADAPTER_COMMON})
target_link_libraries(stdio_adapter_test Catch2::Catch2 -lstdc++fs -lc)
add_dependencies(stdio_adapter_test hermes_stdio)
gcc(stdio_adapter_test "")
gcc_hermes(stdio_adapter_test open "[operation=single_open]")


add_executable(stdio_adapter_mpi_test stdio_adapter_mpi_test.cpp ${ADAPTER_COMMON})
target_link_libraries(stdio_adapter_mpi_test Catch2::Catch2 -lstdc++fs -lc MPI::MPI_CXX)
add_dependencies(stdio_adapter_mpi_test hermes_stdio)
mpi(stdio_adapter_mpi_test 2 "")



