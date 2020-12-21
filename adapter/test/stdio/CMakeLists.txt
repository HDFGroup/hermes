#------------------------------------------------------------------------------
# Posix Adapter tests
#------------------------------------------------------------------------------
add_executable(stdio_adapter_test stdio_adapter_test.cpp ${ADAPTER_COMMON})
target_link_libraries(stdio_adapter_test hermes_stdio Catch2::Catch2 -lstdc++fs -lc)
add_dependencies(stdio_adapter_test hermes_stdio)
gcc(stdio_adapter_test "")


add_executable(stdio_adapter_mpi_test stdio_adapter_mpi_test.cpp ${ADAPTER_COMMON})
target_link_libraries(stdio_adapter_mpi_test hermes_stdio Catch2::Catch2 -lstdc++fs -lc MPI::MPI_CXX)
add_dependencies(stdio_adapter_mpi_test hermes_stdio)
mpi(stdio_adapter_mpi_test 2 "")

