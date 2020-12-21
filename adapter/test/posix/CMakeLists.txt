#------------------------------------------------------------------------------
# Posix Adapter tests
#------------------------------------------------------------------------------
add_executable(posix_adapter_test posix_adapter_test.cpp ${ADAPTER_COMMON})
target_link_libraries(posix_adapter_test hermes_posix Catch2::Catch2 -lstdc++fs -lc)
add_dependencies(posix_adapter_test hermes_posix)
gcc(posix_adapter_test "")


add_executable(posix_adapter_mpi_test posix_adapter_mpi_test.cpp ${ADAPTER_COMMON})
target_link_libraries(posix_adapter_mpi_test hermes_posix Catch2::Catch2 -lstdc++fs -lc MPI::MPI_CXX)
add_dependencies(posix_adapter_mpi_test hermes_posix)
mpi(posix_adapter_mpi_test 2 "")

