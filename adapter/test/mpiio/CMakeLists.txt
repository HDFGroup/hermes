

#------------------------------------------------------------------------------
# MPIIO Example tests
#------------------------------------------------------------------------------
add_executable(mpiio_adapter_test mpiio_adapter_test.cpp ${ADAPTER_COMMON})
target_link_libraries(mpiio_adapter_test Catch2::Catch2 -lstdc++fs -lc MPI::MPI_CXX)
mpi(mpiio_adapter_test 2 "")