

#------------------------------------------------------------------------------
# MPIIO Example tests
#------------------------------------------------------------------------------
add_executable(mpiio_adapter_test mpiio_adapter_test.cpp ${ADAPTER_COMMON})
target_link_libraries(mpiio_adapter_test Catch2::Catch2 -lstdc++fs -lc MPI::MPI_CXX)
mpi(mpiio_adapter_test 2 "")

add_executable(hermes_mpiio_adapter_test mpiio_adapter_test.cpp ${ADAPTER_COMMON})
target_link_libraries(hermes_mpiio_adapter_test Catch2::Catch2 -lstdc++fs -lc MPI::MPI_CXX)
target_link_libraries(hermes_mpiio_adapter_test hermes_mpiio)
add_dependencies(hermes_mpiio_adapter_test hermes_mpiio)
add_dependencies(hermes_mpiio_adapter_test hermes_daemon)
set_target_properties(hermes_mpiio_adapter_test PROPERTIES COMPILE_FLAGS "-DHERMES_INTERCEPT=1")

gcc_hermes_mode(hermes_mpiio_adapter_test "open" "[operation=single_open]" "DEFAULT" "1")
mpi_daemon(hermes_mpiio_adapter_test 2 "[operation=single_open]" "open" 1)