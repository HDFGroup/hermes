#------------------------------------------------------------------------------
# MPIIO Example tests
#------------------------------------------------------------------------------
add_executable(mpiio_adapter_test mpiio_adapter_test.cpp ${ADAPTER_COMMON})
target_include_directories(mpiio_adapter_test PUBLIC ${HERMES_ADAPTER_TEST_DIR})
target_link_libraries(mpiio_adapter_test Catch2::Catch2 -lstdc++fs -lc MPI::MPI_CXX)
mpi(mpiio_adapter_test 2 "")

add_executable(hermes_mpiio_adapter_test mpiio_adapter_test.cpp ${ADAPTER_COMMON})
target_include_directories(hermes_mpiio_adapter_test PUBLIC ${HERMES_ADAPTER_TEST_DIR})
target_link_libraries(hermes_mpiio_adapter_test Catch2::Catch2 -lstdc++fs -lc MPI::MPI_CXX)
target_link_libraries(hermes_mpiio_adapter_test hermes_mpiio)
add_dependencies(hermes_mpiio_adapter_test hermes_mpiio)
add_dependencies(hermes_mpiio_adapter_test hermes_daemon)
set_target_properties(hermes_mpiio_adapter_test PROPERTIES COMPILE_FLAGS "-DHERMES_INTERCEPT=1")

mpi_daemon(hermes_mpiio_adapter_test 2 "[synchronicity=async]" "async" 1)
mpi_daemon(hermes_mpiio_adapter_test 2 "[synchronicity=sync]" "sync" 1)
