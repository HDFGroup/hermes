cmake_minimum_required(VERSION 3.10)
project(hermes_shm)

set(CMAKE_CXX_STANDARD 17)

add_executable(test_data_structure_mpi_exec
        ${TEST_MAIN}/main_mpi.cc
        test_init.cc
        list_vec_mpi.cc
)

add_dependencies(test_data_structure_mpi_exec hermes_shm_data_structures)
target_link_libraries(test_data_structure_mpi_exec
        hermes_shm_data_structures Catch2::Catch2 MPI::MPI_CXX OpenMP::OpenMP_CXX)

# VECTOR TESTS
add_test(NAME test_vector_of_int_mpi COMMAND
        mpirun -n 4 ${CMAKE_CURRENT_BINARY_DIR}/test_data_structure_mpi_exec "VectorOfIntMpi")
add_test(NAME test_vector_of_string_mpi COMMAND
        mpirun -n 1  ${CMAKE_CURRENT_BINARY_DIR}/test_data_structure_mpi_exec "VectorOfStringMpi")

# LIST TESTS
add_test(NAME test_list_of_int_mpi COMMAND
        mpirun -n 4 ${CMAKE_CURRENT_BINARY_DIR}/test_data_structure_mpi_exec "ListOfIntMpi")
add_test(NAME test_list_of_string_mpi COMMAND
        mpirun -n 2 ${CMAKE_CURRENT_BINARY_DIR}/test_data_structure_mpi_exec "ListOfStringMpi")

message("mpirun -n 1 ${CMAKE_CURRENT_BINARY_DIR}/test_data_structure_mpi_exec ListOfStringMpi")

#------------------------------------------------------------------------------
# Code Coverage
#------------------------------------------------------------------------------
if(HERMES_ENABLE_COVERAGE)
    set_coverage_flags(test_data_structure_mpi_exec)
endif()