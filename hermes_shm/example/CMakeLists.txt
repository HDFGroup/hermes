cmake_minimum_required(VERSION 3.10)
project(hermes_shm)

set(CMAKE_CXX_STANDARD 17)

set (EXAMPLES
        allocator
        vector
        list)
set (EXAMPLES)
foreach (EXAMPLE ${EXAMPLES})
    add_executable(example_${EXAMPLE}
            ${CMAKE_CURRENT_SOURCE_DIR}/${EXAMPLE}.cc)
    add_dependencies(example_${EXAMPLE} hermes_shm_data_structures)
    target_link_libraries(example_${EXAMPLE}
            hermes_shm_data_structures
            Catch2::Catch2
            MPI::MPI_CXX
            OpenMP::OpenMP_CXX)

    add_test(NAME test_example_${EXAMPLE} COMMAND
            mpirun -n 4 ${CMAKE_BINARY_DIR}/bin/example_${EXAMPLE})
endforeach()
